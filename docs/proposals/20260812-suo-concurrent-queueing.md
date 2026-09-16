---
title: SandboxUpdateOps Concurrency — Per-Sandbox Last-Writer-Wins (Stamp-Based)
authors:
  - "@mahe"
reviewers:
  - "@zhaomingshan"
  - "@furykerry"
creation-date: 2026-08-12
last-updated: 2026-08-25
status: provisional
see-also:
  - "/docs/proposals/20260804-suo-inplace-strategy.md"
  - "/docs/proposals/20251218-sandbox-inplace-update.md"
---

# SandboxUpdateOps Concurrency — Per-Sandbox Last-Writer-Wins (Stamp-Based)

## Table of Contents

- [Summary](#summary)
- [Motivation](#motivation)
- [Proposal](#proposal)
  - [The Stamp Protocol](#the-stamp-protocol)
  - [Decision 1: Per-Sandbox Granularity](#decision-1-per-sandbox-granularity)
  - [Decision 2: Last-Writer-Wins Requires a Full Template](#decision-2-last-writer-wins-requires-a-full-template)
  - [Decision 3: Safe Update Points](#decision-3-safe-update-points)
  - [Decision 4: Stamp-Based Terminal Accounting](#decision-4-stamp-based-terminal-accounting)
  - [Decision 5: Polling Requeue for Wake-Up](#decision-5-polling-requeue-for-wake-up)
  - [Decision 6: Accepted Intermediate Rollouts](#decision-6-accepted-intermediate-rollouts)
  - [Decision 7: Status Shape — Aggregates on SUO, Details on Sandbox](#decision-7-status-shape--aggregates-on-suo-details-on-sandbox)
  - [Decision 8: Dual-Mode Coexistence — the Mixed-Mode Admission Barrier](#decision-8-dual-mode-coexistence--the-mixed-mode-admission-barrier)
  - [Field Reference](#field-reference)
  - [Three-SUO Interleaving Scenario](#three-suo-interleaving-scenario)
- [Known Limitations](#known-limitations)
- [Risks and Mitigations](#risks-and-mitigations)
- [Alternatives](#alternatives)
- [Test Plan](#test-plan)
- [Implementation History](#implementation-history)

## Summary

`SandboxUpdateOps` (SUO) is this project's batch-update API: one SUO object
declares "bring this set of sandboxes to that target spec", picks a strategy
(`Recreate` / `CheckpointRestore` / `InplaceUpdate`), and the controller rolls
the change out under a rolling window.

Today only one SUO may be active per namespace. When users submit updates
concurrently, the later, overlapping SUO is either rejected outright or
silently completes as a no-op: **the user's change never lands, and nothing
tells them.**

This proposal makes concurrent batch updates safe and predictable through a
**stamp-based last-writer-wins protocol**:

- **Template mode (new):** a SUO carrying the complete desired template
  (`spec.template` / `spec.templateRef`) may run concurrently with other
  template-mode SUOs. Every **successfully completed** round stamps the
  sandbox with the delivering SUO's creation timestamp; **a SUO never
  overwrites a newer stamp**. Each sandbox
  therefore converges monotonically to the newest template targeting it, and
  superseded work is reported, never silently dropped.
- **Patch mode (existing behavior, kept):** a SUO carrying an incremental
  `spec.patch` keeps today's namespace-exclusive serialization.
- **Safety guarantees, both modes:** a sandbox is never touched inside a
  critical section; one busy sandbox never stalls the rest of a batch;
  deleting a SUO cancels its future work and **never rolls back landed work**
  — stamps survive deletion.
- **Accepted cost:** an older SUO may land an obsolete intermediate round in
  the window before the newest SUO reaches a sandbox (Decision 6). The final
  state is unaffected; the sandbox pays extra rollouts.

No new lifecycle states: the sandbox keeps `Running` and `Upgrading`, the SUO
keeps `Pending / Updating / Completed / Failed`.

## Motivation

Users may issue multiple SUOs concurrently — CI/CD pipelines, automation,
several operators — and this is not under our control. Today the later SUO's
change is silently dropped. The user's real expectation when submitting
several updates in a row is that the **last submission describes the final
state**; the system should converge each sandbox to that final state.

### Goals

- **Convergence:** every sandbox ends at the template of the newest surviving
  SUO selecting it.
- **Monotonicity:** a sandbox's landed template revision never moves backward.
- **No silent drops:** superseded work is visible in SUO status.
- **Per-sandbox progress:** one busy sandbox never stalls the rest of a batch.
- **Deterministic final state:** the outcome is a function of which SUOs
  survive and which rounds landed — never of reconcile timing.

### Non-Goals/Future Work

- **Minimal rollouts as a guarantee.** Obsolete intermediate rounds are
  accepted and bounded (Decision 6); eliminating them requires winner
  computation over live SUOs, which was deliberately traded away
  (Alternative 6).
- **Strict FIFO execution.** Older SUOs are intentionally superseded.
- **Waiting timeout.** Liveness comes from fast-fail and round completion.
- **Event-driven wake-up.** Polling requeue is the MVP mechanism.
- **Multi-worker hardening.** Default single worker; higher values best-effort.
- **Mid-critical-section preemption.** Never planned; see Decision 3.

## Proposal

### The Stamp Protocol

The **stamp** is a landed-work record: when a round **successfully
completes**, the sandbox is stamped with the delivering SUO's name and
`creationTimestamp` — it means "this sandbox has been brought to this SUO's
template", never merely "some SUO started working on it".

Two markers carry the protocol, with strictly separated jobs:

- **Pending record** — written at **round start** — the single atomic patch
  by which a SUO starts a round on a sandbox — carrying that SUO's name and
  `creationTimestamp`. It does exactly two things: it signals *occupied* (a
  round is in flight), and it is the raw material for the stamp — promoted
  (copied to the stamp, then removed) when the round succeeds, cleared with
  no stamp written when the round terminally fails. It is **never** an
  input to the newness comparison.
- **Stamp (landed)** — written only by that promotion; never cleared. The
  **sole input** of the update rules.

For a SUO `S` reconciling a selected sandbox:

```
round in flight (pending record present)  → wait for its terminal state
no stamp                                  → update it (start a round)
stamp older than S (tie-break: name)      → update it (start a round)
stamp newer than S                        → skip; count as superseded
```

An unfinished round is never re-targeted, by anyone: SUOs wait for its
terminal state and then re-read the stamp. That single rule is the whole
anti-livelock story — nobody fights over an in-flight round, and the
finished round's outcome (stamp or no stamp) decides who goes next.

"Older/newer" compares `(creationTimestamp, name)` lexicographically, so the
order is total and deterministic even for same-second creations.

Three properties follow:

- **Monotone.** The stamp only ever advances toward newer intents; landed
  work never regresses. This is what makes the final state deterministic: each
  sandbox ends at the newest surviving SUO's template, because that SUO never
  skips and nobody can overwrite it.
- **Self-settling.** An older SUO settles a superseded sandbox the moment it
  observes a newer stamp — there is no handover
  to wait for, no awaiting-takeover state.
- **Durable.** Stamps survive SUO deletion, and so does the pending record
  of an in-flight round. **Implementation requirement:** promotion must not
  depend on the SUO object still existing — a round its deleted SUO left in
  flight (finish-present) is promoted at success by the sandbox-side
  round-completion handling, sourced from the pending record alone. Landed
  work therefore can never be rolled back by a surviving older SUO;
  deletion is cancel-future, finish-present, never rollback.

### Decision 1: Per-Sandbox Granularity

| Aspect | Detail |
|--------|--------|
| **Behavior** | 10 selected, 3 occupied by an in-flight round → 7 update immediately, 3 update at their next safe point |
| **Rationale** | One stuck sandbox must not stall nine ready ones; with the full-template contract the user intent is *convergence to a final state*, not batch atomicity |
| **Rejected** | Whole-SUO all-or-nothing waiting (Alternative 2) — one stuck sandbox stalls the whole batch |

### Decision 2: Last-Writer-Wins Requires a Full Template

Last-writer-wins is only safe if every writer fully describes the desired
state. With incremental patches (`SUO-1: image`, `SUO-2: cpu`), skipping or
overwriting silently loses intents.

| Aspect | Detail |
|--------|--------|
| **Contract** | Every SUO participating in the stamp protocol carries the complete desired template snapshot |
| **API shape** | Add `EmbeddedSandboxTemplate` (`template` \| `templateRef`) alongside `spec.patch`, consistent with `SandboxSet`/`Sandbox`; exactly one mode per SUO (webhook-enforced) |
| **`spec.patch`** | Retained as a legacy mode; patch-mode SUOs never join the stamp protocol — they keep namespace-exclusive serialization (Decision 8) |
| **Spec mutability** | Only `maxUnavailable` and `paused` stay mutable; everything else is immutable after creation. Binding intent to `creationTimestamp` is what makes the stamp order meaningful; new intent = new SUO |
| **Rejected** | Patch with a "must be full" convention (unverifiable); removing `spec.patch` (breaks workflows); patch-stacking (Alternative 3) |

### Decision 3: Safe Update Points

A SUO may start a round on a sandbox only when **no round is in flight**.
An occupied sandbox is never re-targeted mid-round — not even while the
round has not yet touched the pod, and categorically not inside a critical
section (`Checkpointing` with a commit job in flight, `PostUpgrade` hooks
running inside the new pod, or an in-flight InplaceUpdate patch, whose
completion is judged against an ImageID baseline that a re-patch would
corrupt). The SUO waits for the round's terminal state:

- **S2** — the round succeeded (`Succeeded`): the pending record was
  promoted to the stamp; the next round may start by the stamp rules.
- **S3** — the round terminally failed with a determinable pod state (the
  pending record was cleared, no stamp written):
  - *resize infeasible*: pod stable, the next round may start immediately.
  - *image pull failed*: the pod is stuck mid-pull; `ImagePullBackOff` never
    picks up a changed `spec.image` (E2E-verified), so only a
    **Recreate/CheckpointRestore** SUO may start the next round — an
    InplaceUpdate SUO reports the sandbox **failed** with guidance instead
    of falling back or escalating the policy.

**Dropped optimization:** re-targeting a round that has not yet touched the
pod (early takeover) would save one rollout but requires mid-flight
comparison rules; rejected for protocol simplicity — an obsolete in-flight
round simply runs to its end (Decision 6). Checkpointing-completion
switching and half-built-pod rebuild stay out of scope for the same reason.

**Atomic write:** an update is one sandbox patch carrying the new template,
the pending record, `LabelSandboxUpdateOps`, and the upgrade policy/lifecycle
fields together, so no observer sees a record/template mismatch.

**Window accounting:** a failure produced by this SUO's own round consumes
its `maxUnavailable` window (circuit breaker); an inherited policy-mismatch
failure on a pod broken before this SUO touched it is counted and reported
but does not consume the window.

### Decision 4: Stamp-Based Terminal Accounting

A SUO reaches `Completed` when every selected sandbox is either **at its
template carrying its stamp** or **carrying a newer stamp** (superseded).
The status message records the split (e.g. `5 updated, 3 superseded by
ops-c`).

- **Immediate settlement:** observing a newer stamp settles the sandbox on
  the spot — the stamp is delivery evidence (the newer SUO's round actually
  completed), so there is no awaiting-handover state and no dependency on
  the newer SUO's lifecycle.
- **Historical accounting:** counters record what this SUO delivered. A newer
  SUO may later overwrite a sandbox this SUO reported as `updated`;
  `updated` means "was brought to my template", not "is still at it". This
  is accepted and documented; the sandbox itself is the source of truth for
  current state.
- **No new phase:** `Pending / Updating / Completed / Failed` unchanged; no
  `Superseded` value. Supersession is a message, not a state.

### Decision 5: Polling Requeue for Wake-Up

Still necessary: the `SandboxEventHandler` enqueues only the SUO named by the
sandbox's ops label, so a newer SUO waiting on an occupied sandbox is never
woken by that sandbox's events, and deleting a SUO enqueues nobody.

| Aspect | Detail |
|--------|--------|
| **Mechanism** | A SUO with waiting sandboxes requeues with a configurable delay (MVP default 30s); each poll re-reads stamps and occupancy and updates whatever became free |
| **Covers** | round completion, terminal failure (incl. fast-fail), and deletion of other SUOs |
| **Future** | Event-driven wake-up — deferred |

### Decision 6: Accepted Intermediate Rollouts

The stamp protocol has no winner computation: an older SUO does not know a
newer SUO exists until the newer SUO reaches the sandbox. In the window
before that, the older
SUO may start an obsolete round; the newer SUO waits it out, then re-updates
the sandbox.
In a burst of N overlapping template SUOs, a sandbox may in the worst case
run one round per SUO.

**This is accepted by design**, in exchange for protocol simplicity: no
winner computation over live SUOs, no handover-wait states, two on-sandbox
markers (pending + landed) as the entire coordination surface. The final state is unaffected
(monotone stamps). Bounds and escape hatches:

- **Circuit breaker bounds the blast radius.** Failed obsolete rounds consume
  the old SUO's own `maxUnavailable` window, capping how many sandboxes one
  obsolete SUO can damage before stalling.
- **Bad-image wedge and rescue.** An obsolete InplaceUpdate round whose image
  is unpullable wedges the pod in `ImagePullBackOff` (container down).
  Image-pull fast-fail turns this terminal within seconds; per Decision 3
  only a Recreate/CheckpointRestore SUO can rescue it. The rescue is one
  action — submit a newer Recreate-type SUO, which lands by the stamp rules.
  No deletion required.
- **Future work:** if kubelet semantics ever allow a corrected image to be
  re-pulled on a backoff container, in-place repair closes the wedge without
  a pod replacement.

**Paused SUOs:** `paused` stops starting new rounds; in-flight rounds finish
(the brake is not an abort). A paused SUO does **not** block other SUOs — an
older SUO may still update its sandboxes, at the cost of an extra rollout
after unpause. There is no frozen-selection semantics; recovery is unpause,
supersede with a newer SUO, or delete.

### Decision 7: Status Shape — Aggregates on SUO, Details on Sandbox

The SUO status carries **counters only**; per-sandbox detail lives on the
sandbox itself, which is the single source of truth.

```yaml
status:
  phase: Updating
  updatedReplicas: 750     # brought to my template (stamp = mine)
  updatingReplicas: 50     # rounds in flight toward my template
  waitingReplicas: 180     # occupied by another round, waiting for a safe point
  supersededReplicas: 20   # carrying a newer stamp; settled as superseded
  failedReplicas: 0
```

| Aspect | Detail |
|--------|--------|
| **Detail lookup** | `kubectl get sandbox -l <selector>`; the ops label names the round a sandbox currently follows, the stamp annotation names the newest landed revision, and the `Upgrading` condition message carries the wait reason (e.g. `ImagePullBackOff`) |
| **Events** | Key transitions recorded as SUO events: round started, update blocked at a critical section, policy-mismatch failure (Decision 3) |
| **Rationale** | Sandbox state is never duplicated into SUO status; status size is O(1) regardless of batch size |
| **Rejected** | Per-sandbox detail lists in status — O(N) churn, etcd growth, a second copy of sandbox truth |

### Decision 8: Dual-Mode Coexistence — the Mixed-Mode Admission Barrier

| Mode | Field | Semantics | Concurrency |
|------|-------|-----------|-------------|
| **patch** (legacy) | `spec.patch` | incremental SMP on each sandbox's current template | namespace-exclusive serialization — today's behavior |
| **template** | `spec.template` / `spec.templateRef` | full desired final template | per-sandbox stamp-based last-writer-wins |

A SUO must set exactly one mode (webhook-enforced, immutable). A patch is an
increment relative to whatever the sandbox happened to look like, not a
comparable final state, so patch-mode SUOs never join the stamp protocol.
Whenever the two modes could interleave, the namespace degenerates to
exclusive serialization:

| Active (non-terminal) SUOs in namespace | New template SUO | New patch SUO |
|---|---|---|
| none | admit | admit |
| template-mode only | admit (stamp protocol) | reject |
| any patch-mode | reject | reject |

**Admit iff no active SUO exists, or the new SUO and every active SUO are
template-mode.** Enforcement is two-layered: the webhook rejects at creation
(phase-based non-terminal check, which also blocks during finalizer-held
deletion), and the controller re-checks defensively (requeue with delay +
`Blocked` event; deletion handling runs *before* the barrier check so the
escape hatch cannot jam; an unlabeled `Upgrading` sandbox — its SUO deleted
mid-round — is not updated until the round settles).

### Field Reference

#### SUO `spec`

| Field | Purpose | Mutability |
|-------|---------|------------|
| `selector` | Label selector picking targets (SandboxSet-controlled sandboxes excluded) | immutable |
| `patch` | Legacy incremental mode; never joins the stamp protocol | immutable |
| `template` / `templateRef` | **New** — full desired template snapshot (`EmbeddedSandboxTemplate`) | immutable |
| `updateStrategy.type` | `Recreate` (default) / `CheckpointRestore` / `InplaceUpdate` | immutable |
| `updateStrategy.maxUnavailable` | Rolling-window size, default 1 | mutable |
| `lifecycle` | pre/post-upgrade hooks copied onto each sandbox for the round | immutable |
| `paused` | Emergency brake: stops starting new rounds; in-flight rounds finish | mutable |
| `stateFilter` | Sandbox phases eligible as new candidates (default `[Running]`) | immutable |

#### SUO `status`

`phase` (`Pending / Updating / Completed / Failed`, unchanged),
`observedGeneration`, `replicas`, and the five counters of Decision 7.
Counters are recomputed each reconcile from the live sandbox set — status is
a report, never a durable ledger; the durable record is the stamp.

#### Sandbox-side writes (per sandbox, per round)

**Round start** is the single merge patch by which a SUO starts a round on
a sandbox, carrying the template, policy, lifecycle, label, and pending
record atomically; the stamp is written later, by promotion, when the round
succeeds:

| Sandbox field | When | Content |
|---------------|------|---------|
| `spec.template` | Round start (phase 2 of the paused two-phase flow) | Target template — template mode: full overwrite; patch mode: strategic merge |
| `spec.upgradePolicy` | Round start | Mapped from `updateStrategy.type`; cleared once the round succeeds |
| `spec.lifecycle` | Round start | Deep copy of the SUO's `spec.lifecycle`, or removed |
| `metadata.annotations[agents.kruise.io/update-ops-pending-revision]` | Round start | **New — the pending record**: the starting SUO's name + `creationTimestamp`, written atomically with the template. Occupancy signal and promotion source only — never a comparison input. Promoted (copied to the stamp, then removed) when the round succeeds; cleared on terminal failure; **kept if the SUO is deleted mid-round** so the promotion still happens |
| `metadata.annotations[agents.kruise.io/update-ops-revision]` | Round success (promotion) | **New — the stamp**: copied from the pending record by the sandbox-side round-completion handling; **never cleared, survives SUO deletion**; the durable record of the newest landed revision and the sole input of the update rules |
| `metadata.labels[agents.kruise.io/update-ops]` | Round start | Names the SUO the sandbox currently follows — event routing and `kubectl` filtering only, no protocol role; cleared on ops deletion |
| `metadata.labels[agents.kruise.io/upgrade-failed]` | Round end (failure) | Marks terminally failed sandboxes; cleared when a new round starts |
| `metadata.annotations[agents.kruise.io/upgrade-resume-trigger]` | Two-phase upgrade of Paused sandboxes | Phase 1 sets it; phase 2 removes it; ops deletion also removes it |

The SUO never writes the sandbox's `status` — the sandbox controller owns it.

### Three-SUO Interleaving Scenario

SUO-A (sandboxes 1,2), SUO-B (2,3), SUO-C (1,3); created in that order, all
template-mode. One possible execution (single worker):

```
A reconcile: sbx-1,2 free, no stamp → starts rounds (pending=A)
B reconcile: sbx-2 occupied (round A in flight) → wait;
             sbx-3 free, no stamp → starts a round (pending=B)
C reconcile: sbx-1 occupied → wait; sbx-3 occupied → wait
sbx-1 round (A) succeeds (stamp=A) → C: stamp older → starts (pending=C)
sbx-2 round (A) succeeds (stamp=A) → B: stamp older → starts (pending=B)
sbx-3 round (B) succeeds (stamp=B) → C: stamp older → starts (pending=C)
remaining rounds succeed → stamp=C on sbx-1,3; stamp=B on sbx-2
A: Completed ("0 remaining, 2 superseded"); B: Completed; C: Completed
```

Final state — `sbx-1: C, sbx-2: B, sbx-3: C` — is deterministic: stamps are
monotone and each sandbox's newest selector-matching survivor never skips.
Only the *trajectory* is timing-dependent: had B reconciled before A reached
sbx-2, sbx-2 would have gone straight to B.template and A's intermediate
round would not have run. Obsolete intermediate rounds are best-effort
skipped, never guaranteed skipped (Decision 6).

#### SUO Deletion in the Same Scenario

Deleting a SUO is **cancel-future, finish-present** — never a rollback. The
finalizer (`agents.kruise.io/sandboxupdateops-protection`) holds the delete
while the controller strips the ops label and resume-trigger annotation from
sandboxes following the SUO; **neither the stamp nor an in-flight round's
pending record is stripped**. In-flight rounds keep `spec.template` +
`spec.upgradePolicy` and run to completion, and on success the sandbox-side
round-completion handling still promotes the pending record to the stamp —
the promotion needs no SUO object (the sandbox controller drives the round
from the sandbox spec, never the SUO).

- **Delete A while its rounds on sbx-1/2 are in flight.** The rounds finish
  at A.template and are stamped A by promotion; B and C then see an older
  stamp and update those sandboxes. If A is deleted *before* starting a
  round on a sandbox, no stamp exists and the next
  SUO lands directly — A's intermediate rollout is skipped entirely.
- **Delete C after it landed on sbx-3 but before sbx-1.** sbx-3 keeps
  stamp=C and stays at C.template forever: any surviving older SUO sees the
  newer stamp and settles it as superseded — landed work is never rolled
  back, no reclaim logic exists or is needed. sbx-1 carries only A's stamp
  and simply stays with A.
- **Delete all three.** In-flight rounds finish; every sandbox rests at the
  template of its last landed round, stamp intact.

The outcome is a pure function of which SUOs survive and which rounds landed.

## Known Limitations

- **Intermediate rollouts (Decision 6).** Bounded by the burst size and the
  old SUOs' own windows; each extra round is a service interruption of a
  stateful sandbox.
- **Bad-image wedge until manual rescue.** An obsolete round with an
  unpullable image downs the container until a user submits a Recreate-type
  SUO; fast-fail makes it terminal within seconds and the circuit breaker
  caps the count per SUO, but detection-to-rescue downtime is operational.
- **Historical accounting.** A Completed SUO's `updatedReplicas` describes
  delivery history, not current sandbox state.
- **No paused freeze.** A paused newest SUO does not stop older SUOs from
  updating its sandboxes; unpause may trigger one extra rollout.
- **Concurrent writers.** Two SUOs may race to update the same free sandbox;
  resolution is optimistic conflict retry with full re-evaluation (re-read
  the stamp and occupancy after every conflict). The
  stamp order is total, so retries converge. Default remains 1 worker; a
  startup warning is emitted
  for higher values.
- **Wait latency bounded by round duration.** A newer SUO waits out any
  in-flight round. The InplaceUpdate path reaches S3 within seconds on bad
  images (fast-fail, implemented); a Recreate round stuck in
  `ImagePullBackOff` reaches S3 only via the upgrade's own failure detection.
  Accepted for phase 1.

## Risks and Mitigations

### Risk 1: User Submits a Partial Template Believing It Is a Patch

Omitted fields are *removals*, not "keep as is". **Mitigation:** distinct API
field so the semantics are explicit at the type level; webhook requires the
template to be self-contained (same rules as `SandboxSet.spec.template`);
documentation states the override semantics prominently. Users who want
incremental semantics keep patch mode (Decision 8).

### Risk 2: Rapid Successive SUOs (Template Thrash)

Many SUOs in quick succession → superseded ones may still land partial
intermediate rounds before the newest SUO reaches them (Decision 6).
**Mitigation:** accepted; stamps keep the final state monotone and each
superseded SUO's status says so explicitly.

### Risk 3: InplaceUpdate Stuck Round Delays Update

A newer SUO never re-targets a mid-flight round. **Mitigation:**
image-pull failures reach S3 within seconds (fast-fail, implemented); resize
rejections are terminal via the existing gate; the only remaining wait is a
healthy in-flight round, which completes on its own.

### Risk 4: handleDeletion Cache Race (Pre-existing)

Informer lag can cause label-cleanup omission on SUO deletion. Unchanged by
this proposal; `ResourceVersionExpectation` and requeue handle eventual
consistency. Stamps are never cleaned, so the race cannot produce a rollback.

## Alternatives

### Alternative 1: Skip (Current Behavior)

Later SUO silently no-ops. **Rejected:** silent intent loss.

### Alternative 2: Whole-SUO All-or-Nothing Queueing (First Draft)

Queue entire SUOs FIFO; any occupied sandbox parks the whole operation.
**Rejected:** one stuck sandbox stalls the whole batch; strict sequencing
forces obsolete intermediate rollouts on *every* sandbox; new queueing
machinery.

### Alternative 3: Patch-Stacking

Merge all pending patches in timestamp order at each safe point.
**Rejected:** the final state is not stated anywhere; cross-SUO merge
conflicts are undiagnosable.

### Alternative 4: Waiting / Superseded Phases

**Rejected:** supersession is a status message, not a state machine
extension.

### Alternative 5: Event-Driven Wake-Up

Precise, zero polling delay. **Deferred:** polling covers round completion
and deletion with one mechanism.

### Alternative 6: Candidacy-Based Winner Computation (Previous Revision)

The previous revision of this proposal computed, per sandbox, a winner over
the *live* SUO set (`target(sbx)` = newest active selector-matching SUO) so
an older SUO never starts an obsolete round, and separated two kinds of
evidence: candidacy admits, actual takeover (marker written) settles the old
SUO's books.

**What it buys:** obsolete intermediate rollouts are prevented rather than
accepted — including the avoidable bad-image wedge; a paused winner freezes
its selection; `updated` counters describe current state; superseded-by-newer
is visible before the newer SUO lands (`pendingTakeoverReplicas`).

**What it costs:** winner computation on every round-start decision;
handover-wait accounting (an old SUO stays `Updating` until the newer SUO
actually delivers); settlement evidence lives in a strippable label, which a
deletion destroys — requiring a durable-floor fix that converges back toward
stamps anyway.

**Superseded:** the stamp protocol reaches the same deterministic final state
and stronger no-rollback durability with two on-sandbox markers (pending +
landed) and no handover states. The prevented obsolete rounds were judged
not worth the
extra machinery; their damage is bounded and recoverable (Decision 6).

## Test Plan

### Unit Tests

1. **Stamp rules:** a sandbox with a round in flight (pending record
   present) is never re-targeted — the SUO waits for the terminal state;
   otherwise no stamp / older stamp → update; newer stamp → skip and count
   superseded; same-second creations tie-break by name, totally ordered.
2. **Atomic round start:** template, policy, lifecycle, label, and pending
   record land in one patch; the stamp is untouched at round start; no
   partial writes observable.
3. **Safe update points:** an occupied sandbox (incl. Checkpointing /
   PostUpgrade / in-flight inplace patch) is never written; free and
   round-end sandboxes proceed; waiting sandboxes reflected in
   `waitingReplicas`.
4. **S3 rules:** resize-infeasible → immediate update; image-pull-failed →
   update only by a Recreate/CheckpointRestore SUO; an InplaceUpdate SUO
   reports the sandbox failed with guidance.
5. **Accounting:** completion iff every selected sandbox carries my stamp at
   my template or a newer stamp; message records the split; counters
   recomputed from the live set.
6. **Promotion and durability:** a successful round promotes the pending
   record to the stamp; a terminal failure clears the pending record and
   writes no stamp; deletion strips label and resume-trigger but never the
   stamp or an in-flight pending record — the round still promotes at
   success without the SUO object; a surviving older SUO settles a
   newer-stamped sandbox as superseded instead of re-updating it.
7. **Webhook:** exactly one of `patch` / `template`/`templateRef`;
   immutability rules; the dual-mode admission matrix incl.
   finalizer-pending blocking.
8. **Window accounting:** self-inflicted failures consume the window;
   inherited policy-mismatch failures are counted but do not.

### E2E Tests

1. **Same-target pair:** SUO-1 then SUO-2 on the same sandboxes; all end at
   SUO-2's template; SUO-1 completes with a supersession split; sandboxes
   SUO-1 had not reached go straight to SUO-2 (best-effort skip observed).
2. **Three-SUO interleaving:** A(1,2), B(2,3), C(1,3) → final
   `1:C, 2:B, 3:C` regardless of ordering; all three reach terminal phase.
3. **Bad-image rescue:** SUO-1 (InplaceUpdate, bad image) wedges a sandbox;
   the round fast-fails to S3 within seconds; SUO-2 (Recreate, good image)
   lands automatically — no deletion of SUO-1 required.
4. **Deletion durability:** delete the newest SUO after it landed on part of
   its selection; landed sandboxes keep its template (stamp survives, no
   older SUO re-updates them); untouched sandboxes converge to the surviving
   SUOs.
5. **Mixed-mode serialization:** patch-mode SUO active → template SUO
   rejected at creation; after it terminates or is deleted the template SUO
   proceeds; sandboxes never receive writes from two modes concurrently.

## Implementation History

- 2026-08-12: Original draft — whole-SUO all-or-nothing queueing
  (`Pending + WaitingFor`, FIFO admission).
- 2026-08-19: Redesigned to sandbox-centric latest-template-wins after design
  review: per-sandbox granularity, full-template contract, safe switch
  points, no new phases. The queueing draft is preserved as Alternative 2.
- 2026-08-21: Documentation pass: field reference and SUO-deletion semantics
  (cancel-future, finish-present).
- 2026-08-21: Dual-mode revision after review: `spec.patch` retained as a
  legacy namespace-exclusive mode; added the mixed-mode admission barrier.
- 2026-08-21: Accounting hardened: takeover-based completion gate alongside
  candidacy-based admission.
- 2026-08-24: Redesigned coordination from candidacy-based winner computation
  to the stamp-based last-writer-wins protocol after review: a durable stamp
  written at round start is the sole coordination state; obsolete intermediate
  rollouts are accepted and bounded (Decision 6) in exchange for protocol
  simplicity and deletion-proof no-rollback. The candidacy design is
  preserved as Alternative 6.
- 2026-08-25: Stamp moved from round start to completion time after
  review: round start writes a pending record — an occupancy signal and
  the promotion source, never a comparison input; the round's success
  promotes it to the landed stamp, terminal failure clears it. Update
  rules compare the stamp only, and an in-flight round is never
  re-targeted — SUOs wait for its terminal state (early takeover dropped).
  Explicit implementation requirement: promotion is performed by the
  sandbox-side round-completion handling and must not depend on the SUO
  object still existing, so a round left in flight by a deleted SUO is
  still stamped at success.
