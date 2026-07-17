#!/bin/bash
# Build tracing-enabled images and export as tar files
# Usage: ./hack/build-tracing-images.sh
set -e

TAG="tracing-demo"

echo "=== Building sandbox-manager image ==="
docker build --platform linux/amd64 -f dockerfiles/sandbox-manager.Dockerfile \
  -t "sandbox-manager:${TAG}" \
  --build-arg TARGETOS=linux --build-arg TARGETARCH=amd64 \
  .

echo ""
echo "=== Building agent-sandbox-controller image ==="
docker build --platform linux/amd64 -f dockerfiles/agent-sandbox-controller.Dockerfile \
  -t "agent-sandbox-controller:${TAG}" \
  --build-arg TARGETOS=linux --build-arg TARGETARCH=amd64 \
  .

echo ""
echo "=== Exporting manager image as tar ==="
docker save -o "sandbox-manager-${TAG}.tar" "sandbox-manager:${TAG}"

echo ""
echo "=== Done! ==="
echo "  Manager tar:  sandbox-manager-${TAG}.tar (for local cluster import)"
echo "  Controller:   agent-sandbox-controller:${TAG} (built locally, colleague will build & push to ACR)"
