# Build stage
FROM golang:1.25 AS builder
ARG TARGETOS
ARG TARGETARCH

WORKDIR /app

# Copy go mod and sum files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy the source code
COPY cmd/sandbox-manager ./cmd/sandbox-manager
COPY pkg ./pkg
COPY api  ./api
COPY client ./client
COPY proto ./proto
COPY test ./test

# Build the binary
RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} go build -a -installsuffix cgo -o sandbox-manager ./cmd/sandbox-manager

# Final stage
FROM alpine:3.20 AS runtime

RUN apk --no-cache add ca-certificates

WORKDIR /
# Copy the binary from builder stage
COPY --from=builder /app/sandbox-manager .
USER 65532:65532

# Expose port
EXPOSE 8080

# Run the binary
CMD ["/sandbox-manager"]
