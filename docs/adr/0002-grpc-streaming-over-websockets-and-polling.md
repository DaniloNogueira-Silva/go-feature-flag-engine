# ADR 0002: gRPC Server Streaming over WebSockets and HTTP Polling

- **Status:** Accepted
- **Date:** 2026-11-11

## Context

Once feature flags are cached in the SDK's memory, we need a mechanism to synchronize the initial snapshot and propagate real-time invalidations when a Product Manager toggles a kill-switch or updates targeting rules. The three main candidates are:
1. **HTTP Short/Long Polling**
2. **WebSockets**
3. **gRPC Server Streaming over HTTP/2**

## Decision

We chose **gRPC Server Streaming** (`FlagStreamService.Subscribe`) for SDK-to-Data-Plane communication.

## Rationale

- **Vs. HTTP Polling:** Polling introduces a painful trade-off between propagation latency and server load. Polling every 1s across 5,000 microservice pods generates 5,000 RPS of redundant traffic even when zero flags change.
- **Vs. WebSockets:** WebSockets lack native schema enforcement, multiplexing, and built-in flow control. Every custom framing, heartbeat, and reconnection semantic must be hand-rolled.
- **Why gRPC Server Streaming:**
  - Strict contract evolution via Protocol Buffers (`api/proto/v1/flagstream.proto`).
  - Native HTTP/2 transport keepalives, stream multiplexing, and backpressure.
  - Unified stream lifecycle: the Data Plane registers the client in its in-memory `Hub` *before* reading the initial `FULL_SNAPSHOT`, transmits the snapshot as the first frame, and seamlessly transitions to streaming `FLAG_UPDATED` delta events on the same channel without race conditions.

## Consequences

- **Positive:** Sub-10ms end-to-end cache invalidation with near-zero idle network bandwidth.
- **Trade-off:** Requires L7 load balancers that support HTTP/2 and long-lived gRPC connections (e.g., Envoy, AWS ALB with gRPC target groups).

