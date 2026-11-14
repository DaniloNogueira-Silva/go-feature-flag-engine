# ADR 0001: Physical and Logical Separation of Control Plane and Data Plane

- **Status:** Accepted
- **Date:** 2026-11-11

## Context

Enterprise applications evaluate feature flags on every incoming request—often multiple times per request across microservices. If an application queries a central database or REST API during request handling, the feature flag system becomes:
1. A latency bottleneck on the critical path.
2. A Single Point of Failure (SPOF) where database maintenance or admin traffic spikes cause cascading outages across customer-facing services.

## Decision

We split the system into three distinct runtime boundaries:

1. **Control Plane (`cmd/control-plane`):** An administrative REST API backed by PostgreSQL (`flags`, `environments`, `flag_configs`, `audit_logs`). It handles low-frequency, high-consistency operations (CRUD, optimistic concurrency control via `If-Match`/`version`, and audit logging) and publishes state transitions to Redis Pub/Sub.
2. **Data Plane (`cmd/data-plane`):** A horizontally scalable gRPC Server Streaming fleet that hydrates initial snapshots from PostgreSQL/replicas and subscribes to Redis Pub/Sub to push delta updates (`FLAG_UPDATED`) over persistent HTTP/2 connections.
3. **In-Memory SDK Evaluation (`pkg/sdk`):** Client applications evaluate rules exclusively against a pre-compiled local memory store (`sync.RWMutex`) with **zero network I/O** on the hot path.

## Consequences

- **Positive:** Customer-facing evaluation latency drops from `5–50ms` (network hop) to `~51–75ns` (in-memory read lock).
- **Positive:** Complete fault isolation—if PostgreSQL or the Control Plane goes offline completely, running SDK clients continue serving requests using their last known in-memory snapshot (*fail-open local resilience*).
- **Trade-off:** Eventual consistency window (typically `< 10ms`) between a `PUT` on the Control Plane and cache invalidation across all connected SDKs.

