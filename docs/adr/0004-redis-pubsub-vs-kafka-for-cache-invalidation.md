# ADR 0004: Redis Pub/Sub with Versioned Snapshot Fallback vs. Apache Kafka

- **Status:** Accepted
- **Date:** 2026-11-11

## Context

When the Control Plane updates a flag in PostgreSQL, all horizontally scaled Data Plane gRPC servers must be notified immediately so they can fan out the update to connected SDKs. We evaluated **Redis Pub/Sub** versus **Apache Kafka**.

## Decision

We chose **PostgreSQL as the durable Source of Truth** paired with **Redis Pub/Sub as an ephemeral low-latency invalidation bus**, reinforced by monotonic `version` numbers and full-snapshot hydration on stream reconnect.

## Rationale

- **Why not Kafka:** Kafka excels at high-throughput durable event logs, but adds significant operational complexity (brokers, partitions, consumer group rebalancing latency) for low-write-volume control plane mutations. Worse, each Data Plane pod needs *broadcast* delivery of every flag update (which in Kafka requires unique consumer groups per pod), and consumer group rebalances can introduce multi-second stalls.
- **How we solve Redis Pub/Sub's fire-and-forget nature:**
  1. Every mutation in PostgreSQL atomically increments `flag_configs.version`.
  2. Whenever an SDK (or Data Plane node) connects or reconnects after a network partition, the Data Plane first queries the authoritative snapshot from PostgreSQL (`EVENT_TYPE_FULL_SNAPSHOT`) *after* subscribing to the live channel.
  3. The SDK enforces a **Monotonic Version Guard** (`incoming.Version > local.Version`), discarding stale or duplicate events.

## Consequences

- **Positive:** Sub-millisecond broker fan-out latency with minimal operational footprint.
- **Positive:** Guaranteed eventual consistency even if Redis restarts or drops a transient Pub/Sub message during a network blip.

