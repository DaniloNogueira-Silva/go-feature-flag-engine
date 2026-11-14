# ADR 0003: Deterministic Percentage Rollouts via Stack-Allocated MurmurHash3

- **Status:** Accepted
- **Date:** 2026-11-11

## Context

Percentage rollouts (e.g., "enable for 20% of users") and A/B experiments require two strict properties:
1. **Stickiness (Determinism):** A given `userID` must consistently receive the same variation across all microservice instances and across time without storing per-user assignments in a database.
2. **Uniformity & Independence:** A 20% rollout must activate for `20.0% ± 0.5%` of the user population, and being in the top 20% for `flag-A` must not correlate with being in the top 20% for `flag-B`.

## Decision

We use **32-bit MurmurHash3 (`x86_32`)** over the composite key `flagKey + ":" + userID + ":" + salt`, modulo `10,000` (basis points, where `10000 = 100.00%` and `1 = 0.01%`).

To eliminate garbage collection pressure on the hot path, `ComputeRolloutBucket` concatenates keys up to 256 bytes onto a **stack-allocated array (`var stackBuf [256]byte`)**, avoiding heap allocations altogether.

## Consequences

- **Positive:** Zero database lookups, zero network calls, and **0 heap allocations (`0 B/op`, `0 allocs/op`)** during rollout evaluation.
- **Positive:** Basis-point precision (`0..10000`) enables gradual canary releases starting at `0.01%` (`1` basis point).
- **Positive:** Rotating a flag's `salt` re-randomizes the user population independently when resetting an experiment.

