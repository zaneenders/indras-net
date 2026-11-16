# M15 — Sustained operation with retention and reclamation

Status: planned

Dependencies: [M14](14-reference-application.md)

Read the shared [delivery contract](../PLAN.md#delivery-contract) and
[evidence format](../PLAN.md#evidence-and-verification).

## Observable outcome

Run repeated workflow and storage-update cycles under deliberately small quotas. Expire eligible history/artifacts, reclaim storage, restart during cleanup, and keep retained runs recoverable.

## Scope and boundaries

Extend M03's storage, M06's checkpoint/log lifecycle, M07's workflow records, and
M13b's artifact retention receipts with tested reclamation. Include the Raft
request-result ledger in the policy; pruning workflow rows alone does not bound
storage.

Define operation retry/deduplication horizons before discarding results. Requests
outside the supported horizon must receive an explicit expired/rejected outcome,
not silently re-execute a forgotten mutation. Preserve pinned checkpoints,
active runs, and required artifact copies.

## Implementation steps

- [ ] Specify live/history/byte quotas, retention horizons, and explicit expired-reference behavior.
- [ ] Define safe request-ledger pruning with expiry/session watermarks or an equally checkable policy.
- [ ] Add record/history deletion and indexes required by workflow retention.
- [ ] Implement crash-safe SwiftDB generation rotation or equivalent space reclamation.
- [ ] Coordinate checkpoint/log cleanup with M06 recovery and lagging-member catch-up.
- [ ] Release artifact retention only when no retained run requires the data.
- [ ] Make orphan publication/pin reconciliation explicit without deleting adopted references.
- [ ] Add bounded cleanup batches and observable capacity/backpressure results.
- [ ] Run long enough under small quotas to force several cleanup/restart cycles.

## Runnable demo

Proposed commands; the scenario must exist before these can pass:

~~~sh
swift run indras-demo sustained --seed 42 --output .demo/m15
swift run indras-demo verify .demo/m15
~~~

| Case | Driver action | Required observation |
| --- | --- | --- |
| Quota cycle | Run beyond configured live/history limits with retention enabled | Eligible history is removed; accepted new work progresses |
| At capacity | Exhaust quota while records remain pinned | Clear admission failure; no required state is deleted |
| Cleanup crash | Kill during generation/checkpoint/retention publication | Recover a complete consistent state |
| Old retry | Retry within and beyond the declared deduplication horizon | Original result within horizon; explicit expiry/rejection after it |
| Pinned artifacts | Clean while active/retained runs reference content | Required copies remain fetchable; unneeded copies become reclaimable |
| Lagging member | Keep follower down across cleanup cycles, then restart | Valid checkpoint plus suffix restores it |
| Final restart | Restart all services after repeated reclamation | Retained workflow state and artifacts still verify |

## Acceptance criteria

- [ ] Fixed live data and configured retained history have a declared storage bound.
- [ ] The driver forces actual reclamation rather than merely finishing below quotas.
- [ ] Cleanup cannot turn an old ambiguous request into a new external effect silently.
- [ ] Retained checkpoints, runs, operation results, and artifacts have consistent lifetimes.
- [ ] Crash recovery never selects a partially published generation or checkpoint.
- [ ] Expired-run/reference behavior is explicit and matches the client contract.
- [ ] Storage and pending cleanup work stay within declared fixture-specific limits.
- [ ] Active work continues or receives explicit capacity errors under pressure.

## Required evidence

Record configured quotas/horizons, lifecycle counts, byte usage before/after cleanup, generation/checkpoint identities, deleted/pinned records and artifacts, old retry outcomes, and final restart digests.

## Regression and completion gate

The entire accepted demo suite plus storage, request-identity, checkpoint, and artifact-retention crash checks.

- [ ] All listed cases and acceptance checks pass.
- [ ] Independent verification passes and rejects deliberately invalid evidence.
- [ ] Record exact commands, source identity, configuration, and run location here.
- [ ] Document public API/disk-format changes and completed simplification work.

Evidence: not yet produced.

## Out of scope

Unlimited retention, transparent retries forever, production throughput guarantees, and cross-region operations.
