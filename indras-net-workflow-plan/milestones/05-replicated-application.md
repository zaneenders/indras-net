# M05 — Durable application of committed commands

Status: planned

Dependencies: [M03](03-swiftdb-storage.md), [M04](04-standalone-raft.md)

Read the shared [delivery contract](../PLAN.md#delivery-contract) and
[evidence format](../PLAN.md#evidence-and-verification).

## Observable outcome

The M04 counter uses SwiftDB for application records. A committed command updates domain records, its durable operation result, and the applied Raft position in one transaction. Losing a reply never requires guessing whether the command applied.

## Scope and boundaries

Add a reusable replicated-store adapter between the RaftRuntime and SwiftDB
interfaces introduced by M04 and M03. This adapter is not implemented yet.
Keep deterministic command interpretation separate from the async storage
boundary. Raft keeps the consensus journal; SwiftDB holds resulting records.
The adapter has no workflow-specific commands or table names.

## Implementation steps

- [ ] Define a minimal application adapter for deterministic interpretation and record mutations.
- [ ] Introduce a serialized async application boundary that cannot be overtaken across suspension.
- [ ] Choose one owner for request deduplication and connect the runtime to that durable ledger.
- [ ] Atomically store mutations, operation fingerprint/result, and applied index.
- [ ] Specify watermark handling for duplicate, rejected, no-op, and membership entries.
- [ ] Persist storage identity binding the database to the intended application/cluster.
- [ ] Stop participation on unreconciled application-storage failure and recover through reopen/replay.
- [ ] Use the M04 counter as the first application of the adapter.

## Runnable demo

Proposed commands; the scenario must exist before these can pass:

~~~sh
swift run indras-demo replicated-store --seed 42 --output .demo/m05
swift run indras-demo verify .demo/m05
~~~

| Case | Driver action | Required observation |
| --- | --- | --- |
| Normal application | Submit distinct counter increments | State and durable operation results match the independent model |
| Before DB commit | Kill after Raft commit but before application transaction | Committed command applies during recovery |
| After DB commit | Kill after durable transaction but before reply | Retry returns original result without reapplying |
| Retry conflict | Reuse identity with different command bytes | Conflict is rejected without mutation |
| Storage failure | Inject failure during committed application | Node does not claim successful application from inconsistent state |
| Follower lag | Pause application on a follower, then release it | Ordered application catches up without skipping committed entries |
| Full restart | Stop all processes and reopen their original directories | Records, ledger, and applied positions reconcile |

## Acceptance criteria

- [ ] Every visible revision has matching records, durable result ledger, and watermark.
- [ ] The database watermark never claims uncommitted or undurably applied state.
- [ ] Replay skips already applied entries safely and applies the remaining committed suffix.
- [ ] Repeated identities return the recorded response; conflicting identities never mutate state.
- [ ] Application processing remains serialized through asynchronous persistence.
- [ ] Local storage failure suppresses success and unsafe continued participation.
- [ ] The reusable store contains no workflow, lease, or handler knowledge.

## Required evidence

Export domain records, operation fingerprints/results, applied and committed positions, application/cluster identity, and crash barrier acknowledgements. Verify the counter using distinct operation identities, not the production reducer.

## Regression and completion gate

M03/M04 and lower accepted demos, storage-boundary process-death cases, and request-deduplication tests.

- [ ] All listed cases and acceptance checks pass.
- [ ] Independent verification passes and rejects deliberately invalid evidence.
- [ ] Record exact commands, source identity, configuration, and run location here.
- [ ] Document public API/disk-format changes and completed simplification work.

Evidence: not yet produced.

## Out of scope

Workflow scheduling, database-backed consensus-log replacement, cross-store transactions, and checkpoint/log-prefix deletion before M06.
