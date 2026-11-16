# M03 — Generic SwiftDB records and checkpoints

Status: planned

Dependencies: [M00](00-demo-harness.md)

Read the shared [delivery contract](../PLAN.md#delivery-contract) and
[evidence format](../PLAN.md#evidence-and-verification).

## Observable outcome

A standalone program atomically updates related records, survives process death, and exports/restores a versioned checkpoint into another directory without Raft, actors, or workflows.

## Scope and boundaries

Introduce a standalone SwiftDB target; there is no database implementation or
disk format in this checkout. Build the generic batch/checkpoint interface
needed by M05 and standalone storage consumers. Keep scheduling and job
interpretation in higher-level adapters. Version the durable format from its
introduction and add process-death checks around commit/publication barriers.

## Implementation steps

- [ ] Specify keys, byte limits, conditional atomic batches, and uncertain-commit errors.
- [ ] Implement durable records and conditional atomic batches behind a public storage interface.
- [ ] Keep single-owner directory locking and poisoned-handle/reopen behavior explicit.
- [ ] Export an immutable checkpoint with format identity, revision, and checksums.
- [ ] Restore into staging and publish only after validation and synchronization.
- [ ] Add a standalone consumer of the generic records API.
- [ ] Record finite storage limits; sustained reclamation is handled by M15.

## Runnable demo

Proposed commands; the scenario must exist before these can pass:

~~~sh
swift run indras-demo storage --seed 42 --output .demo/m03
swift run indras-demo verify .demo/m03
~~~

| Case | Driver action | Required observation |
| --- | --- | --- |
| Conditional batch | Update two balances and a revision; try a conflict | Valid batch publishes together; conflict changes nothing |
| Commit-boundary death | Kill around write/sync/publication barriers | Whole old or whole new state; no mixed revision/balances |
| Acknowledged commit | Kill after success and reopen | Acknowledged revision and values survive |
| Checkpoint round trip | Export consistent revision and restore elsewhere | Identical logical records and revision |
| Invalid checkpoint | Truncate/corrupt data or change format | Restore rejects without replacing valid destination |
| Failure/limit | Inject write failure or exhaust quota | Defined error; no false success or stale continuing writes |

## Acceptance criteria

- [ ] All conditions are checked before a batch is visible.
- [ ] An independent constant-balance-total/revision invariant survives each crash case.
- [ ] Only one writer owns a database directory.
- [ ] Unknown commit outcomes require the documented reconciliation/reopen behavior.
- [ ] Export represents a consistent revision rather than copying arbitrary live files.
- [ ] Invalid checkpoints or committed corruption cannot silently become empty databases.
- [ ] The standalone consumer uses only the public storage API.

## Required evidence

Include named persistence barriers, acknowledgement status, pre/post records, checkpoint metadata/checksums, and restored record digests.

## Regression and completion gate

New storage batch/process-death checks, the standalone records consumer, and M00.

- [ ] All listed cases and acceptance checks pass.
- [ ] Independent verification passes and rejects deliberately invalid evidence.
- [ ] Record exact commands, source identity, configuration, and run location here.
- [ ] Document public API/disk-format changes and completed simplification work.

Evidence: not yet produced.

## Out of scope

Consensus, scheduling, cross-node replication, distributed transactions, and production reclamation.
