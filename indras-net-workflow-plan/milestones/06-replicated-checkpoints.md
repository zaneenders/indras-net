# M06 — Replicated checkpoint recovery and safe compaction

Status: planned

Dependencies: [M05](05-replicated-application.md)

Read the shared [delivery contract](../PLAN.md#delivery-contract) and
[evidence format](../PLAN.md#evidence-and-verification).

## Observable outcome

Export a checkpoint of replicated application state, prune only the safely covered log prefix, restore a lagging node through snapshot installation, and recover the same state after full restart.

## Scope and boundaries

Extend the M05 adapter using the journal, snapshot metadata, and recovery
interfaces introduced by M04. These components are planned, not current sources.
Bind application records, request results, index/term, schema identity, and
configuration metadata at one consistent cut. Do not copy arbitrary live DB files.

## Implementation steps

- [ ] Specify a versioned checkpoint manifest with application/schema/cluster identity and Raft position.
- [ ] Export a consistent application view and retain the associated request-result ledger.
- [ ] Stage, validate, and synchronize checkpoints before publishing their durable reference.
- [ ] Order checkpoint publication and log compaction so every crash point remains recoverable.
- [ ] Install into staged local state and publish it with matching runtime metadata.
- [ ] Reconcile mismatched local positions explicitly; never silently discard acknowledged state.
- [ ] Add a lagging existing member that must catch up after the retained log prefix is removed.

## Runnable demo

Proposed commands; the scenario must exist before these can pass:

~~~sh
swift run indras-demo checkpoints --seed 42 --output .demo/m06
swift run indras-demo verify .demo/m06
~~~

| Case | Driver action | Required observation |
| --- | --- | --- |
| Export under traffic | Create checkpoint at N while later commands proceed | Export is exactly state/ledger at N; suffix remains replayable |
| Publication crashes | Kill before/after checkpoint publication and before/after pruning | At least one valid recovery path remains |
| Snapshot installation | Restart a configured lagging member after compaction | It installs matching state and then applies the suffix |
| Lost-reply retry | Retry a pre-checkpoint operation after installation | Original result is returned without duplicate effect |
| Invalid checkpoint | Corrupt bytes or change app/schema/cluster identity | Installation fails before replacing valid state |
| Full restart | Restart all nodes from compacted directories | Committed state, deduplication, and positions agree |

## Acceptance criteria

- [ ] The snapshot position describes the actual persisted state and ledger.
- [ ] No required log prefix is discarded before its replacement is durable.
- [ ] Partial exports/installations cannot become active checkpoints.
- [ ] Checkpoint corruption or identity mismatch cannot be treated as an empty fresh node.
- [ ] A restored node participates only after storage and runtime positions agree.
- [ ] Uncommitted suffix entries cannot become accepted application state.
- [ ] Recovery can proceed when an offline member has missed the compacted prefix.

## Required evidence

Keep checkpoint manifests/checksums, publication/pruning/install barriers, before/after retained log ranges, restored state/ledger digests, and retried operation results.

## Regression and completion gate

M05 crash cases, snapshot persistence/installation barriers, compacted-follower catch-up, and all prerequisite demos.

- [ ] All listed cases and acceptance checks pass.
- [ ] Independent verification passes and rejects deliberately invalid evidence.
- [ ] Record exact commands, source identity, configuration, and run location here.
- [ ] Document public API/disk-format changes and completed simplification work.

Evidence: not yet produced.

## Out of scope

Adding/removing voters, arbitrary backup products, rolling upgrades, and workflow semantics.
