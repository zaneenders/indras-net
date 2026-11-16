# M08 — Atomic completion and dynamic child steps

Status: planned

Dependencies: [M07](07-coordinator-workers.md)

Read the shared [delivery contract](../PLAN.md#delivery-contract) and
[evidence format](../PLAN.md#evidence-and-verification).

## Observable outcome

A planner step creates a fork/join continuation as part of its accepted completion. Recovery and retries cannot lose children, duplicate them, or change the accepted child definition.

## Scope and boundaries

Extend M07's workflow records, shared storage contract, actor API, and replicated
transitions with atomic completion and successor creation. These interfaces are
planned; there is no existing local-only successor implementation in this checkout.
Represent accepted runtime additions while preserving the identity of the
original submission.
Planner/model execution remains a worker handler outside consensus.

## Implementation steps

- [ ] Define a completion-with-children command carrying stable child IDs and full child specifications.
- [ ] Validate IDs, dependencies, workflow membership, and quotas before any mutation.
- [ ] Persist parent result, accepted child specification, child records, and readiness atomically.
- [ ] Retain the accepted child definition or its canonical fingerprint for reconciliation.
- [ ] Reject changed child lists/results on a repeated completion, including added/removed children.
- [ ] Expose runtime membership and join status through the workflow query API.
- [ ] Use a deterministic planning fixture that emits the same explicit child specification on retry.

## Runnable demo

Proposed commands; the scenario must exist before these can pass:

~~~sh
swift run indras-demo dynamic-steps --seed 42 --output .demo/m08
swift run indras-demo verify .demo/m08
~~~

| Case | Driver action | Required observation |
| --- | --- | --- |
| Fork/join | Planner emits A/B and a join | Exactly the accepted graph is created and executed |
| Before atomic commit | Kill before completion transaction | Parent remains unfinished and no partial children appear |
| After atomic commit | Lose reply and restart | Parent result and every child survive together |
| Identical retry | Resubmit same completion and child specification | Original result is reconciled without new children |
| Conflicting retry | Change/add/remove a child under the same accepted completion | Conflict; accepted graph remains unchanged |
| Invalid graph | Try duplicate IDs, invalid dependencies, or a cycle | Entire transition rejects without partial mutation |

## Acceptance criteria

- [ ] Parent completion and child creation share one durable transition.
- [ ] Each accepted logical child is created once and has stable identity across attempts.
- [ ] A repeated completion is checked against the entire accepted child specification.
- [ ] No child can become runnable before its required committed dependencies.
- [ ] Runtime additions do not make an identical original workflow submission conflict with itself.
- [ ] Workflow/child quota failures leave the full prior state intact.
- [ ] Failed dependencies and join behavior follow an explicit tested policy.

## Required evidence

Keep the submitted graph, complete child specifications/fingerprints, transaction barriers, accepted parent/child records, and final membership. Verify against the fixture's expected graph independently.

## Regression and completion gate

Shared provider contracts for completion-with-successors, M07 static workflows, and replicated application/recovery checks.

- [ ] All listed cases and acceptance checks pass.
- [ ] Independent verification passes and rejects deliberately invalid evidence.
- [ ] Record exact commands, source identity, configuration, and run location here.
- [ ] Document public API/disk-format changes and completed simplification work.

Evidence: not yet produced.

## Out of scope

Persisting arbitrary Swift continuations, nondeterministic logic inside Raft, and general cyclic workflow programs.
