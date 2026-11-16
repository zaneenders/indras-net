# M14 — Composed build and agent-workflow reference application

Status: planned

Dependencies: [M08](08-dynamic-steps.md), [M09](09-signals-approvals.md), [M10](10-durable-timers.md), [M11](11-workflow-cancellation.md), [M12](12-version-compatibility.md), [M13b](13-setu-data-services.md#m13b--workflow-artifact-retention)

Read the shared [delivery contract](../PLAN.md#delivery-contract) and
[evidence format](../PLAN.md#evidence-and-verification).

## Observable outcome

Run one real application: a planner creates a compile step, tests and review consume its result, approval gates archival, and two Setu destinations retain the verified outputs. Exercise the same application through failures.

## Scope and boundaries

Keep application code outside RaftRuntime, SwiftDB, and generic workflow
modules. Use a small checked-in Swift program and executable tests, the installed
compiler, and deterministic planner/reviewer fixtures. Record the toolchain.
No external model account or network API is required for the acceptance suite.

The normal graph is plan → compile → test + review → approval → archive.
Use durable timers for the declared approval deadline and cancellation behavior.
A fixture reviewer is not evidence of model quality; real agent handlers can
later implement the same public interfaces.

## Implementation steps

- [ ] Add checked-in source/test fixtures with independently known outcomes.
- [ ] Implement planner, compiler, test, review, and archive handlers through public worker APIs.
- [ ] Store files through M13a/M13b and pass versioned references between steps.
- [ ] Expose submit/status/signal/cancel entry points through the M07 workflow client.
- [ ] Capture child graph, results, approvals, deadlines, and archive receipts.
- [ ] Add fault barriers around compile, completion reply, approval wait, and archive publication.
- [ ] Add a deliberately failing test fixture to prove failed work does not archive as success.
- [ ] Document one command for each case and the complete local demonstration.

## Runnable demo

Proposed commands; the scenario must exist before these can pass:

~~~sh
swift run indras-demo workflow --seed 42 --output .demo/m14
swift run indras-demo verify .demo/m14
~~~

| Case | Driver action | Required observation |
| --- | --- | --- |
| Happy path | Plan, compile, test/review, approve, archive | Program/test outputs match and archive copies verify |
| Worker death | Kill compiler worker mid-step | Replacement attempt finishes; stale result cannot win |
| Leader/reply loss | Kill leader or drop a completion acknowledgement | Accepted graph/progress is reconciled |
| Approval restart | Restart cluster while waiting, then signal | Wait survives and resumes once |
| Approval timeout/cancel | Withhold approval or cancel the run | Declared terminal behavior; no unauthorized archive |
| Archive-node loss | Stop producer and one archive destination after completion | Retrieve matching artifact from surviving retained copy |
| Failing fixture | Run a program with a known failed test | Failure is recorded and successful archival is blocked |

## Acceptance criteria

- [ ] Every stage uses the published layer interfaces and declared dependencies.
- [ ] The compiler and test runner execute real subprocesses with captured outputs.
- [ ] Deterministic planning/review fixtures make expected outcomes independently checkable.
- [ ] Recorded completed steps are not rerun simply to reconstruct workflow state.
- [ ] Retries preserve task/effect identities and external effects reconcile as specified.
- [ ] Only the approved successful path produces accepted archive receipts.
- [ ] Final retrieved bytes match the accepted artifact digest and expected executable behavior.
- [ ] Evidence explains each injected failure and resulting recovery path.

## Required evidence

Include fixture/toolchain hashes, generated graph, subprocess commands/outputs, accepted step records, wait/timer history, approvals, and independent archive retrieval checks. Do not claim cross-build byte reproducibility unless separately established.

## Regression and completion gate

All prerequisite demos and the successful/failing fixture application cases.

- [ ] All listed cases and acceptance checks pass.
- [ ] Independent verification passes and rejects deliberately invalid evidence.
- [ ] Record exact commands, source identity, configuration, and run location here.
- [ ] Document public API/disk-format changes and completed simplification work.

Evidence: not yet produced.

## Out of scope

A UI, model-quality evaluation, a remote compiler sandbox, production deployment, and generalized build caching.
