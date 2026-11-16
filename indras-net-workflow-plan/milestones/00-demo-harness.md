# M00 — Reproducible demo harness

Status: planned; no runner or evidence verifier exists in this checkout.

Dependencies: none

Read the shared [delivery contract](../PLAN.md#delivery-contract) and
[evidence format](../PLAN.md#evidence-and-verification).

## Observable outcome

Launch a child process, wait for structured readiness, perform a known operation, kill/restart it, check the outcome, and leave an independently verifiable run directory. Prove the runner before relying on it for later layers.

## Scope and boundaries

Use the existing [counter example](../../Sources/IndrasCounter/CounterDemo.swift)
as the first child process. Add process lifecycle and fault helpers without a
workflow or Raft dependency; no shared process-control helpers exist here yet.
Add structured readiness and a route-level connection cut that affects the
counter's actual actor TCP connection.

## Implementation steps

- [ ] Define the manifest, event, operation, assertion, and terminal-result schemas.
- [ ] Implement bounded structured readiness, process ownership, and per-process logs.
- [ ] Allocate endpoints without fixed-port assumptions; retain required routes across restart.
- [ ] Implement kill/restart and connection-cut controls with observable effects.
- [ ] Guarantee child/listener cleanup after success, failure, timeout, and cancellation.
- [ ] Implement offline verification and deliberately invalid evidence fixtures.
- [ ] Reject unimplemented scenarios and stale/mixed output directories explicitly.

## Runnable demo

Proposed commands; the scenario must exist before these can pass:

~~~sh
swift run indras-demo harness --seed 42 --output .demo/m00
swift run indras-demo verify .demo/m00
~~~

| Case | Driver action | Required observation |
| --- | --- | --- |
| Healthy child | Start, request a known value, stop | Correct response and clean process exit |
| Process death | Kill at a named barrier and restart | Death and replacement identities recorded; replacement ready |
| Missing readiness | Start a child that never becomes ready | Bounded failure with logs; no remaining child |
| Driver cancellation | Cancel while a child is active | Teardown completes; incomplete run is retained |
| Bad evidence | Alter an expected value or remove an observation | Verifier rejects the modified run |
| Unexercised fault | Request a cut affecting no intended connection | Scenario fails instead of claiming fault coverage |

## Acceptance criteria

- [ ] A declared toolchain can run the demo from a clean checkout.
- [ ] Every wait has a deadline and identifies the condition it awaited.
- [ ] Every requested fault records an actual affected process or connection.
- [ ] No child processes or held listeners remain after any driver exit path.
- [ ] Setup failures, assertion failures, and incomplete runs are distinguishable.
- [ ] Missing or intentionally altered evidence fails independent verification.
- [ ] Runs do not depend on previous output or silently overwrite unrelated evidence.

## Required evidence

Include child lifecycle events, actual endpoints, fault acknowledgements, and cleanup assertions. Keep each negative verifier fixture separate from the successful original run.

## Regression and completion gate

Runner self-tests and invalid-evidence fixtures. Subsequent demos cannot depend on this milestone until these cases pass.

- [ ] All listed cases and acceptance checks pass.
- [ ] Independent verification passes and rejects deliberately invalid evidence.
- [ ] Record exact commands, source identity, configuration, and run location here.
- [ ] Document public API/disk-format changes and completed simplification work.

Evidence: not yet produced.

## Out of scope

Cluster discovery, a dashboard, general chaos orchestration, and unrelated observability.
