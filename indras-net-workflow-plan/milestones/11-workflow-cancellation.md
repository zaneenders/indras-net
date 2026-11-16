# M11 — Durable workflow cancellation

Status: planned

Dependencies: [M10](10-durable-timers.md)

Read the shared [delivery contract](../PLAN.md#delivery-contract) and
[evidence format](../PLAN.md#evidence-and-verification).

## Observable outcome

Cancel a workflow containing running steps, a signal wait, and a pending timer. The committed ordering determines which earlier results remain accepted and prevents later attempts or callbacks from resurrecting the run.

## Scope and boundaries

Add cancellation to the durable workflow transition model and public client
contract. Keep local worker pause separate. Cancellation stops future accepted
progress and requests cooperative worker shutdown; it does not undo external
effects that have already occurred.

## Implementation steps

- [ ] Specify terminal states and ordering for cancel versus completion, signal, timer, and child creation.
- [ ] Make repeated cancellation idempotent and return the durable terminal state.
- [ ] Prevent new claims and invalidate active attempts once cancellation is committed.
- [ ] Define handling of pending waits/timers and preserve already accepted result/history records.
- [ ] Notify reachable workers while keeping correctness independent of notification delivery.
- [ ] Cancel handler/subprocess work cooperatively when a worker learns ownership is lost.
- [ ] Ensure recovery does not regenerate cancelled ready work or timer continuations.

## Runnable demo

Proposed commands; the scenario must exist before these can pass:

~~~sh
swift run indras-demo cancellation --seed 42 --output .demo/m11
swift run indras-demo verify .demo/m11
~~~

| Case | Driver action | Required observation |
| --- | --- | --- |
| Cancel active run | Commit cancellation with leased work, wait, and timer pending | Run is terminal; no new claim or continuation is accepted |
| Completion before cancel | Commit valid completion first | Earlier accepted result remains recorded under the documented terminal policy |
| Completion after cancel | Deliver a result from the old attempt | Result is rejected; cancellation remains authoritative |
| Lost cancel reply | Retry the same cancel request | Original decision is reconciled without new effects |
| Stale inputs | Deliver old timer callbacks, signals, and child proposals | They cannot resurrect progress |
| Restart while cancelling | Lose notifications and restart cluster/workers | Recovered state still rejects further progress |

## Acceptance criteria

- [ ] Each race has an outcome determined by committed order, not wall-clock arrival claims.
- [ ] Cancelled runs cannot grant new leases or accept stale completions.
- [ ] Repeated cancellation preserves the recorded terminal result.
- [ ] Previously accepted records remain available for inspection and cleanup policy.
- [ ] Correctness survives loss of worker notifications.
- [ ] Worker slots/subprocesses terminate according to the documented cooperative behavior.
- [ ] The demo does not claim cancellation reverses external side effects.

## Required evidence

Record cancellation/completion ordering, attempt invalidation, timer/wait terminal states, rejected late commands, and worker cleanup observations.

## Regression and completion gate

M07 lease handling and M08–M10 progress semantics, plus all lower accepted recovery checks.

- [ ] All listed cases and acceptance checks pass.
- [ ] Independent verification passes and rejects deliberately invalid evidence.
- [ ] Record exact commands, source identity, configuration, and run location here.
- [ ] Document public API/disk-format changes and completed simplification work.

Evidence: not yet produced.

## Out of scope

Rolling back external services, force-stopping unreachable machines, and compensation workflows.
