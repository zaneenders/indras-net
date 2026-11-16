# M07 — Workflow coordinator and replaceable workers

Status: planned

Dependencies: [M02](02-actor-messaging.md), [M06](06-replicated-checkpoints.md)

Read the shared [delivery contract](../PLAN.md#delivery-contract) and
[evidence format](../PLAN.md#evidence-and-verification).

## Observable outcome

Three coordinator processes and two actor workers execute a bounded static dependency graph. Worker loss, leader loss, lost replies, and full-cluster restart preserve accepted workflow progress.

## Scope and boundaries

Introduce workflow records, deterministic transitions, coordinator/client APIs,
and an actor worker runtime; none exists in this checkout. Compose the coordinator
with its local replicated store from M05/M06 and shared actor messaging from M02.
Worker registration, status, and wakeups are rebuildable hints; committed leases
own scheduling. Keep standalone storage adapters independently usable.

## Implementation steps

- [ ] Implement static workflow transitions on the replicated-store application contract.
- [ ] Submit coordinator commands locally and return follower hints to remote clients.
- [ ] Carry client-generated durable operation identity through every mutation and retry.
- [ ] Add worker context containing workflow/step ID, attempt/token, and stable effect-key inputs.
- [ ] Inject optional services into handlers without making the runtime depend on Setu.
- [ ] Implement the authoritative leader clock loop in the runtime.
- [ ] Implement bounded claims, lease renewal, fallback polling, and connection-based re-registration.
- [ ] Separate local worker pause/control from durable workflow cancellation.

## Runnable demo

Proposed commands; the scenario must exist before these can pass:

~~~sh
swift run indras-demo workers --seed 42 --output .demo/m07
swift run indras-demo verify .demo/m07
~~~

| Case | Driver action | Required observation |
| --- | --- | --- |
| Static graph | Run A followed by parallel B/C and a final join | Dependency order and expected outputs match |
| Worker death | Kill a worker during a leased step | Lease expires; another attempt can finish |
| Lost claim/completion reply | Drop acknowledgement after commit; retry saved envelope | Same claim/result is reconciled rather than consuming unintended work |
| Stale attempt | Deliver an old completion after lease replacement | Old token is rejected; accepted result remains unchanged |
| Coordinator failover | Kill leader and reconnect the surviving workers | New leader serves; workers re-register without losing durable ownership |
| Lost wakeup | Drop a work-available callback | Fallback polling still discovers ready work |
| Full restart | Stop cluster and workers, restart from original storage | Completed steps stay completed; unfinished work is recoverable |

## Acceptance criteria

- [ ] Each step has at most one currently valid lease under the authoritative clock.
- [ ] Execution attempts may repeat, but accepted results cannot be overwritten by stale attempts.
- [ ] Ready steps depend only on committed predecessor success.
- [ ] Every mutation has stable identity across redirects, reconnects, and a saved caller-envelope retry.
- [ ] Handler infrastructure failures are distinct from application failures and retry policy.
- [ ] Worker slots remain bounded and losing ownership triggers the documented cancellation behavior.
- [ ] The leader serves workflow mutations through local submission with no second election layer.
- [ ] Volatile worker registration/wakeup loss cannot lose durable jobs.

## Required evidence

Record workflow graph, all lease grants/renewals/attempts, worker registrations, operation envelopes, accepted results, clock advances, and recovered workflow state.

## Regression and completion gate

Static workflow/worker contracts, client failover, M05/M06 recovery, and all lower accepted demos.

- [ ] All listed cases and acceptance checks pass.
- [ ] Independent verification passes and rejects deliberately invalid evidence.
- [ ] Record exact commands, source identity, configuration, and run location here.
- [ ] Document public API/disk-format changes and completed simplification work.

Evidence: not yet produced.

## Out of scope

Dynamic children, signals, timers beyond lease/retry scheduling, resource-placement policy, and durable actor memory.
