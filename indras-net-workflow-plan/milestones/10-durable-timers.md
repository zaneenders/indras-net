# M10 — Durable workflow timers

Status: planned

Dependencies: [M09](09-signals-approvals.md)

Read the shared [delivery contract](../PLAN.md#delivery-contract) and
[evidence format](../PLAN.md#evidence-and-verification).

## Observable outcome

A workflow schedules a deadline, the cluster goes down before it is due, and recovery makes the continuation ready through one accepted timer transition. Leader changes and duplicate callbacks do not fire it twice.

## Scope and boundaries

Store timer ID, deadline, workflow/run correlation, and state in replicated
records. Use explicit committed authoritative time inputs for deterministic
transitions. Local sleeps only wake the scheduler; they do not establish durable
timer state or independently authorize work.

## Implementation steps

- [ ] Define schedule/fire timer commands with stable timer identity and immutable deadline.
- [ ] Integrate leader-owned timer driving with M07's authoritative lease-clock boundary.
- [ ] Persist pending/fired timer state and continuation readiness atomically.
- [ ] Recover overdue timers from records after startup and leadership changes.
- [ ] Bound catch-up batches so overdue timers cannot bypass normal admission limits.
- [ ] Discard stale local wakeups by rechecking committed timer/run state.
- [ ] Provide virtual-time model checks alongside real-process restart demonstrations.

## Runnable demo

Proposed commands; the scenario must exist before these can pass:

~~~sh
swift run indras-demo timers --seed 42 --output .demo/m10
swift run indras-demo verify .demo/m10
~~~

| Case | Driver action | Required observation |
| --- | --- | --- |
| Not yet due | Advance authoritative time below deadline | Timer stays pending and continuation is not ready |
| Deadline reached | Advance to or beyond deadline | One accepted firing makes continuation ready |
| Downtime | Stop all coordinators before deadline; restart after it | Pending deadline recovers and can fire |
| Leader replacement | Replace leader with pending timers | New driver reconstructs timers from durable state |
| Duplicate callback | Deliver stale/repeated wake/fire proposals | No duplicate accepted firing or continuation |
| Invalid retry | Reuse timer ID with changed deadline | Conflict; original timer is preserved |

## Acceptance criteria

- [ ] Timer readiness is determined by committed authoritative time, not arbitrary local callbacks.
- [ ] No firing is accepted before the declared deadline under that clock.
- [ ] Timer state and dependent readiness change atomically.
- [ ] Pending deadlines survive full restart without a saved in-memory task.
- [ ] Repeated wakeups and leader changes cannot duplicate accepted firing.
- [ ] Overdue catch-up remains bounded and observable.
- [ ] Virtual-time expectations and real-process recovery results agree.

## Required evidence

Include scheduled deadlines, authoritative clock commands, scheduler ownership, actual wake/proposal events, firing positions, and recovered timer records.

## Regression and completion gate

M09 durable waits/signals, lease-clock behavior, replicated recovery, and prior accepted demos.

- [ ] All listed cases and acceptance checks pass.
- [ ] Independent verification passes and rejects deliberately invalid evidence.
- [ ] Record exact commands, source identity, configuration, and run location here.
- [ ] Document public API/disk-format changes and completed simplification work.

Evidence: not yet produced.

## Out of scope

Cron/calendar syntax, precise physical-clock guarantees, a general time service, and waking external systems without workflow records.
