# M09 — Durable signals and approval waits

Status: planned

Dependencies: [M08](08-dynamic-steps.md)

Read the shared [delivery contract](../PLAN.md#delivery-contract) and
[evidence format](../PLAN.md#evidence-and-verification).

## Observable outcome

A workflow reaches an explicit approval wait, releases worker capacity, and resumes after a durable signal. Signals arriving before the wait or being retried after reply loss are handled deterministically.

## Scope and boundaries

Add a durable workflow inbox/wait model above the replicated store.
Identify each logical wait and input explicitly with workflow/run ID, wait
correlation, and signal ID. The workflow state owns acceptance/consumption; an
in-memory continuation or actor mailbox is not the recovery record.

## Implementation steps

- [ ] Specify named/correlated waits and one-consumption-per-wait semantics.
- [ ] Persist signal identity, payload fingerprint, acceptance, and consumption.
- [ ] Handle input that arrives before its matching wait is established.
- [ ] Make wait registration/consumption and continuation readiness atomic transitions.
- [ ] Return worker capacity while a workflow waits instead of holding a handler alive.
- [ ] Expose a public signal/approval command with stable retry identity.
- [ ] Define rejection for unknown/closed workflows and conflicting signal-ID reuse.

## Runnable demo

Proposed commands; the scenario must exist before these can pass:

~~~sh
swift run indras-demo signals --seed 42 --output .demo/m09
swift run indras-demo verify .demo/m09
~~~

| Case | Driver action | Required observation |
| --- | --- | --- |
| Approval after wait | Commit wait, then send approval | Matching continuation becomes ready once |
| Early approval | Commit signal before the matching wait | Signal remains durable and is consumed when the wait is established |
| Lost reply | Retry signal after acceptance acknowledgement is lost | No duplicate consumption or continuation |
| Conflicting input | Reuse signal ID with changed payload | Conflict without changing accepted input |
| Full restart | Restart cluster while waiting or after signal acceptance | Inbox/wait state and progress recover |
| Multiple waits | Signal one explicit correlation | Only its intended wait resumes |

## Acceptance criteria

- [ ] Accepted signals survive coordinator and full-cluster restart.
- [ ] Duplicate delivery cannot resume a logical wait twice.
- [ ] Signals received before a wait are not silently lost.
- [ ] No worker slot or actor continuation must remain alive to preserve the wait.
- [ ] Signal acceptance/consumption and new ready work are transactionally consistent.
- [ ] Payload conflicts and unknown/closed-run behavior have explicit public results.
- [ ] An independent inbox/wait model agrees with final records.

## Required evidence

Record signal IDs/fingerprints, wait correlations, acceptance/consumption positions, worker capacity while waiting, and resumed step identities.

## Regression and completion gate

M08 atomic scheduling, operation deduplication, restart recovery, and earlier workflow demos.

- [ ] All listed cases and acceptance checks pass.
- [ ] Independent verification passes and rejects deliberately invalid evidence.
- [ ] Record exact commands, source identity, configuration, and run location here.
- [ ] Document public API/disk-format changes and completed simplification work.

Evidence: not yet produced.

## Out of scope

Email/chat delivery, external approval UIs, broadcast semantics, and general actor-mailbox persistence.
