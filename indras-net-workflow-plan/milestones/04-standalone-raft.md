# M04 — Standalone Raft service and local submission

Status: planned

Dependencies: [M02](02-actor-messaging.md)

Read the shared [delivery contract](../PLAN.md#delivery-contract) and
[evidence format](../PLAN.md#evidence-and-verification).

## Observable outcome

Three nodes expose a counter actor backed directly by their local Raft runtime. Clients follow leadership and preserve request identity across reply loss, leader death, and partitions. This demo has no SwiftDB or workflow dependency.

## Scope and boundaries

Introduce RaftCore and RaftRuntime; neither exists in this checkout. Separate
deterministic consensus transitions from persistence, timers, and actor-based
peer communication. Implement a file-backed journal and recovery/snapshot
primitives. Keep the replicated counter example above those libraries; the
existing indras-counter is an in-memory actor example, not a Raft service.
The replicated facade submits locally and returns follower hints.

## Implementation steps

- [ ] Implement election, log replication, quorum commitment, and deterministic core replay.
- [ ] Persist term, vote, log, and snapshot metadata with explicit crash-recovery ordering.
- [ ] Expose a narrow local submit result and leadership-hint interface.
- [ ] Preserve full operation identity and command bytes through calls and redirects.
- [ ] Use one serving TCPActorSystem per node for peer calls and service facades.
- [ ] Implement a counter facade with local submission and leader-following client.
- [ ] Add lost-reply, leader-death, and isolated-former-leader cases.
- [ ] Serve authoritative reads through a committed path that verifies current leadership.
- [ ] Keep application examples outside the reusable consensus targets.

## Runnable demo

Proposed commands; the scenario must exist before these can pass:

~~~sh
swift run indras-demo raft --seed 42 --output .demo/m04
swift run indras-demo verify .demo/m04
~~~

| Case | Driver action | Required observation |
| --- | --- | --- |
| Follower entry | Begin at a follower | Leader hint works; facade submits to its local node |
| Lost reply | Commit increment, lose reply, retry same identity | Original result returns; increment applies once |
| Leader death | Kill leader with requests pending | New leader reconciles preserved requests |
| Minority isolation | Isolate former leader; submit fresh mutations | Healthy majority progresses; minority cannot acknowledge fresh writes |
| Healing/restart | Heal and restart, then perform committed reads | Recovered replicas converge on expected state |
| No quorum | Stop two nodes and attempt mutation | No successful fresh mutation until quorum returns |

## Acceptance criteria

- [ ] Independent counter expectations agree with distinct committed operation identities.
- [ ] Acknowledged mutations survive leader death and full-cluster restart.
- [ ] Ambiguous outcomes reconcile using the same envelope.
- [ ] The facade does not create a network client to forward local submissions.
- [ ] Fault evidence shows affected actor connections.
- [ ] Authoritative reads do not present an isolated stale leader as current.
- [ ] RaftCore/RaftRuntime have no workflow or SwiftDB dependency.

## Required evidence

Include request identities/fingerprints, redirects, leader/term observations, actual partition effects, committed/applied positions, and recovered values. Keep deterministic replay separate from process scheduling.

## Regression and completion gate

Consensus, replay, relevant linearizability/public-API checks, and accepted transport/actor demos.

- [ ] All listed cases and acceptance checks pass.
- [ ] Independent verification passes and rejects deliberately invalid evidence.
- [ ] Record exact commands, source identity, configuration, and run location here.
- [ ] Document public API/disk-format changes and completed simplification work.

Evidence: not yet produced.

## Out of scope

A second election layer, SWIM, new consensus algorithms, read optimization, and dynamic membership in this demo.
