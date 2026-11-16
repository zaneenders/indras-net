# Durable workflow system: implementation plan

Status: roadmap. Networking, actor messaging, and standalone Setu transfers are
implemented; milestone demo acceptance is pending. Later layers are planned.

## Objective and layers

Build a durable workflow engine with distributed actor workers from independently
useful layers. Build/test/render farms and agent workflow networks use the same
runtime through application handlers. Workers compose optional services such as
Setu for data transfer, sharing, and archiving.

| Layer | Owns | Boundary |
| --- | --- | --- |
| TCP transport | Connections, framing, backpressure, scoped socket ownership | Opaque bytes; callers own replacement connections and replay policy |
| Actor messaging | Actor addressing, calls, replies, errors | Uses TCP; callers decide whether operations can be replayed |
| Setu transfers | File verification, publication, transfer and retry reconciliation | Uses Networking; independently usable without workflows |
| Raft | Ordered, committed decisions | Independently usable consensus core and runtime |
| SwiftDB | Local durable records and transactions | Independently usable storage; no scheduling rules |
| Replicated store | Durable application and consistent recovery | Composes Raft and SwiftDB without workflow-specific rules |
| Workflow runtime | Dependencies, leases, retries, timers, signals, completion | Uses replicated storage and actor messaging |
| Application handlers | Building, testing, reviewing, rendering, archiving | Execute outside consensus and compose explicit services |

The planned Raft layer retains its consensus journal; SwiftDB stores application
records derived from committed commands. Each coordinator owns its local storage
directory.
Workers are replaceable; accepted workflow state and required data survive their
producing actors.

Preserve the Networking and TCPActorSystem target names. Setu now uses Networking
for peer transfers and local Unix-socket control. Add storage,
consensus, replicated-store, and workflow targets as their milestones require.
Keep Raft, SwiftDB, Setu, and standalone storage mode independently useful.

## Current checkout

| Target | Implemented behavior |
| --- | --- |
| [Networking](../Sources/Networking) | Duplex binary frames, fragmentation/coalescing, NIO async backpressure, scoped connections, half-close and truncated-frame handling |
| [TCPActorSystem](../Sources/TCPActorSystem) | Named services, actor addressing, typed calls/errors, callbacks, reply correlation, timeouts/cancellation, disconnect and shutdown handling |
| [IndrasCounter](../Sources/IndrasCounter) | `indras-counter serve` and `check`, configuration, concurrent calls, callbacks, typed errors and timeout/no-replay checks |
| [Setu](../Sources/Setu) | Manifests, hashing, chunk verification, file publication, progress and temporary/recovery storage |
| [SetuNetworking](../Sources/SetuNetworking) | Versioned transfer/control messages, raw binary chunk frames, retry reconciliation and node lifecycle over Networking |
| [SetuCLI](../Sources/SetuCLI) | `setu` transfer, node, job, peer discovery and service setup commands |

[NetworkingTests](../Tests/NetworkingTests),
[TCPActorSystemTests](../Tests/TCPActorSystemTests), [SetuTests](../Tests/SetuTests),
[SetuNetworkingTests](../Tests/SetuNetworkingTests), and
[SetuCLITests](../Tests/SetuCLITests) cover these implementations.
Current commands are in the [README](../README.md).

There is no Raft, SwiftDB, replicated store, workflow coordinator, or worker
runtime in this checkout. The `indras-demo` runner, process/fault harness, and
offline evidence verifier are also planned. Existing tests and the counter
example do not establish completion of M00–M02's demo acceptance gates.

Setu was imported from revision `e0accf34dda1df3d3fad2177cd26a673133c9a21` of the
sibling repository. This package has no dependency on that checkout or its HTTP
transport. Communicating nodes and their local CLIs use the new framed protocol.
M13a's immutable content references remain planned; file catalog IDs still derive
from filenames. M13b adds workflow artifact retention and archiving later.

Reuse code when it satisfies a milestone contract. Preserve unrelated staged and
unstaged work. Existing test results are regression evidence; completion requires
the milestone's own runnable demonstration.

## Milestone index

M01, M02, and M13a's transfer migration have implementation and regression tests;
their milestone evidence remains pending. M13a's content-reference work and other
milestones are planned. Dependencies are
completion gates, not a requirement to combine libraries into one target.

| ID | Milestone | Depends on | Observable result |
| --- | --- | --- | --- |
| M00 | [Demo harness](milestones/00-demo-harness.md) | None | Process control, actual fault injection, and verifiable evidence |
| M01 | [TCP transport](milestones/01-tcp-transport.md) | M00 | Binary exchange through fragmentation, backpressure, and explicit reconnection |
| M02 | [Actor messaging](milestones/02-actor-messaging.md) | M01 | Shared services, callbacks, typed failures, and reconnect |
| M03 | [SwiftDB storage](milestones/03-swiftdb-storage.md) | M00 | Atomic records and versioned checkpoint export/restore |
| M04 | [Standalone Raft](milestones/04-standalone-raft.md) | M02 | Replicated counter, local submission, and client failover |
| M05 | [Durable replicated application](milestones/05-replicated-application.md) | M03, M04 | Atomic state, request results, and applied index |
| M06 | [Replicated checkpoints](milestones/06-replicated-checkpoints.md) | M05 | Snapshot installation, replay, and safe compaction |
| M07 | [Coordinator and workers](milestones/07-coordinator-workers.md) | M02, M06 | Static workflows survive worker and coordinator failure |
| M08 | [Dynamic steps](milestones/08-dynamic-steps.md) | M07 | Parent completion and child creation are atomic |
| M09 | [Signals and approvals](milestones/09-signals-approvals.md) | M08 | Durable input resumes a wait once |
| M10 | [Durable timers](milestones/10-durable-timers.md) | M09 | Deadlines survive downtime and leader changes |
| M11 | [Cancellation](milestones/11-workflow-cancellation.md) | M10 | Defined ordering and rejection of stale progress |
| M12 | [Version compatibility](milestones/12-version-compatibility.md) | M11 | Old runs recover under a documented upgrade policy |
| M13a | [Standalone Setu migration](milestones/13-setu-data-services.md#m13a--standalone-transfers) | M02 | Verified file transfers through Networking without workflows |
| M13b | [Workflow artifact retention](milestones/13-setu-data-services.md#m13b--workflow-artifact-retention) | M13a, M07 | Retained artifacts and archive receipts survive producer/node loss |
| M14 | [Reference application](milestones/14-reference-application.md) | M08–M12, M13b | Plan, compile, test/review, approve, archive |
| M15 | [Sustained operation](milestones/15-sustained-operation.md) | M14 | Retention and reclamation under small quotas |

Establish M00–M02's process/fault evidence and complete M13a's content-reference
work and milestone verification. M03 and M04 are independent after their prerequisites and
precede replicated storage and workflows. M13b follows M07 and M13a. Milestone
numbers retain their identifiers; they are not a strict implementation order.

## Delivery contract

Each milestone delivers:

1. A small public API and an example consuming it.
2. One command exercising the real implementation and checking its results.
3. The specified failure cases with explicit expected outcomes.
4. Saved evidence for independent verification.
5. Regression checks for affected lower layers and earlier accepted milestones.
6. Removal or deliberate isolation of replaced implementation paths.

Examples use public APIs. Fault hooks may live in dedicated test/demo helpers;
production callers must not need private APIs. Build the process and fault
helpers needed by the counter first, then reuse them for later demos.

Proposed command interface:

~~~sh
swift run indras-demo harness --output .demo/harness
swift run indras-demo actors --seed 42 --output .demo/actors
swift run indras-demo transfers --seed 42 --output .demo/transfers
swift run indras-demo replicated-store --seed 42 --output .demo/store
swift run indras-demo verify .demo/store
swift run indras-demo all --seed 42 --output .demo/all
~~~

These commands are planned. A requested unimplemented scenario must report that
fact and return nonzero. During development run the current milestone and earlier
accepted scenarios. The final "all" suite requires every published scenario and
reports coverage explicitly. Do not silently skip a required check.

Use fresh output directories or explicit replacement. Do not mix unrelated runs.

## Evidence and verification

| File or directory | Contents |
| --- | --- |
| manifest.json | Scenario/version, source/build fingerprint including relevant uncommitted changes, toolchain/dependencies, configuration, seed, endpoints, and deadlines |
| operations.jsonl | Operation identities, command fingerprints, attempts, acknowledgements, and ambiguous outcomes |
| events.jsonl | Readiness, observations, requested and actual faults, barriers, and process exits |
| assertions.json | Named checks, expected/observed values, and evidence references |
| result.json | Passed/failed/incomplete status, required-case coverage, and summary |
| logs/ | Per-process diagnostics |
| state/ | Relevant recovered records and checkpoint evidence |
| artifacts/ | Transfer manifests and checksums where applicable |

Keep relevant evidence, including diagnostics on failure. Missing evidence, an
unexercised requested fault, or a failed assertion causes verification and the
demo to return nonzero.

Compare submitted operations, recovered records, and artifact hashes with a small
independent expected model. Do not use the production transition function as its
own oracle. Altering known-good evidence must make verification reject the run.

Seeds reproduce generated choices, not OS scheduling. Deterministic core replay
and real-process tests are separate checks. Use explicit persistence barriers
and condition-based waits with declared deadlines, not fixed sleeps as readiness.
Network fault routes must affect the actual Networking connections used by
actors and transfers without depending on application-specific frame parsing.

## Recovery contracts

- A disconnected RPC may have an unknown outcome; reconnect does not authorize replay.
- Preserve operation identity and command bytes through retries and redirects.
- Unacknowledged storage changes may be absent or wholly committed; acknowledged
  durable changes survive the failures exercised by the demo.
- Worker attempts can repeat. Accepted completion and successor creation follow
  committed state-machine rules.
- External effects need idempotency or reconciliation; a lease token alone does
  not prevent an external service from observing repeated effects.
- A checkpoint binds state, request results, and Raft position. Publish it durably
  before removing the log prefix required for recovery.
- Required artifacts meet their retention/availability policy before completion
  accepts their references.

## Layering gates

- M02/M04/M07: share serving actor systems across peers and submit coordinator
  operations to the local Raft runtime.
- M04: use actor messaging for peer communication and keep example applications
  above RaftCore/RaftRuntime.
- M03/M05: build generic storage with versioned formats before adding job adapters.
- M13a: import Setu's reusable transfer code and tests, replace peer HTTP with
  Networking, and keep workflow dependencies above the transfer library.
- M14: keep concrete handlers and optional integrations above generic modules.
- M15: exercise quotas, retention, and reclamation through repeated cleanup cycles.

Provide standalone storage mode, deterministic replay, process-death tests, and
linearizability checks as the relevant layers are introduced.

## Completion policy

A milestone becomes complete when its document links to a fresh evidence run,
records the exact commands/configuration, and has every acceptance item checked.
Record intentional public API and disk-format changes. A later change that breaks
an accepted milestone must restore its checks.

M14 demonstrates the composed system; M15 additionally establishes the specified
bounded sustained-operation behavior. Deployment, security, and production
performance requirements are separate from these local demonstrations.

Defer SWIM, placement-aware push dispatch, cross-cluster discovery, arbitrary
async-function checkpointing, mandatory external LLM calls, and new consensus
protocols until a concrete application requires them.
