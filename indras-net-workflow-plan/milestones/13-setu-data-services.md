# M13 — Setu migration and workflow data services

Status: standalone transfer migration implemented; content references and
milestone evidence pending. Workflow retention remains planned.

Dependencies: M13a requires [M02](02-actor-messaging.md); M13b requires M13a and
[M07](07-coordinator-workers.md).

Read the shared [delivery contract](../PLAN.md#delivery-contract) and
[evidence format](../PLAN.md#evidence-and-verification).

## Scope and boundaries

Setu was imported from revision `e0accf34dda1df3d3fad2177cd26a673133c9a21` of the
sibling repository. Hashing, manifests, verification, publication, progress,
temporary/recovery storage, node/CLI behavior, and relevant tests are here.
Peer transfers and local Unix-socket control use Networking; HTTP dependencies
and the sibling-checkout dependency are absent.

Keep transfer/storage operations independent of workflows. Targets are
[Setu](../../Sources/Setu) for transport-neutral operations,
[SetuNetworking](../../Sources/SetuNetworking) for the Networking adapter,
and [SetuCLI](../../Sources/SetuCLI) for commands and node management. Keep dependencies acyclic and put
workflow adapters above these targets. Imported code follows this repository's
explicit-argument and compiler-checked Sendable rules.

Setu owns transfer chunking, parallelism, retry identity, and reconciliation.
Each request owns a scoped connection; chunks use raw binary frames and awaited
I/O. Control requests have separate connections. Restart requests are sent once;
publication retry first reconciles the same manifest and destination.
Interrupted partial uploads can resume while the receiver remains alive;
receiver restart clears private partial files and a retry starts fresh. Verified
published files are reconciled after restart. Persistent partial resume remains optional.

Artifact references use content digest and size, with display names as metadata.
The current Setu catalog uses filename-derived IDs and its manifest identity
includes the name and chunking; neither is the immutable content identity.
Ordinary file sends still support destination paths and no-overwrite publication.

## M13a — Standalone transfers

### Observable outcome

Independent processes send and fetch verified files through Networking without
Raft, SwiftDB, or a workflow runtime. Interrupted transfers can be retried, and
lost publication replies can be reconciled against verified destination bytes.

### Implementation steps

- [x] Import reusable Setu operations, node/CLI behavior, and relevant tests with source provenance.
- [x] Expose transport-neutral manifest, verification, and publication operations.
- [ ] Define a versioned immutable content reference.
- [x] Implement versioned begin/status, chunk, commit, and fetch messages over Networking.
- [x] Preserve transfer identity across retries and reconcile ambiguous publication results above TCP.
- [x] Use transfer-owned chunking/parallelism and separate bulk/control connections as needed.
- [ ] Add content-based publish/fetch alongside ordinary filename/destination transfers.
- [x] Preserve verified publication, no-overwrite behavior, progress, and cleanup on failure.
- [x] Replace the peer HTTP path and remove its dependencies when no remaining code uses them.
- [x] Specify interrupted-transfer behavior; persistent partial resume remains optional.

### Runnable demo

Proposed commands; the scenario must exist before these can pass:

~~~sh
swift run indras-demo transfers --seed 42 --output .demo/m13a
swift run indras-demo verify .demo/m13a
~~~

| Case | Driver action | Required observation |
| --- | --- | --- |
| Send/fetch | Transfer empty and binary fixtures, then retrieve | Independent digest and size match |
| Destination handling | Send to a selected directory and try a conflicting existing file | Requested destination is honored; conflicting bytes are preserved |
| Corruption | Alter a chunk or final content | Invalid bytes are rejected before accepted publication |
| Interrupted transfer | Kill peer mid-upload and retry | Correct final file; no partial accepted artifact |
| Duplicate publish | Publish identical content with different display names | Content identity stays stable; metadata does not overwrite bytes |
| Lost publication reply | Cut after publication and retry | Existing verified file is reconciled |
| Control under load | Transfer while issuing actor calls | Backpressure is observed and control calls remain serviceable |

### Acceptance criteria

- [ ] Standalone node and transfer commands build and run from this repository.
- [ ] A clean checkout has no dependency on the sibling Setu checkout.
- [ ] Data/control paths use Networking and preserve transfer verification/publication behavior.
- [ ] Content references identify bytes independently of filename and transfer chunking.
- [ ] Failed transfers leave no falsely published partial file or overwritten destination.
- [ ] Retries preserve identity and reconcile unknown outcomes without TCP-level replay.
- [ ] Setu remains usable without Raft, SwiftDB, or workflow modules.

### Required evidence and completion gate

Include source provenance, transfer/chunk manifests, independent content hashes,
publication receipts, fault effects, backpressure observations, and process cleanup.
Run imported verification/publication/retry checks and M00–M02 regressions.

- [ ] All M13a cases and acceptance checks pass.
- [ ] Independent verification passes and rejects deliberately invalid evidence.
- [ ] Record exact commands, configuration, run location, and public API/disk-format changes.

M13a milestone evidence: pending the M00 harness/verifier and remaining content-reference work.

~~~sh
# Run imported regressions and framed transport fault checks.
swift test
~~~

## M13b — Workflow artifact retention

### Observable outcome

Workers publish and fetch immutable artifacts through the migrated Setu service.
An archive handler stores two verified copies and records retention receipts;
the data remains fetchable after the producer and one archive node disappear.

### Implementation steps

- [ ] Add a publish/fetch/verify/retain interface for workflow handlers.
- [ ] Define retention receipts with content identity, storage destination identity, and retention policy.
- [ ] Publish durable bytes and obtain required retention receipts before accepting workflow references.
- [ ] Implement archive as an ordinary handler copying to two configured independent destinations.
- [ ] Keep retention/replica policy in the data service or handler above the transfer library.
- [ ] Exercise bulk transfer while workers renew leases through their shared actor system.

### Runnable demo

Proposed commands; the scenario must exist before these can pass:

~~~sh
swift run indras-demo artifacts --seed 42 --output .demo/m13b
swift run indras-demo verify .demo/m13b
~~~

| Case | Driver action | Required observation |
| --- | --- | --- |
| Accepted publication | Complete a step with a required artifact | Verified durable copies and receipts precede accepted completion |
| Missing retention | Withhold required storage acknowledgement | Workflow completion does not accept the reference |
| Producer/archive loss | Stop producer and one of two archive destinations | Remaining retained copy is discoverable and verifies |
| Control under load | Transfer while workers renew leases | Control remains serviceable and transfer backpressure is observed |

### Acceptance criteria

- [ ] Accepted references meet the configured durable-copy/retention policy.
- [ ] Archive receipts identify independent configured storage destinations.
- [ ] Artifact bytes are not embedded in the Raft command history.
- [ ] Workflow completion waits for required storage acknowledgements.
- [ ] Required artifacts remain fetchable after the specified producer/archive loss.
- [ ] Transfer libraries retain their independence from workflow modules.

### Required evidence and completion gate

Include retention receipts, actual destination identities, committed completion
ordering, fault effects, and retrieval after node loss. Run M13a and M07
lease/control regressions alongside earlier accepted demos.

- [ ] All M13b cases and acceptance checks pass.
- [ ] Independent verification passes and rejects deliberately invalid evidence.
- [ ] Record exact commands, configuration, run location, and public API/disk-format changes.

M13b evidence: not yet produced.

## Out of scope

A general object-storage platform, cross-region replication, SWIM discovery,
automatic placement, and mandatory partial-transfer persistence.
