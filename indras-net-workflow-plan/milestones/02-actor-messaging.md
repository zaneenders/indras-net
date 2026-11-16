# M02 — Shared actor messaging

Status: actor messaging implemented; process demo and evidence acceptance pending.

Dependencies: [M01](01-tcp-transport.md)

Read the shared [delivery contract](../PLAN.md#delivery-contract) and
[evidence format](../PLAN.md#evidence-and-verification).

## Observable outcome

Two processes host multiple named services, exchange typed calls/errors, and perform nested callbacks through shared actor systems. Named services can be looked up again after restart; old actor references retain the old node identity. A lost mutation reply is reported without automatic replay.

## Scope and boundaries

Use [TCPActorSystem](../../Sources/TCPActorSystem/TCPActorSystem.swift),
[RPCTransport](../../Sources/TCPActorSystem/RPCTransport.swift), and
[actor tests](../../Tests/TCPActorSystemTests/TCPActorSystemTests.swift).
The layer owns addressing, envelopes, correlation, and call lifetime. Codable
argument/result encoding may remain JSON. Durable operation ledgers belong above it.

Named publication/lookup, typed errors, callbacks, concurrent reply correlation,
timeouts, cancellation, disconnect/shutdown, and stale-node rejection have test
coverage. The [counter example](../../Sources/IndrasCounter/CounterDemo.swift)
exercises calls between serving systems. Admission controls and the process-level
fault/evidence scenario remain planned.

## Implementation steps

- [ ] Document named service lifetimes, ephemeral actor IDs, and socket identities.
- [x] Build a public-API counter demonstrating typed results, errors, and callbacks.
- [ ] Use one serving actor system to call multiple peers through its shared registry.
- [ ] Define bounded pending-call/inbound-execution admission where needed.
- [ ] Add a reply-loss barrier after a demo counter mutation and before acknowledgement.
- [ ] Exercise simultaneous dialing, cancellation, shutdown, and peer replacement.
- [ ] Keep future service adapters on the serving system's shared registry and transport.

## Runnable demo

Proposed commands; the scenario must exist before these can pass:

~~~sh
swift run indras-demo actors --seed 42 --output .demo/m02
swift run indras-demo verify .demo/m02
~~~

| Case | Driver action | Required observation |
| --- | --- | --- |
| Shared services | Call two services and perform a callback | Correct typed results and intended runtime/connection reuse |
| Application failure | Invoke a declared failing method | Caller decodes its typed error |
| Simultaneous dial | Both serving systems dial | Stable communication with documented duplicate-connection handling |
| Lost reply | Increment locally, cut before response | Caller receives unknown/failure outcome; reconnect alone does not increment again |
| Restart | Replace process, republish service name, and look it up again | Fresh reference works on a new connection; stale node identity is rejected |
| Cancel/overload | Cancel or exceed configured admission | Calls terminate and admitted resources are released |

## Acceptance criteria

- [ ] Replies correlate to the correct call and socket incarnation.
- [ ] Late replies from a replaced socket cannot finish new calls.
- [ ] Nested callbacks use the intended duplex connection.
- [ ] Disconnect/shutdown resolves pending calls without silently replaying mutations.
- [ ] Published services retain their actors until explicitly unpublished.
- [ ] Admission limits and timeout/cancellation cleanup are checked.
- [ ] The actor demo has no workflow or storage dependency.

## Required evidence

Include service/call/connection identities, typed failures, cleanup counts, and a fresh query of the counter after reply loss. Querying final state is separate from the failed mutation call.

## Regression and completion gate

Actor lifecycle, reconnection, service messaging, and earlier accepted transport checks.

- [ ] All listed cases and acceptance checks pass.
- [ ] Independent verification passes and rejects deliberately invalid evidence.
- [ ] Record exact commands, source identity, configuration, and run location here.
- [ ] Document public API/disk-format changes and completed simplification work.

Evidence: not yet produced.

## Out of scope

Durable RPC retry policy, persisting actor memory, leadership, and a new argument codec.
