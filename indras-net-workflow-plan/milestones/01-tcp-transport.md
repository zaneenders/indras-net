# M01 — Protocol-neutral TCP transport

Status: transport implemented; process demo and evidence acceptance pending.

Dependencies: [M00](00-demo-harness.md)

Read the shared [delivery contract](../PLAN.md#delivery-contract) and
[evidence format](../PLAN.md#evidence-and-verification).

## Observable outcome

Two independent processes exchange known binary messages. A receiver restart produces a new connection and permits a fresh exchange without replaying earlier application messages.

## Scope and boundaries

Use [Frame](../../Sources/Networking/Frame.swift),
[FrameCodec](../../Sources/Networking/FrameCodec.swift),
[Connection](../../Sources/Networking/Connection.swift), and
[TCP](../../Sources/Networking/TCP.swift). Networking owns opaque ByteBuffer
frames, scoped sockets, and NIO async backpressure. The wire header contains
UInt32 payload length, target, and opcode fields in big-endian order.

[Existing tests](../../Tests/NetworkingTests) cover wire fixtures, fragmented and
coalesced frames, binary traffic, half-close, cancellation, and socket cleanup.
TCP.connect opens one connection; callers explicitly open a replacement after
loss and decide what to send. This layer has no actor, Raft, job, or JSON requirement.

## Implementation steps

- [x] Implement the wire byte order, length representation, and truncated-frame close behavior.
- [x] Expose public connect/listen and scoped duplex connection APIs.
- [ ] Implement a public-API binary echo/checksum helper.
- [ ] Add fragmented-header/body and coalesced-frame driver cases.
- [ ] Use slow-reader barriers to exercise NIO read/write backpressure.
- [ ] Record disconnect notifications, socket incarnations, and caller-managed replacement/shutdown behavior.
- [x] Add codec and lifecycle regression coverage.

## Runnable demo

Proposed commands; the scenario must exist before these can pass:

~~~sh
swift run indras-demo tcp --seed 42 --output .demo/m01
swift run indras-demo verify .demo/m01
~~~

| Case | Driver action | Required observation |
| --- | --- | --- |
| Binary exchange | Send empty, binary, and varied-size payloads | Exact bytes and frame count match input fixtures |
| Fragment/coalesce | Split headers/bodies; combine adjacent frames | Same decoded message sequence |
| Truncated frame | Close mid-header or mid-payload | Defined error/closure; no fabricated message |
| Slow receiver | Hold reads until writes encounter backpressure, then resume | Producer suspension is observed; traffic completes after reads resume |
| Receiver restart | Cut, restart, send a new message | New connection identity; no automatic old-message replay |
| Shutdown while dialing | Cancel a caller-owned replacement attempt | No continuing dial task or retained socket |

## Acceptance criteria

- [ ] Decoding is independent of socket read boundaries.
- [ ] Payload length overflow fails encoding; EOF within a frame fails decoding.
- [ ] Slow-reader backpressure and recovery are observed independently of kernel buffering.
- [ ] Each socket loss has the documented notification and replacement identity.
- [ ] Replacement is explicit, and cancellation terminates caller-owned connection attempts.
- [ ] Send results/errors are observable and replay policy remains with callers.
- [ ] The demo imports Networking without actor, database, or Raft modules.

## Required evidence

Record sent/received sequence numbers, lengths, fixture digests, frame errors, backpressure observations, and old/new connection IDs. Derive expected results from the input fixture independently.

## Regression and completion gate

Networking codec/lifecycle checks and M00.

- [ ] All listed cases and acceptance checks pass.
- [ ] Independent verification passes and rejects deliberately invalid evidence.
- [ ] Record exact commands, source identity, configuration, and run location here.
- [ ] Document public API/disk-format changes and completed simplification work.

Evidence: not yet produced.

## Out of scope

RPC correlation, durable delivery, application deduplication, TLS, and artifact protocols.
