import Foundation
import NIOCore
import Networking
import TCPActorSystem
import Testing

@Suite(.timeLimit(.minutes(1)))
struct DataTransferTests {
  @Test func transfersBytesWhileRPCRemainsUsableAndConsumesTheTicketOnce() async throws {
    try await withSystems(host: "127.0.0.1", port: 0, callTimeout: .seconds(2), connectTimeout: .seconds(2)) {
      receiver, sender in
      let sink = MemorySink()
      let ticket = try await receiver.transfers.offer(to: sender.endpoint, byteCount: 3) { .sink(sink) }
      #expect(ticket.endpoint == receiver.endpoint)
      // A different peer cannot consume the intended sender's grant.
      await #expect(throws: (any Error).self) {
        try await receiver.transfers.transfer(ticket) { .source(MemorySource(bytes: ByteBuffer(), beforeRead: {})) }
      }
      let (started, signal) = AsyncStream<Void>.makeStream(bufferingPolicy: .bufferingNewest(1))
      let (resume, gate) = AsyncStream<Void>.makeStream(bufferingPolicy: .bufferingNewest(1))
      defer {
        gate.finish()
        signal.finish()
      }
      async let transfer = sender.transfers.transfer(ticket) {
        .source(
          MemorySource(
            bytes: ByteBuffer(string: "abc"),
            beforeRead: {
              signal.yield()
              for await _ in resume { break }
            }))
      }
      for await _ in started { break }
      let counter = TestCounter(actorSystem: receiver, started: nil)
      let remote = try TestCounter.resolve(id: counter.id, using: sender)
      #expect(try await remote.add(1) == 1)
      gate.finish()
      let receipt = try await transfer
      #expect(receipt.sha256 == "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad")
      #expect(await sink.published == ByteBuffer(string: "abc"))
      #expect(await receiver.transfers.receipt(for: ticket.id) == receipt)
      await #expect(throws: (any Error).self) {
        try await sender.transfers.transfer(ticket) { .source(MemorySource(bytes: ByteBuffer(), beforeRead: {})) }
      }
    }
  }

  @Test func truncatedSourceAbortsTheSinkWithoutAReceipt() async throws {
    try await withSystems(host: "127.0.0.1", port: 0, callTimeout: .seconds(2), connectTimeout: .seconds(2)) {
      receiver, sender in
      let sink = MemorySink()
      let ticket = try await receiver.transfers.offer(to: sender.endpoint, byteCount: 4) { .sink(sink) }
      await #expect(throws: (any Error).self) {
        try await sender.transfers.transfer(ticket) {
          .source(MemorySource(bytes: ByteBuffer(string: "abc"), beforeRead: {}))
        }
      }
      // Server shutdown waits for the handler to release its sink after the disconnect.
      await receiver.shutdown()
      #expect(await sink.aborted)
      #expect(await sink.published == nil)
      #expect(await receiver.transfers.receipt(for: ticket.id) == nil)
    }
  }

  @Test func storageFailureReachesTheSenderDuringASuspendedRead() async throws {
    try await withSystems(host: "127.0.0.1", port: 0, callTimeout: .seconds(2), connectTimeout: .seconds(2)) {
      receiver, sender in
      let (reading, signal) = AsyncStream<Void>.makeStream(bufferingPolicy: .bufferingNewest(1))
      defer { signal.finish() }
      let ticket = try await receiver.transfers.offer(to: sender.endpoint, byteCount: 2) {
        .sink(RejectingSink(sourceReading: reading))
      }

      do {
        _ = try await sender.transfers.transfer(ticket) { .source(SuspendingSource(reading: signal)) }
        Issue.record("Expected the receiver's storage error")
      } catch let error as TransferError {
        #expect(error.description == "Storage write failed")
      }
      #expect(await receiver.transfers.receipt(for: ticket.id) == nil)
    }
  }

  @Test func mismatchedDigestAbortsPublication() async throws {
    try await withSystems(host: "127.0.0.1", port: 0, callTimeout: .seconds(2), connectTimeout: .seconds(2)) {
      receiver, sender in
      let sink = MemorySink()
      let ticket = try await receiver.transfers.offer(to: sender.endpoint, byteCount: 3) { .sink(sink) }
      let connection = try await TCP.connect(
        host: ticket.endpoint.host, port: ticket.endpoint.port, connectTimeout: .seconds(2))
      try await connection.executeThenClose { inbound, outbound in
        struct Opening: Encodable {
          let version: Int
          let ticket: UUID
          let peer: ActorEndpoint
        }
        struct Finish: Encodable {
          let id: UUID
          let byteCount: Int64
          let sha256: String
        }
        let target: UInt32 = 0x4441_5441
        var input = inbound.makeAsyncIterator()
        _ = try #require(try await input.next())
        let header = Opening(version: 1, ticket: ticket.id, peer: sender.endpoint)
        try await outbound.write(
          Frame(target: target, opcode: 1, payload: ByteBuffer(bytes: JSONEncoder().encode(header))))
        #expect(try await input.next()?.opcode == 2)
        try await outbound.write(Frame(target: target, opcode: 3, payload: ByteBuffer(string: "abc")))
        let finish = Finish(id: ticket.id, byteCount: 3, sha256: String(repeating: "0", count: 64))
        try await outbound.write(
          Frame(target: target, opcode: 4, payload: ByteBuffer(bytes: JSONEncoder().encode(finish))))
        #expect(try await input.next()?.opcode == 6)
      }
      #expect(await sink.published == nil)
      #expect(await sink.aborted)
      #expect(await receiver.transfers.receipt(for: ticket.id) == nil)
    }
  }

  @Test func zeroLengthPayloadStillCommitsAndReceivesAReceipt() async throws {
    try await withSystems(host: "127.0.0.1", port: 0, callTimeout: .seconds(2), connectTimeout: .seconds(2)) {
      receiver, sender in
      let sink = MemorySink()
      let ticket = try await receiver.transfers.offer(to: sender.endpoint, byteCount: 0) { .sink(sink) }
      let receipt = try await sender.transfers.transfer(ticket) {
        .source(MemorySource(bytes: ByteBuffer(), beforeRead: {}))
      }
      #expect(receipt.byteCount == 0)
      #expect(await sink.published == ByteBuffer())
    }
  }

  @Test(arguments: [false, true])
  func disconnectCancelsASuspendedRead(outbound: Bool) async throws {
    try await withSystems(host: "127.0.0.1", port: 0, callTimeout: .seconds(2), connectTimeout: .seconds(2)) {
      reader, receiver in
      let (started, start) = AsyncStream<Void>.makeStream()
      let (blocked, release) = AsyncStream<Void>.makeStream()
      let (ended, end) = AsyncStream<Bool>.makeStream()
      let source = MemorySource(bytes: ByteBuffer(string: "x")) {
        start.yield(())
        for await _ in blocked { break }
        end.yield(Task.isCancelled)
      }
      let sink = MemorySink()
      let transfer: Task<TransferReceipt, any Error>
      if outbound {
        let ticket = try await receiver.transfers.offer(to: reader.endpoint, byteCount: 1) { .sink(sink) }
        transfer = Task { try await reader.transfers.transfer(ticket) { .source(source) } }
      } else {
        let ticket = try await reader.transfers.offer(to: receiver.endpoint, byteCount: 1) { .source(source) }
        transfer = Task { try await receiver.transfers.transfer(ticket) { .sink(sink) } }
      }
      let watchdog = Task {
        try await Task.sleep(for: .seconds(5))
        start.finish()
        release.finish()
      }
      defer {
        watchdog.cancel()
        start.finish()
        release.finish()
        end.finish()
        transfer.cancel()
      }
      var starts = started.makeAsyncIterator()
      _ = try #require(await starts.next())
      await receiver.shutdown()
      var ends = ended.makeAsyncIterator()
      #expect(await ends.next() == true)
      await #expect(throws: (any Error).self) { try await transfer.value }
      #expect(await sink.published == nil)
    }
  }

  @Test func losingTheAcknowledgementPreservesTheExpectedReceipt() async throws {
    try await withSystems(host: "127.0.0.1", port: 0, callTimeout: .seconds(2), connectTimeout: .seconds(2)) {
      receiver, sender in
      let (started, start) = AsyncStream<Void>.makeStream()
      let (paused, resume) = AsyncStream<Void>.makeStream()
      let sink = PausedCommitSink(started: start, resume: paused)
      let ticket = try await receiver.transfers.offer(to: sender.endpoint, byteCount: 0) { .sink(sink) }
      let transfer = Task {
        try await sender.transfers.transfer(ticket) { .source(MemorySource(bytes: ByteBuffer(), beforeRead: {})) }
      }
      defer { resume.finish() }
      var events = started.makeAsyncIterator()
      _ = try #require(await events.next())
      transfer.cancel()
      do {
        _ = try await transfer.value
        Issue.record("Expected an unconfirmed commit")
      } catch let uncertain as CommitUnconfirmed {
        #expect(uncertain.expected.id == ticket.id)
        resume.finish()
        await receiver.shutdown()
        #expect(await receiver.transfers.receipt(for: ticket.id) == uncertain.expected)
      }
    }
  }

  @Test(arguments: [false, true])
  func stoppingClosesTransferSocketDuringACommit(shutdown: Bool) async throws {
    try await withSystems(host: "127.0.0.1", port: 0, callTimeout: .seconds(2), connectTimeout: .seconds(2)) {
      server, client in
      let (started, signal) = AsyncStream<Void>.makeStream(bufferingPolicy: .bufferingNewest(1))
      let (resume, gate) = AsyncStream<Void>.makeStream(bufferingPolicy: .bufferingNewest(1))
      let (closed, sourceClosed) = AsyncStream<Void>.makeStream(bufferingPolicy: .bufferingNewest(1))
      let sink = PausedCommitSink(started: signal, resume: resume)
      let ticket = try await server.transfers.offer(to: client.endpoint, byteCount: 0) {
        .source(SignallingSource(closed: sourceClosed))
      }
      let transfer = Task { try await client.transfers.transfer(ticket) { .sink(sink) } }
      let watchdog = Task {
        try await Task.sleep(for: .seconds(5))
        signal.finish()
        gate.finish()
        sourceClosed.finish()
      }
      defer {
        watchdog.cancel()
        transfer.cancel()
        signal.finish()
        gate.finish()
        sourceClosed.finish()
      }
      var commitEvents = started.makeAsyncIterator()
      _ = try #require(await commitEvents.next())
      let stopping = Task {
        if shutdown {
          await client.shutdown()
          #expect(await sink.published)
          #expect(await client.transfers.receipt(for: ticket.id) != nil)
        } else {
          await client.close()
          #expect(await sink.published == false)
        }
      }
      var sourceEvents = closed.makeAsyncIterator()
      _ = try #require(await sourceEvents.next())
      if !shutdown { await stopping.value }
      gate.finish()
      await stopping.value
      #expect(try await transfer.value.byteCount == 0)
    }
  }

  @Test(arguments: [false, true], [false, true])
  func sourceCanStopItsSystemDuringARead(outbound: Bool, shutdown: Bool) async throws {
    let sourceSystem = try await TCPActorSystem(
      host: "127.0.0.1", advertisedHost: "127.0.0.1", port: 0, callTimeout: .seconds(2), connectTimeout: .seconds(2))
    let sinkSystem = try await TCPActorSystem(
      host: "127.0.0.1", advertisedHost: "127.0.0.1", port: 0, callTimeout: .seconds(2), connectTimeout: .seconds(2))
    defer { await sinkSystem.shutdown() }

    let (returned, signal) = AsyncStream<Void>.makeStream(bufferingPolicy: .bufferingNewest(1))
    let source = MemorySource(bytes: ByteBuffer(string: "x")) {
      if shutdown {
        await sourceSystem.shutdown()
      } else {
        await sourceSystem.close()
      }
      signal.yield(())
    }
    let sink = MemorySink()
    let transfer: Task<TransferReceipt, any Error>
    if outbound {
      let ticket = try await sinkSystem.transfers.offer(to: sourceSystem.endpoint, byteCount: 1) { .sink(sink) }
      transfer = Task { try await sourceSystem.transfers.transfer(ticket) { .source(source) } }
    } else {
      let ticket = try await sourceSystem.transfers.offer(to: sinkSystem.endpoint, byteCount: 1) { .source(source) }
      transfer = Task { try await sinkSystem.transfers.transfer(ticket) { .sink(sink) } }
    }
    let watchdog = Task {
      try await Task.sleep(for: .seconds(5))
      signal.finish()
    }
    defer {
      transfer.cancel()
      watchdog.cancel()
      signal.finish()
    }
    var events = returned.makeAsyncIterator()
    _ = try #require(await events.next(), "Stopping the system never returned to the transfer callback")
    await #expect(throws: (any Error).self) { try await transfer.value }
    await sourceSystem.shutdown()
    #expect(await sink.published == nil)
  }
}

private struct SuspendingSource: DataSource {
  let reading: AsyncStream<Void>.Continuation

  func read(at offset: Int64, maximumBytes: Int) async throws -> ByteBuffer {
    if offset == 0 { return ByteBuffer(string: "x") }
    reading.yield(())
    try await Task.sleep(for: .seconds(5))
    throw TransferError("Source read was not cancelled")
  }

  func close() async {}
}

private struct RejectingSink: DataSink {
  let sourceReading: AsyncStream<Void>

  func write(_ bytes: ByteBuffer, at offset: Int64) async throws {
    for await _ in sourceReading { break }
    throw TransferError("Storage write failed")
  }

  func commit() async throws {}
  func abort() async {}
}

private struct MemorySource: DataSource {
  let bytes: ByteBuffer
  let beforeRead: @Sendable () async -> Void

  func read(at offset: Int64, maximumBytes: Int) async throws -> ByteBuffer {
    if offset == 0 { await beforeRead() }
    return bytes.getSlice(at: Int(offset), length: min(maximumBytes, bytes.readableBytes - Int(offset))) ?? ByteBuffer()
  }

  func close() async {}
}

private actor MemorySink: DataSink {
  var bytes = ByteBuffer()
  var published: ByteBuffer?
  var aborted = false

  func write(_ data: ByteBuffer, at offset: Int64) throws {
    guard offset == bytes.readableBytes else { throw TransferError("Unexpected offset") }
    bytes.writeImmutableBuffer(data)
  }

  func commit() { published = bytes }
  func abort() { aborted = true }
}

private struct SignallingSource: DataSource {
  let closed: AsyncStream<Void>.Continuation

  func read(at offset: Int64, maximumBytes: Int) async throws -> ByteBuffer { ByteBuffer() }

  func close() async { closed.yield(()) }
}

private actor PausedCommitSink: DataSink {
  let started: AsyncStream<Void>.Continuation
  let resume: AsyncStream<Void>
  var published = false

  init(started: AsyncStream<Void>.Continuation, resume: AsyncStream<Void>) {
    self.started = started
    self.resume = resume
  }

  func write(_ bytes: ByteBuffer, at offset: Int64) async throws {}

  func commit() async throws {
    started.yield(())
    for await _ in resume { break }
    try Task.checkCancellation()
    published = true
  }

  func abort() async {}
}
