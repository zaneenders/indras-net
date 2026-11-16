import Foundation
import NIOCore
import Networking
import Setu
import Testing

@testable import SetuNetworking

@Suite(.serialized, .timeLimit(.minutes(1)))
struct RequestActivityTests {
  @Test func diskProgressOutlastsTheInactivityInterval() async throws {
    let activity = RequestActivity()
    let tracker = TransferProgressTracker(onUpdate: { activity.record() })
    let started = ContinuousClock.now
    let result = try await withInactivityTimeout(timeout: .seconds(3), activity: activity) {
      for _ in 0..<16 {
        try await Task.sleep(for: .milliseconds(250))
        await tracker.advance(by: 1)
      }
      return 42
    }
    #expect(result == 42)
    #expect(started.duration(to: .now) > .seconds(3))
    await #expect(throws: PeerTransportError.timedOut) {
      try await withInactivityTimeout(timeout: .milliseconds(50), activity: RequestActivity()) {
        try await Task.sleep(for: .seconds(10))
      }
    }
  }

  @Test func heartbeatsExtendARequestAndMayPrecedeMetadataOrPayload() async throws {
    let listener = try await TCP.listen(host: "127.0.0.1", port: 0)
    let port = try #require(listener.channel.localAddress?.port)
    let client = PeerConnection(peer: Peer(host: "127.0.0.1", port: port, connections: 1, chunkSize: 1_048_576))
    do {
      async let reply = client.request(
        .downloadChunk(name: "file", index: 0, chunkSize: 1_048_576, length: 4), body: nil,
        timeout: .seconds(3))
      try await listener.executeThenClose { inbound in
        var connections = inbound.makeAsyncIterator()
        let connection = try #require(try await connections.next())
        try await connection.executeThenClose { input, output in
          var requests = input.makeAsyncIterator()
          #expect(try await requests.next() != nil)
          let heartbeat = Frame(target: SetuWire.service, opcode: SetuWire.heartbeat, payload: ByteBuffer())
          try await output.write(heartbeat)
          try await output.write(
            SetuWire.encode(
              TransferResponse.chunk(encoding: .identity, length: 4), target: SetuWire.service,
              opcode: SetuWire.response))
          for _ in 0..<16 {
            try await Task.sleep(for: .milliseconds(250))
            try await output.write(heartbeat)
          }
          try await output.write(SetuWire.bytes(ByteBuffer(bytes: [1, 2, 3, 4])))
          output.finish()
          #expect(try await requests.next() == nil)
        }
      }
      let (_, bytes) = try await reply
      #expect(bytes == ByteBuffer(bytes: [1, 2, 3, 4]))
      await client.close()
    } catch {
      await client.close()
      try? await listener.channel.close()
      throw error
    }
  }

  @Test func serverHeartbeatsDoNotConcealAnIdlePeer() async throws {
    let root = FileManager.default.temporaryDirectory.appendingPathComponent("setu-idle-\(UUID())")
    defer { try? FileManager.default.removeItem(at: root) }
    let temporary = try await TemporaryFiles(directory: root.appendingPathComponent("temporary").path, protecting: [])
    let store = try await UploadStore(
      root: root.appendingPathComponent("shared").path, quota: 1, temporary: temporary,
      maxActiveUploads: 1, idleTimeout: .seconds(900))
    let service = TransferService(store: store, catalog: FileCatalog(root: store.root, node: UUID()), onRestart: nil)
    let listener = try await TCP.listen(host: "127.0.0.1", port: 0)
    let port = try #require(listener.channel.localAddress?.port)
    let serving = Task {
      try await listener.executeThenClose { inbound in
        var connections = inbound.makeAsyncIterator()
        let connection = try #require(try await connections.next())
        try await service.handle(connection, idleTimeout: .seconds(3))
      }
    }
    do {
      let connection = try await TCP.connect(host: "127.0.0.1", port: port, connectTimeout: .seconds(1))
      try await connection.executeThenClose { input, _ in
        var heartbeats = 0
        for try await frame in input {
          #expect(SetuWire.isHeartbeat(frame))
          heartbeats += 1
        }
        #expect(heartbeats > 0)
      }
      await #expect(throws: PeerTransportError.timedOut) { try await serving.value }
      await store.close()
      await temporary.close()
    } catch {
      serving.cancel()
      try? await listener.channel.close()
      _ = try? await serving.value
      await store.close()
      await temporary.close()
      throw error
    }
  }

  @Test func fragmentedChunkPayloadExtendsTheInactivityTimeout() async throws {
    let listener = try await TCP.listen(host: "127.0.0.1", port: 0)
    let port = try #require(listener.channel.localAddress?.port)
    let client = PeerConnection(peer: Peer(host: "127.0.0.1", port: port, connections: 1, chunkSize: 1_048_576))
    let payload = ByteBuffer(repeating: 7, count: 2048)
    do {
      async let reply = client.request(
        .downloadChunk(name: "file", index: 0, chunkSize: 1_048_576, length: payload.readableBytes), body: nil,
        timeout: .seconds(3))
      try await listener.executeThenClose { inbound in
        var connections = inbound.makeAsyncIterator()
        let connection = try #require(try await connections.next())
        let beforeEncoder = try await connection.channel.pipeline.context(
          handlerType: MessageToByteHandler<FrameEncoder>.self
        ).map { NIOLoopBound($0, eventLoop: connection.channel.eventLoop) }.get()
        try await connection.executeThenClose { input, output in
          var requests = input.makeAsyncIterator()
          #expect(try await requests.next() != nil)
          var bytes = ByteBuffer()
          let encoder = FrameEncoder()
          try encoder.encode(
            data: SetuWire.encode(
              TransferResponse.chunk(encoding: .identity, length: payload.readableBytes), target: SetuWire.service,
              opcode: SetuWire.response), out: &bytes)
          try encoder.encode(data: SetuWire.bytes(payload), out: &bytes)
          while let part = bytes.readSlice(length: min(bytes.readableBytes, 128)), part.readableBytes > 0 {
            try await connection.channel.eventLoop.makeSucceededVoidFuture().flatMap {
              beforeEncoder.value.writeAndFlush(NIOAny(part))
            }.get()
            try await Task.sleep(for: .milliseconds(250))
          }
          output.finish()
          #expect(try await requests.next() == nil)
        }
      }
      let (_, bytes) = try await reply
      #expect(bytes == payload)
      await client.close()
    } catch {
      await client.close()
      try? await listener.channel.close()
      throw error
    }
  }
}
