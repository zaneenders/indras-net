import Foundation
import NIOCore
import Networking
import Setu
import Synchronization
import Testing

@testable import SetuNetworking

@Suite(.timeLimit(.minutes(2)))
struct TransferTests {
  private static let owner = UUID()

  @Test func concurrentIdenticalUploadsBothComplete() async throws {
    try await withReceiver { fixture in
      let source = fixture.root.appendingPathComponent("duplicate.bin")
      let bytes = Data(repeating: 0xA7, count: 1_048_589)
      try bytes.write(to: source)
      let first = try await fixture.client(port: fixture.server.port)
      let second = try await fixture.client(port: fixture.server.port)
      do {
        try await withThrowingTaskGroup(of: TransferResult.self) { tasks in
          for client in [first, second] {
            tasks.addTask {
              try await client.upload(file: source.path, destination: nil, createDestination: false, progress: nil)
            }
          }
          for try await result in tasks { #expect(result.receipt?.size == Int64(bytes.count)) }
        }
        #expect(try Data(contentsOf: fixture.received.appendingPathComponent("duplicate.bin")) == bytes)
        #expect(try FileManager.default.contentsOfDirectory(atPath: fixture.received.path) == ["duplicate.bin"])
        await first.close()
        await second.close()
      } catch {
        await first.close()
        await second.close()
        throw error
      }
    }
  }

  @Test func incompressibleChunksDrainBeforeTheServerCloses() async throws {
    try await withReceiver { fixture in
      var buffer = ByteBuffer()
      var state: UInt64 = 0xB14F_C392_8D57_E6A1
      let size = 8_388_621
      for _ in 0..<((size + 7) / 8) {
        state ^= state << 13
        state ^= state >> 7
        state ^= state << 17
        buffer.writeInteger(state)
      }
      let bytes = Data(buffer.readableBytesView.prefix(size))
      let encoded = try await encodeChunk(ByteBuffer(bytes: bytes))
      #expect(encoded.readableBytes >= bytes.count)
      let source = fixture.root.appendingPathComponent("incompressible.bin")
      try bytes.write(to: source)
      let client = try await TransferClient.connect(
        host: "127.0.0.1", port: fixture.server.port, connections: 4, chunkSize: 8_388_608,
        temporaryDirectory: fixture.root.appendingPathComponent("bulk-client").path,
        recoveryBudgetBytes: defaultDownloadRecoveryBytes)
      do {
        let sent = try await client.upload(file: source.path, destination: nil, createDestination: false, progress: nil)
        #expect(sent.bytesTransferred == Int64(size))
        let entry = try #require(try await client.list().first)
        let manifest = try await client.request(fileID: entry.id)
        let output = fixture.root.appendingPathComponent("incompressible-download.bin")
        _ = try await client.download(manifest, to: output.path, progress: nil)
        #expect(try Data(contentsOf: output) == bytes)
        await client.close()
      } catch {
        await client.close()
        throw error
      }
    }
  }

  @Test(arguments: [0, 1_048_577, 8_388_621])
  func binaryRoundTripAndDuplicatePublication(size: Int) async throws {
    try await withReceiver { fixture in
      let bytes = Data((0..<size).map { UInt8(truncatingIfNeeded: $0) })
      let source = fixture.root.appendingPathComponent("file 雨.bin")
      try bytes.write(to: source)
      let client = try await fixture.client(port: fixture.server.port)
      do {
        let result = try await client.upload(
          file: source.path, destination: nil, createDestination: false, progress: nil)
        #expect(result.receipt?.size == Int64(size))
        #expect(result.bytesTransferred == Int64(size))
        let duplicate = try await client.upload(
          file: source.path, destination: nil, createDestination: false, progress: nil)
        #expect(duplicate.bytesTransferred == 0)
        let entries = try await client.list()
        let entry = try #require(entries.first)
        #expect(entries.count == 1)
        let manifest = try await client.request(fileID: entry.id)
        let output = fixture.root.appendingPathComponent("download.bin")
        let fetched = try await client.download(manifest, to: output.path, progress: nil)
        #expect(fetched.receipt?.sha256 == result.receipt?.sha256)
        #expect(try Data(contentsOf: output) == bytes)
        await client.close()
      } catch {
        await client.close()
        throw error
      }
    }
  }

  @Test func destinationCreationAndConflictingFilePreservation() async throws {
    try await withReceiver { fixture in
      let source = fixture.root.appendingPathComponent("result.bin")
      try Data("original".utf8).write(to: source)
      let destination = fixture.root.appendingPathComponent("custom/nested")
      let client = try await fixture.client(port: fixture.server.port)
      do {
        await #expect(throws: (any Error).self) {
          try await client.upload(
            file: source.path, destination: destination.path, createDestination: false, progress: nil)
        }
        let result = try await client.upload(
          file: source.path, destination: destination.path, createDestination: true, progress: nil)
        #expect(result.receipt?.destination == destination.appendingPathComponent("result.bin").path)
        try Data("replacement".utf8).write(to: source)
        await #expect(throws: (any Error).self) {
          try await client.upload(
            file: source.path, destination: destination.path, createDestination: false, progress: nil)
        }
        #expect(try Data(contentsOf: destination.appendingPathComponent("result.bin")) == Data("original".utf8))
        await client.close()
      } catch {
        await client.close()
        throw error
      }
    }
  }

  @Test func corruptChunkReleasesReservationAndCannotPublish() async throws {
    try await withReceiver { fixture in
      let source = fixture.root.appendingPathComponent("source.bin")
      let bytes = Data("verified bytes".utf8)
      try bytes.write(to: source)
      let manifest = try await Manifest.scan(
        path: source.path, name: "result.bin", chunkSize: 1_048_576,
        followSymbolicLinks: true, progress: nil)
      let connection = PeerConnection(
        peer: Peer(host: "127.0.0.1", port: fixture.server.port, connections: 1, chunkSize: 1_048_576))
      do {
        let (reply, _) = try await connection.request(
          .begin(manifest: manifest, destination: nil, createDestination: false, owner: Self.owner), body: nil,
          timeout: .seconds(5))
        guard case .status(let status) = reply else { throw TestFailure.unexpectedReply }
        await #expect(throws: (any Error).self) {
          try await connection.request(
            .uploadChunk(id: status.id, index: 0, encoding: .identity),
            body: ByteBuffer(repeating: 0, count: bytes.count), timeout: .seconds(5))
        }
        await #expect(throws: (any Error).self) {
          try await connection.request(.commit(id: status.id), body: nil, timeout: .seconds(5))
        }
        #expect(!FileManager.default.fileExists(atPath: fixture.received.appendingPathComponent("result.bin").path))
        _ = try await connection.request(
          .uploadChunk(id: status.id, index: 0, encoding: .identity),
          body: ByteBuffer(bytes: bytes), timeout: .seconds(5))
        _ = try await connection.request(.commit(id: status.id), body: nil, timeout: .seconds(5))
        #expect(try Data(contentsOf: fixture.received.appendingPathComponent("result.bin")) == bytes)
        await connection.close()
      } catch {
        await connection.close()
        throw error
      }
    }
  }

  @Test func lostPublicationReplyReconcilesWithoutResendingChunks() async throws {
    try await withReceiver { fixture in
      let proxy = try await PublicationReplyProxy.start(peerPort: fixture.server.port)
      let source = fixture.root.appendingPathComponent("result.bin")
      let bytes = Data(repeating: 0xA7, count: 1_048_577)
      try bytes.write(to: source)
      let client = try await fixture.client(port: proxy.port)
      do {
        let result = try await client.upload(
          file: source.path, destination: nil, createDestination: false, progress: nil)
        #expect(result.receipt?.size == Int64(bytes.count))
        #expect(proxy.counts.mutex.withLock { $0.drops } == 1)
        #expect(proxy.counts.mutex.withLock { $0.uploads } == 2)
        #expect(try Data(contentsOf: fixture.received.appendingPathComponent("result.bin")) == bytes)
        await client.close()
        await proxy.close()
      } catch {
        await client.close()
        await proxy.close()
        throw error
      }
    }
  }

  @Test func receiverRestartDropsPartialProgressAndPermitsFreshTransfer() async throws {
    try await withReceiver { fixture in
      let source = fixture.root.appendingPathComponent("result.bin")
      let bytes = Data(repeating: 0x42, count: 1_048_577)
      try bytes.write(to: source)
      let manifest = try await Manifest.scan(
        path: source.path, name: nil, chunkSize: 1_048_576,
        followSymbolicLinks: true, progress: nil)
      let connection = PeerConnection(
        peer: Peer(host: "127.0.0.1", port: fixture.server.port, connections: 1, chunkSize: 1_048_576))
      let (reply, _) = try await connection.request(
        .begin(manifest: manifest, destination: nil, createDestination: false, owner: Self.owner), body: nil,
        timeout: .seconds(5))
      guard case .status(let status) = reply else { throw TestFailure.unexpectedReply }
      _ = try await connection.request(
        .uploadChunk(id: status.id, index: 0, encoding: .identity),
        body: ByteBuffer(bytes: bytes.prefix(1_048_576)), timeout: .seconds(5))
      await connection.close()
      await fixture.server.shutdown()
      #expect(try FileManager.default.contentsOfDirectory(atPath: fixture.received.path).isEmpty)
      let replacement = try await TransferServer.start(
        host: "127.0.0.1", port: fixture.server.port,
        directory: fixture.received.path, temporaryDirectory: fixture.root.appendingPathComponent("receiver-temp").path,
        maxActiveUploads: 128, uploadIdleTimeout: .seconds(900), onRestart: nil)
      let client = try await fixture.client(port: replacement.port)
      do {
        let result = try await client.upload(
          file: source.path, destination: nil, createDestination: false, progress: nil)
        #expect(result.resumedChunks == 0)
        #expect(try Data(contentsOf: fixture.received.appendingPathComponent("result.bin")) == bytes)
        await client.close()
        await replacement.shutdown()
      } catch {
        await client.close()
        await replacement.shutdown()
        throw error
      }
    }
  }

  @Test(arguments: [UInt32(1), 999])
  func unsupportedVersionFailsBeforeMutation(version: UInt32) async throws {
    try await withReceiver { fixture in
      let connection = try await TCP.connect(host: "127.0.0.1", port: fixture.server.port, connectTimeout: .seconds(5))
      try await connection.executeThenClose { inbound, outbound in
        let request = Frame(
          target: SetuWire.service, opcode: SetuWire.request,
          payload: ByteBuffer(string: "{\"version\":\(version),\"value\":{\"restart\":{}}}"))
        try await outbound.write(request)
        var iterator = inbound.makeAsyncIterator()
        let frame = try #require(try await iterator.next())
        let response = try SetuWire.decode(
          frame, target: SetuWire.service, opcode: SetuWire.response, as: TransferResponse.self)
        guard case .failure(let message) = response else { throw TestFailure.unexpectedReply }
        #expect(message.contains("Unsupported Setu protocol version"))
      }
      #expect(try FileManager.default.contentsOfDirectory(atPath: fixture.received.path).isEmpty)
    }
  }

  @Test func shutdownClosesAnIncompleteRequest() async throws {
    try await withReceiver { fixture in
      let connection = try await TCP.connect(host: "127.0.0.1", port: fixture.server.port, connectTimeout: .seconds(5))
      try await connection.executeThenClose { inbound, _ in
        await fixture.server.shutdown()
        var iterator = inbound.makeAsyncIterator()
        let frame = try await iterator.next()
        #expect(frame == nil)
      }
      await fixture.server.shutdown()
    }
  }
}

private enum TestFailure: Error { case unexpectedReply }

private struct ReceiverFixture: Sendable {
  let root: URL
  let received: URL
  let server: TransferServer

  func client(port: Int) async throws -> TransferClient {
    try await TransferClient.connect(
      host: "127.0.0.1", port: port, connections: 4, chunkSize: 1_048_576,
      temporaryDirectory: root.appendingPathComponent("client-\(UUID())").path,
      recoveryBudgetBytes: defaultDownloadRecoveryBytes)
  }
}

private func withReceiver(_ body: @Sendable (ReceiverFixture) async throws -> Void) async throws {
  let root = FileManager.default.temporaryDirectory.appendingPathComponent("indras-transfer-\(UUID())")
  defer { try? FileManager.default.removeItem(at: root) }
  let received = root.appendingPathComponent("received")
  let server = try await TransferServer.start(
    host: "127.0.0.1", port: 0, directory: received.path,
    temporaryDirectory: root.appendingPathComponent("receiver-temp").path,
    maxActiveUploads: 128, uploadIdleTimeout: .seconds(900), onRestart: nil)
  do {
    try await body(ReceiverFixture(root: root, received: received, server: server))
    await server.shutdown()
  } catch {
    await server.shutdown()
    throw error
  }
}

private final class PublicationReplyProxy: Sendable {
  struct Counts: Sendable {
    var drops = 0
    var uploads = 0
  }
  let listener: Listener
  let task: Task<Void, Never>
  let connections: ServerConnections
  final class CountState: Sendable {
    let mutex = Mutex(Counts())
  }
  let counts: CountState
  var port: Int { listener.channel.localAddress!.port! }

  init(listener: Listener, task: Task<Void, Never>, connections: ServerConnections, counts: CountState) {
    self.listener = listener
    self.task = task
    self.connections = connections
    self.counts = counts
  }

  static func start(peerPort: Int) async throws -> PublicationReplyProxy {
    let listener = try await TCP.listen(host: "127.0.0.1", port: 0)
    let connections = ServerConnections()
    let counts = CountState()
    let task = Task<Void, Never> {
      try? await listener.executeThenClose { inbound in
        await withDiscardingTaskGroup { tasks in
          do {
            for try await downstream in inbound {
              connections.add(downstream.channel)
              tasks.addTask {
                try? await downstream.executeThenClose { input, output in
                  var requests = input.makeAsyncIterator()
                  guard let first = try await requests.next() else { return }
                  let request = try SetuWire.decode(
                    first, target: SetuWire.service, opcode: SetuWire.request, as: TransferRequest.self)
                  let upstream = try await TCP.connect(host: "127.0.0.1", port: peerPort, connectTimeout: .seconds(5))
                  connections.add(upstream.channel)
                  let body: Frame?
                  if case .uploadChunk = request {
                    counts.mutex.withLock { $0.uploads += 1 }
                    guard let frame = try await requests.next() else { throw TestFailure.unexpectedReply }
                    body = frame
                  } else {
                    body = nil
                  }
                  try await upstream.executeThenClose { replies, send in
                    try await send.write(first)
                    if let body { try await send.write(body) }
                    var responses = replies.makeAsyncIterator()
                    let response = try #require(try await responses.next())
                    if case .commit = request {
                      let drop = counts.mutex.withLock { state in
                        guard state.drops == 0 else { return false }
                        state.drops += 1
                        return true
                      }
                      if drop { return }
                    }
                    try await output.write(response)
                  }
                }
              }
            }
          } catch {}
          tasks.cancelAll()
        }
      }
    }
    return PublicationReplyProxy(listener: listener, task: task, connections: connections, counts: counts)
  }

  func close() async {
    try? await listener.channel.close()
    await connections.close()
    await task.value
  }
}
