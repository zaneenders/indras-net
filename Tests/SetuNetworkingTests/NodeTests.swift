import Foundation
import NIOCore
import Networking
import Setu
import Testing

@testable import SetuNetworking

@Suite(.timeLimit(.minutes(1)))
struct NodeTests {
  @Test func localControlSubmitsAndMonitorsVerifiedTransferAndCleansUpOnCancellation() async throws {
    let root = URL(fileURLWithPath: "/tmp/indras-node-\(UUID())")
    defer { try? FileManager.default.removeItem(at: root) }
    let socket = root.appendingPathComponent("run/control.sock").path
    let shared = root.appendingPathComponent("shared")
    let temporary = root.appendingPathComponent("temporary")
    let node = Task {
      try await runNode(
        host: "127.0.0.1", port: 0, directory: shared.path, socket: socket,
        temporaryDirectory: temporary.path, maxActiveUploads: 128, uploadIdleTimeout: .seconds(900),
        downloadRecoveryBytes: defaultDownloadRecoveryBytes, onRestart: nil)
    }
    let control = try NodeControlClient(socket: socket)
    do {
      let readyDeadline = ContinuousClock.now + .seconds(5)
      while !FileManager.default.fileExists(atPath: socket) {
        guard ContinuousClock.now < readyDeadline else { throw NodeTestFailure.readinessTimeout }
        try await Task.sleep(for: .milliseconds(10))
      }
      guard case .status(_, let port, _) = try await control.request(.status) else {
        throw NodeTestFailure.unexpectedReply
      }
      let source = root.appendingPathComponent("file.bin")
      let bytes = Data(repeating: 0x9A, count: 1_048_577)
      try bytes.write(to: source)
      let peer = PeerTarget(host: "127.0.0.1", port: port, connections: 4, chunkSize: 1_048_576)
      guard case .job(let submitted) = try await control.request(.send(peer: peer, file: source.path)) else {
        throw NodeTestFailure.unexpectedReply
      }
      let completionDeadline = ContinuousClock.now + .seconds(10)
      var job = submitted
      while job.state == .running {
        guard ContinuousClock.now < completionDeadline else { throw NodeTestFailure.completionTimeout }
        try await Task.sleep(for: .milliseconds(10))
        guard case .job(let next) = try await control.request(.job(job.id)) else {
          throw NodeTestFailure.unexpectedReply
        }
        job = next
      }
      #expect(job.state == .completed)
      #expect(job.result?.receipt?.size == Int64(bytes.count))
      #expect(try Data(contentsOf: shared.appendingPathComponent("file.bin")) == bytes)
      guard case .files(let files) = try await control.request(.list(nil)) else {
        throw NodeTestFailure.unexpectedReply
      }
      #expect(files.map(\.name) == ["file.bin"])
      await control.close()
      node.cancel()
      _ = try? await node.value
      #expect(!FileManager.default.fileExists(atPath: socket))
      #expect(try FileManager.default.contentsOfDirectory(atPath: temporary.path) == [".lock"])
    } catch {
      await control.close()
      node.cancel()
      _ = try? await node.value
      throw error
    }
  }

  @Test func deadlineAndCancellationCloseSocketsWithoutReplayingRestart() async throws {
    let listener = try await TCP.listen(host: "127.0.0.1", port: 0)
    let port = try #require(listener.channel.localAddress?.port)
    let connection = PeerConnection(peer: Peer(host: "127.0.0.1", port: port, connections: 1, chunkSize: 1_048_576))
    do {
      try await listener.executeThenClose { inbound in
        let call = Task { try await connection.request(.restart, body: nil, timeout: .milliseconds(50)) }
        var accepted = inbound.makeAsyncIterator()
        let socket = try #require(try await accepted.next())
        try await socket.executeThenClose { input, _ in
          var frames = input.makeAsyncIterator()
          let request = try #require(try await frames.next())
          let decoded = try SetuWire.decode(
            request, target: SetuWire.service, opcode: SetuWire.request, as: TransferRequest.self)
          guard case .restart = decoded else { throw NodeTestFailure.unexpectedReply }
          let eof = try await frames.next()
          #expect(eof == nil)
        }
        await #expect(throws: PeerTransportError.timedOut) { try await call.value }

        let cancelled = Task { try await connection.request(.restart, body: nil, timeout: .seconds(10)) }
        let next = try #require(try await accepted.next())
        try await next.executeThenClose { input, _ in
          var frames = input.makeAsyncIterator()
          let request = try await frames.next()
          #expect(request != nil)
          cancelled.cancel()
          let eof = try await frames.next()
          #expect(eof == nil)
        }
        await #expect(throws: CancellationError.self) { try await cancelled.value }
      }
      await connection.close()
    } catch {
      await connection.close()
      try? await listener.channel.close()
      throw error
    }
  }
}

private enum NodeTestFailure: Error {
  case readinessTimeout
  case completionTimeout
  case unexpectedReply
}
