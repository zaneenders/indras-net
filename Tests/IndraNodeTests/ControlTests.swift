import Foundation
import NIOCore
import Networking
import TCPActorSystem
import Testing

@testable import IndraNode

@Suite(.timeLimit(.minutes(1)))
struct ControlTests {
  @Test func requestUsesTheCallersDeadline() async throws {
    let root = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
    try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true, attributes: nil)
    defer { try? FileManager.default.removeItem(at: root) }
    let socket = root.appendingPathComponent("control.sock").path
    let listener = try await TCP.listen(unixDomainSocketPath: socket)
    try await listener.executeThenClose { connections in
      let reply = Task { try await NodeControl.request(.monitor, socket: socket, timeout: .milliseconds(250)) }
      defer { reply.cancel() }
      var connections = connections.makeAsyncIterator()
      let connection = try #require(try await connections.next())
      try await connection.executeThenClose { input, _ in
        var frames = input.makeAsyncIterator()
        #expect(try await frames.next()?.opcode == 1)
        do {
          _ = try await reply.value
          Issue.record("Expected a control deadline error")
        } catch let error as IndraError {
          #expect(error.description == "Control request timed out")
        }
      }
    }
  }

  @Test(arguments: [false, true])
  func largeMonitorReplyHandlesBackpressureAndCancellation(cancel: Bool) async throws {
    try await withControlRuntime { runtime in
      let peer = NodeIdentity(
        id: UUID(), name: "peer",
        endpoint: ActorEndpoint(host: "127.0.0.1", port: 7332, systemID: UUID()), build: nil)
      for number in 0..<128 {
        _ = try await runtime.receive(
          MembershipRequest(
            version: 1, cluster: "test", node: peer,
            probe: .ping(sequenceNumber: UInt32(number), payload: [])))
      }
      let root = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
      try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true, attributes: nil)
      defer { try? FileManager.default.removeItem(at: root) }
      let socket = root.appendingPathComponent("control.sock").path
      let listener = try await TCP.listen(unixDomainSocketPath: socket)
      let accepting = Task {
        try await listener.executeThenClose { connections in
          var incoming = connections.makeAsyncIterator()
          return try #require(try await incoming.next())
        }
      }
      do {
        let client = try await TCP.connect(unixDomainSocketPath: socket, connectTimeout: .seconds(2))
        let server = try await accepting.value
        try await client.channel.setOption(ChannelOptions.autoRead, value: false).get()
        try await server.channel.setOption(ChannelOptions.socketOption(.so_sndbuf), value: 1024).get()
        try await server.channel.setOption(
          ChannelOptions.writeBufferWaterMark,
          value: .init(low: 1024, high: 2048)
        ).get()
        let serving = Task { try await serveControlConnection(server, runtime: runtime) }
        do {
          try await client.executeThenClose { input, output in
            let request = Frame(
              target: 0x494E_4452, opcode: 1,
              payload: ByteBuffer(bytes: try JSONEncoder().encode(ControlRequest.monitor)))
            try await output.write(request)
            output.finish()
            let deadline = ContinuousClock.now.advanced(by: .seconds(3))
            while server.channel.isWritable, server.channel.isActive {
              guard ContinuousClock.now < deadline else { throw IndraError("Reply did not encounter backpressure") }
              await Task.yield()
            }
            try await Task.sleep(for: .milliseconds(30))
            if cancel {
              serving.cancel()
              _ = try? await serving.value
              #expect(!server.channel.isActive)
              return
            }
            try await client.channel.setOption(ChannelOptions.autoRead, value: true).get()
            var frames = input.makeAsyncIterator()
            let frame = try #require(try await frames.next())
            #expect(frame.payload.readableBytes > 2048)
            let reply = try JSONDecoder().decode(ControlReply.self, from: Data(frame.payload.readableBytesView))
            guard case .monitor(let snapshot) = reply else { throw IndraError("Expected a monitor snapshot") }
            #expect(snapshot.events.count == 128)
            #expect(snapshot.inbound.ping == 128)
            #expect(snapshot.outbound.ack == 128)
            #expect(try await frames.next() == nil)
          }
          if cancel { _ = try? await serving.value } else { try await serving.value }
        } catch {
          serving.cancel()
          try? await client.channel.close()
          try? await server.channel.close()
          _ = try? await serving.value
          throw error
        }
      } catch {
        accepting.cancel()
        try? await listener.channel.close()
        _ = try? await accepting.value
        throw error
      }
    }
  }
}

private func withControlRuntime(_ body: @Sendable (NodeRuntime) async throws -> Void) async throws {
  let configuration = makeConfiguration(name: "control-test", seeds: [])
  let system = try await TCPActorSystem(
    host: "127.0.0.1", advertisedHost: "127.0.0.1", port: 0,
    callTimeout: .seconds(2), connectTimeout: .seconds(2))
  let runtime = NodeRuntime(
    configuration: configuration, configurationPath: "/unused", peerHistoryPath: nil, system: system)
  do {
    try await runtime.start(services: [])
    try await body(runtime)
    await runtime.shutdown()
  } catch {
    await runtime.shutdown()
    throw error
  }
}
