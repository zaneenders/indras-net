import Foundation
import NIOCore
import Networking
import Testing

@testable import IndraNode
@testable import TCPActorSystem

@Suite(.timeLimit(.minutes(1)))
struct MembershipCachingTests {
  @Test func actorSystemRejectsASecondSetOfBootstrapActors() async throws {
    let configuration = cacheConfiguration(name: "client", port: 0)
    try await withTestRuntime(configuration: configuration) { runtime in
      let system = await runtime.system
      let duplicate = NodeRuntime(
        configuration: configuration, configurationPath: "/unused", peerHistoryPath: nil, system: system)
      await #expect(throws: RPCError.invalidConfiguration) { try await duplicate.start() }
      #expect(await runtime.receptionist.services().count == 2)
    }
  }

  @Test(arguments: [false, true])
  func pingsReuseTheHandshakeAndAddressTheReservedActor(viaAlias: Bool) async throws {
    let listener = try await TCP.listen(host: "127.0.0.1", port: 0)
    let endpoint = ActorEndpoint(
      host: "127.0.0.1", port: try #require(listener.channel.localAddress?.port), node: UUID())
    let peer = NodeIdentity(id: UUID(), name: "peer", endpoint: endpoint, build: nil)
    let server = Task {
      try await serveMembership(
        listener: listener, peer: peer, exchanges: 4, beforeReply: { _ in })
    }
    do {
      try await withTestRuntime(configuration: cacheConfiguration(name: "client", port: 0)) { runtime in
        let address = PeerAddress(host: viaAlias ? "localhost" : endpoint.host, port: endpoint.port)
        let first = try await runtime.ping(address.description)
        #expect(first.node == peer)
        #expect(try await runtime.ping(address.description).node == peer)
        #expect(try await runtime.ping(peer.name).node == peer)
        #expect(try await runtime.ping(peer.id.uuidString).node == peer)
        try await server.value
        #expect(await runtime.status().members.first?.state == .alive)
      }
    } catch {
      server.cancel()
      try? await listener.channel.close()
      _ = try? await server.value
      throw error
    }
  }

  @Test func firstPingAfterPeerRestartLearnsTheNewEndpoint() async throws {
    try await withTestRuntime(configuration: cacheConfiguration(name: "client", port: 0)) { client in
      let configuration = cacheConfiguration(name: "peer", port: 0)
      try await withTestRuntime(configuration: configuration) { original in
        let identity = await original.status().node
        #expect(try await client.ping("127.0.0.1:\(identity.endpoint.port)").node == identity)
        await original.shutdown()
        var updated = configuration
        updated.port = identity.endpoint.port
        try await withTestRuntime(configuration: updated) { replacement in
          let current = await replacement.status().node
          #expect(current.id == identity.id)
          #expect(current.endpoint != identity.endpoint)
          let byName = try await client.ping(identity.name)
          let byID = try await client.ping(identity.id.uuidString)
          #expect(byName.node == current)
          #expect(byID.node == current)
          let members = await client.status().members
          #expect(members.count == 1)
          #expect(members.first?.node == current)
          #expect(members.first?.state == .alive)
        }
      }
    }
  }

  @Test(arguments: [false, true])
  func staleGossipCannotReplaceVerifiedProcess(knowPrevious: Bool) async throws {
    try await withTestRuntime(configuration: cacheConfiguration(name: "client", port: 0)) { client in
      try await withTestRuntime(configuration: cacheConfiguration(name: "peer", port: 0)) { peer in
        let current = await peer.status().node
        let previous = NodeIdentity(
          id: current.id, name: current.name,
          endpoint: ActorEndpoint(host: current.endpoint.host, port: current.endpoint.port, node: UUID()), build: nil)
        let reporter = NodeIdentity(
          id: UUID(), name: "reporter", endpoint: ActorEndpoint(host: "127.0.0.1", port: 7332, node: UUID()), build: nil
        )
        if knowPrevious {
          _ = try await client.receive(
            MembershipRequest(
              version: 1, cluster: "cache-tests", node: reporter,
              probe: .ping(
                sequenceNumber: 1,
                payload: [GossipMember(node: previous, status: .alive(0), protocolPeriod: 0)])))
        }
        #expect(try await client.ping(current.address.description).node == current)
        for status in [GossipStatus.alive(1000), .unreachable(2000), .dead] {
          _ = try await client.receive(
            MembershipRequest(
              version: 1, cluster: "cache-tests", node: reporter,
              probe: .ping(
                sequenceNumber: 2,
                payload: [GossipMember(node: previous, status: status, protocolPeriod: 1000)])))
          let members = await client.status().members.filter { $0.node.id == current.id }
          #expect(members.count == 1)
          #expect(members.first?.node == current)
          #expect(members.first?.state == .alive)
        }
        #expect(try await client.ping(current.name).node == current)
        #expect(try await client.ping(current.id.uuidString).node == current)
        _ = try await client.receive(
          MembershipRequest(
            version: 1, cluster: "cache-tests", node: reporter,
            probe: .ping(
              sequenceNumber: 3,
              payload: [GossipMember(node: current, status: .unreachable(0), protocolPeriod: 1001)])))
        #expect(await client.status().members.first?.state == .unreachable)
        #expect(try await client.ping(current.name).node == current)
        #expect(await client.status().members.first?.state == .alive)
      }
    }
  }

  @Test(arguments: [false, true])
  func delayedReplyCannotRestorePreviousProcess(knowPrevious: Bool) async throws {
    let originalListener = try await TCP.listen(host: "127.0.0.1", port: 0)
    let replacementListener = try await TCP.listen(host: "127.0.0.1", port: 0)
    let original = NodeIdentity(
      id: UUID(), name: "peer",
      endpoint: ActorEndpoint(
        host: "127.0.0.1", port: try #require(originalListener.channel.localAddress?.port), node: UUID()), build: nil)
    let replacement = NodeIdentity(
      id: original.id, name: original.name,
      endpoint: ActorEndpoint(host: original.endpoint.host, port: original.endpoint.port, node: UUID()), build: nil)
    let replacementAddress = "127.0.0.1:\(try #require(replacementListener.channel.localAddress?.port))"
    let (started, start) = AsyncStream<Void>.makeStream(bufferingPolicy: .bufferingNewest(1))
    let (released, release) = AsyncStream<Void>.makeStream(bufferingPolicy: .bufferingNewest(1))
    defer {
      start.finish()
      release.finish()
    }
    let originalServer = Task {
      try await serveMembership(
        listener: originalListener, peer: original, exchanges: knowPrevious ? 2 : 1,
        beforeReply: { completed in
          if completed == (knowPrevious ? 1 : 0) {
            start.yield(())
            for await _ in released { break }
            try Task.checkCancellation()
          }
        })
    }
    let replacementServer = Task {
      try await serveMembership(
        listener: replacementListener, peer: replacement, exchanges: 2, beforeReply: { _ in })
    }
    do {
      try await withTestRuntime(configuration: cacheConfiguration(name: "client", port: 0)) { client in
        if knowPrevious { _ = try await client.ping(original.address.description) }
        let delayed = Task { try await client.ping(original.address.description) }
        defer { delayed.cancel() }
        var starts = started.makeAsyncIterator()
        _ = try #require(await starts.next())
        #expect(try await client.ping(replacementAddress).node == replacement)
        release.yield(())
        await #expect(throws: IndraError.self) { try await delayed.value }
        #expect(await client.status().discoveryErrors.isEmpty)
        #expect(try await client.ping(replacementAddress).node == replacement)
        let members = await client.status().members
        #expect(members.count == 1)
        #expect(members.first?.node == replacement)
        #expect(members.first?.state == .alive)
      }
      try await originalServer.value
      try await replacementServer.value
    } catch {
      release.finish()
      originalServer.cancel()
      replacementServer.cancel()
      try? await originalListener.channel.close()
      try? await replacementListener.channel.close()
      _ = try? await originalServer.value
      _ = try? await replacementServer.value
      throw error
    }
  }
}

private func serveMembership(
  listener: Listener, peer: NodeIdentity, exchanges: Int,
  beforeReply: @Sendable (Int) async throws -> Void
) async throws {
  try await listener.executeThenClose { connections in
    var connections = connections.makeAsyncIterator()
    let connection = try #require(try await connections.next())
    try await connection.executeThenClose { input, output in
      try await output.write(WireMessage.hello(version: WireMessage.version, endpoint: peer.endpoint).frame())
      let address = ActorAddress(endpoint: peer.endpoint, id: MembershipService.fixedID)
      var completed = 0
      for try await frame in input {
        switch try WireMessage(frame: frame) {
        case .hello: break
        case .call(let invocation):
          try #require(invocation.actorID == address)
          #expect(invocation.actorType == String(reflecting: MembershipService.self))
          let request = try JSONDecoder().decode(MembershipRequest.self, from: #require(invocation.arguments.first))
          try await beforeReply(completed)
          let response = MembershipReply(
            version: 1, cluster: "cache-tests", node: peer,
            response: .ack(
              target: peer, incarnation: 0,
              payload: [GossipMember(node: peer, status: .alive(0), protocolPeriod: 0)],
              sequenceNumber: request.probe.sequenceNumber))
          try await output.write(
            WireMessage.reply(invocation.requestID, .value(try JSONEncoder().encode(response))).frame())
          completed += 1
          if completed == exchanges { return }
        case .reply:
          throw IndraError("Unexpected reply from membership client")
        }
      }
      throw IndraError("Membership client disconnected before completing exchanges")
    }
  }
}

private func withTestRuntime(
  configuration: NodeConfiguration, _ body: @Sendable (NodeRuntime) async throws -> Void
) async throws {
  let system = try await TCPActorSystem(
    host: configuration.listenHost, advertisedHost: configuration.advertisedHost, port: configuration.port,
    callTimeout: .milliseconds(NodeRuntime.transportTimeoutMilliseconds(configuration: configuration)),
    connectTimeout: .milliseconds(NodeRuntime.transportTimeoutMilliseconds(configuration: configuration)))
  let runtime = NodeRuntime(
    configuration: configuration, configurationPath: "/unused", peerHistoryPath: nil, system: system)
  do {
    try await runtime.start()
    try await body(runtime)
    await runtime.shutdown()
  } catch {
    await runtime.shutdown()
    throw error
  }
}

private func cacheConfiguration(name: String, port: Int) -> NodeConfiguration {
  NodeConfiguration(
    version: 1, id: UUID(), name: name, cluster: "cache-tests", listenHost: "127.0.0.1",
    advertisedHost: "127.0.0.1", port: port, seeds: [], probeIntervalMilliseconds: 100,
    requestTimeoutMilliseconds: 1000)
}
