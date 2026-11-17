import Foundation
import NIOCore
import Networking
import Testing

@testable import IndraNode
@testable import TCPActorSystem

@Suite(.timeLimit(.minutes(1)))
struct MembershipCachingTests {
  @Test(arguments: [false, true])
  func pingsAndDiscoveryReuseLookupAndRefreshReplacedActors(replaceService: Bool) async throws {
    let listener = try await TCP.listen(host: "127.0.0.1", port: 0)
    let endpoint = ActorEndpoint(
      host: "127.0.0.1", port: try #require(listener.channel.localAddress?.port), node: UUID())
    let peer = NodeIdentity(id: UUID(), name: "peer", endpoint: endpoint)
    let server = Task {
      try await serveMembership(listener: listener, peer: peer, exchanges: 4, replaceService: replaceService)
    }
    do {
      try await withTestRuntime(configuration: cacheConfiguration(name: "client", port: 0)) { runtime in
        let first = try await runtime.ping("127.0.0.1:\(endpoint.port)")
        #expect(first.node == peer)
        #expect(try await runtime.ping(peer.name).node == peer)
        #expect(try await runtime.ping(peer.id.uuidString).node == peer)
        await runtime.discover()
        let counts = try await server.value
        #expect(counts.lookups == (replaceService ? 2 : 1))
        #expect(counts.calls == (replaceService ? 5 : 4))
        #expect(await runtime.status().members.first?.state == .reachable)
      }
    } catch {
      server.cancel()
      try? await listener.channel.close()
      _ = try? await server.value
      throw error
    }
  }

  @Test func firstPingAfterPeerRestartRefreshesCachedService() async throws {
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
          #expect(members.first?.state == .reachable)
        }
      }
    }
  }
}

private struct MembershipRequestCounts: Sendable {
  let lookups: Int
  let calls: Int
}

private func serveMembership(listener: Listener, peer: NodeIdentity, exchanges: Int, replaceService: Bool) async throws
  -> MembershipRequestCounts
{
  try await listener.executeThenClose { connections in
    var connections = connections.makeAsyncIterator()
    let connection = try #require(try await connections.next())
    return try await connection.executeThenClose { input, output in
      try await output.write(WireMessage.hello(peer.endpoint).frame())
      var address = ActorAddress(endpoint: peer.endpoint, id: UUID())
      var lookups = 0
      var calls = 0
      var completed = 0
      for try await frame in input {
        switch try WireMessage(frame: frame) {
        case .hello: break
        case .lookup(let id, let name, let actorType):
          #expect(name == MembershipService.serviceName)
          #expect(actorType == String(reflecting: MembershipService.self))
          lookups += 1
          try await output.write(WireMessage.reply(id, .value(try JSONEncoder().encode(address))).frame())
        case .call(let invocation):
          calls += 1
          guard invocation.actorID == address else {
            try await output.write(WireMessage.reply(invocation.requestID, .systemError(.unknownActor)).frame())
            continue
          }
          let response = MembershipExchange(version: 1, cluster: "cache-tests", node: peer, peers: [])
          try await output.write(
            WireMessage.reply(invocation.requestID, .value(try JSONEncoder().encode(response))).frame())
          completed += 1
          if completed == exchanges { return MembershipRequestCounts(lookups: lookups, calls: calls) }
          if replaceService && completed == 1 { address = ActorAddress(endpoint: peer.endpoint, id: UUID()) }
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
    callTimeout: .milliseconds(configuration.requestTimeoutMilliseconds),
    connectTimeout: .milliseconds(Int64(configuration.requestTimeoutMilliseconds)))
  let runtime = NodeRuntime(configuration: configuration, configurationPath: "/unused", system: system)
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
