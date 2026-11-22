import Foundation
import IndraHealth
import NIOCore
import Networking
import Testing
import _NIOFileSystem

@testable import IndraNode
@testable import TCPActorSystem

@Suite(.timeLimit(.minutes(1)))
struct MembershipCachingTests {
  @Test func actorSystemRejectsASecondSetOfBootstrapActors() async throws {
    let configuration = cacheConfiguration(name: "client", port: 0)
    try await withTestRuntime(configuration: configuration, configurationPath: "/unused", services: []) { runtime in
      let system = await runtime.system
      let duplicate = NodeRuntime(
        configuration: configuration, configurationPath: "/unused", peerHistoryPath: nil, system: system)
      await #expect(throws: RPCError.invalidConfiguration) { try await duplicate.start(services: []) }
      #expect(await runtime.receptionist.services().count == 2)
    }
  }

  @Test func healthReportsResponsiveTimedOutAndUnavailableNodesTogether() async throws {
    let listener = try await TCP.listen(host: "127.0.0.1", port: 0)
    defer { listener.channel.close(promise: nil) }
    let port = try #require(listener.channel.localAddress?.port)
    let stalled = NodeIdentity(
      id: UUID(), name: "stalled", endpoint: ActorEndpoint(host: "127.0.0.1", port: port, systemID: UUID()), build: nil)
    let legacy = NodeIdentity(
      id: UUID(), name: "legacy", endpoint: ActorEndpoint(host: "localhost", port: port, systemID: UUID()), build: nil)
    try await withTestRuntime(
      configuration: cacheConfiguration(name: "client", port: 0), configurationPath: "/unused",
      services: [HealthService.install]
    ) { runtime in
      _ = try await runtime.receive(
        MembershipRequest(
          version: 1, cluster: "cache-tests", node: stalled,
          probe: .ping(
            sequenceNumber: 1,
            payload: [stalled, legacy].map { GossipMember(node: $0, status: .alive(0), protocolPeriod: 0) })))
      let registration = ServiceRegistration(
        key: HealthService.key.name, actorType: String(reflecting: HealthService.self),
        address: ActorAddress(endpoint: stalled.endpoint, id: UUID()))
      try await runtime.receptionist.accept(
        ServiceSnapshot(version: 1, cluster: "cache-tests", node: stalled, revision: 1, registrations: [registration]),
        from: stalled)

      let status = await runtime.status()
      let payload = try await runtime.receptionist.query(HealthService.key.name, status: status)
      let reports = try JSONDecoder().decode([ServiceReport<HealthMetrics>].self, from: payload)
      #expect(reports.count == 3)
      guard case .responded(let local)? = reports.first?.outcome else {
        throw IndraError("Expected a local health report")
      }
      #expect(local.freeDiskBytes > 0)
      guard case .timedOut? = reports.first(where: { $0.node == stalled })?.outcome else {
        throw IndraError("Expected the stalled health service to time out")
      }
      guard case .unavailable? = reports.first(where: { $0.node == legacy })?.outcome else {
        throw IndraError("Expected the peer without a health service to be listed")
      }
    }
  }

  @Test func joinsAndBackgroundDiscoveryShareOneProbe() async throws {
    let files = FileSystem.shared
    let root = try await files.createTemporaryDirectory(template: files.temporaryDirectory.appending("XXXXXXXX"))
    defer { _ = try? await files.removeItem(at: root, strategy: .sequential, recursively: true) }
    let path = root.appending("config.json").string
    var configuration = cacheConfiguration(name: "client", port: 0)
    configuration.requestTimeoutMilliseconds = 5000
    try await configuration.save(path: path, overwrite: false)
    let listener = try await TCP.listen(host: "127.0.0.1", port: 0)
    let endpoint = ActorEndpoint(
      host: "127.0.0.1", port: try #require(listener.channel.localAddress?.port), systemID: UUID())
    let peer = NodeIdentity(id: UUID(), name: "seed", endpoint: endpoint, build: nil)
    let (started, start) = AsyncStream<Void>.makeStream(bufferingPolicy: .bufferingNewest(1))
    let (released, release) = AsyncStream<Void>.makeStream(bufferingPolicy: .bufferingNewest(1))
    defer {
      start.finish()
      release.finish()
    }
    let server = Task {
      try await serveMembership(
        listener: listener, peer: peer, exchanges: 1, includeGossip: true,
        beforeReply: { _ in
          start.yield(())
          for await _ in released { break }
          try Task.checkCancellation()
        })
    }
    do {
      try await withTestRuntime(configuration: configuration, configurationPath: path, services: []) { runtime in
        let membership = Task { try await runtime.runMembership() }
        defer { membership.cancel() }
        let first = Task { try await runtime.join(peer.address) }
        defer { first.cancel() }
        var arrivals = started.makeAsyncIterator()
        _ = try #require(await arrivals.next())
        var saved = try await NodeConfiguration.load(path: path)
        #expect(saved.seeds == [peer.address])
        saved.name = "second-join"
        try await saved.save(path: path, overwrite: true)
        let second = Task { try await runtime.join(peer.address) }
        defer { second.cancel() }
        let period = await runtime.monitor().protocolPeriod
        let deadline = ContinuousClock.now.advanced(by: .seconds(3))
        while true {
          let monitor = await runtime.monitor()
          if monitor.status.node.name == saved.name, monitor.protocolPeriod > period { break }
          guard ContinuousClock.now < deadline else { throw IndraError("Second join or tick did not run") }
          try await Task.sleep(for: .milliseconds(10))
        }
        try await runtime.reload()
        first.cancel()
        await #expect(throws: CancellationError.self) { try await first.value }
        let pending = await runtime.monitor()
        #expect(pending.outbound.ping == 1)
        #expect(pending.inFlight == 1)
        release.yield(())
        #expect(try await second.value.node == peer)
        #expect(try await NodeConfiguration.load(path: path).seeds == [peer.address])
        try await server.value
        membership.cancel()
        do { try await membership.value } catch is CancellationError {}
      }
    } catch {
      release.finish()
      server.cancel()
      try? await listener.channel.close()
      _ = try? await server.value
      throw error
    }
  }

  @Test(arguments: [false, true])
  func pingsReuseTheHandshakeAndAddressTheReservedActor(viaAlias: Bool) async throws {
    let listener = try await TCP.listen(host: "127.0.0.1", port: 0)
    let endpoint = ActorEndpoint(
      host: "127.0.0.1", port: try #require(listener.channel.localAddress?.port), systemID: UUID())
    let peer = NodeIdentity(id: UUID(), name: "peer", endpoint: endpoint, build: nil)
    let server = Task {
      try await serveMembership(
        listener: listener, peer: peer, exchanges: 4, includeGossip: !viaAlias, beforeReply: { _ in })
    }
    do {
      try await withTestRuntime(
        configuration: cacheConfiguration(name: "client", port: 0), configurationPath: "/unused", services: []
      ) { runtime in
        let address = PeerAddress(host: viaAlias ? "localhost" : endpoint.host, port: endpoint.port)
        let first = try await runtime.ping(address.description)
        #expect(first.node == peer)
        let registration = ServiceRegistration(
          key: MembershipService.serviceName, actorType: String(reflecting: MembershipService.self),
          address: ActorAddress(endpoint: peer.endpoint, id: MembershipService.fixedID))
        try await runtime.receptionist.accept(
          ServiceSnapshot(version: 1, cluster: "cache-tests", node: peer, revision: 1, registrations: [registration]),
          from: peer)
        #expect(await runtime.receptionist.services().contains { $0.registration == registration })
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
    let listener = try await TCP.listen(host: "127.0.0.1", port: 0)
    let port = try #require(listener.channel.localAddress?.port)
    let original = NodeIdentity(
      id: UUID(), name: "peer", endpoint: ActorEndpoint(host: "127.0.0.1", port: port, systemID: UUID()), build: nil)
    let replacement = NodeIdentity(
      id: original.id, name: original.name,
      endpoint: ActorEndpoint(host: original.endpoint.host, port: port, systemID: UUID()), build: nil)
    let (originalClosed, signalClose) = AsyncStream<Void>.makeStream(bufferingPolicy: .bufferingNewest(1))

    try await listener.executeThenClose { incoming in
      try await withThrowingDiscardingTaskGroup { tasks in
        tasks.addTask {
          defer { signalClose.finish() }
          var connections = incoming.makeAsyncIterator()
          let first = try #require(try await connections.next())
          try await replyToMembershipPings(
            connection: first, peer: original, exchanges: 1, includeGossip: true, beforeReply: { _ in })
          signalClose.yield(())
          let second = try #require(try await connections.next())
          try await replyToMembershipPings(
            connection: second, peer: replacement, exchanges: 2, includeGossip: true, beforeReply: { _ in })
        }
        tasks.addTask {
          try await withTestRuntime(
            configuration: cacheConfiguration(name: "client", port: 0), configurationPath: "/unused", services: []
          ) { client in
            try #require(try await client.ping(original.address.description).node == original)
            var closures = originalClosed.makeAsyncIterator()
            _ = try #require(await closures.next())

            #expect(try await client.ping(original.name).node == replacement)
            #expect(try await client.ping(original.id.uuidString).node == replacement)
            let members = await client.status().members
            #expect(members.count == 1)
            #expect(members.first?.node == replacement)
            #expect(members.first?.state == .alive)
          }
        }
      }
    }
  }

  @Test func liveProcessesWithTheSameIDReportAConflict() async throws {
    let configuration = cacheConfiguration(name: "peer", port: 0)
    try await withTestRuntime(configuration: configuration, configurationPath: "/unused", services: []) { first in
      try await withTestRuntime(configuration: configuration, configurationPath: "/unused", services: []) { second in
        try await withTestRuntime(
          configuration: cacheConfiguration(name: "observer", port: 0),
          configurationPath: "/unused", services: []
        ) { observer in
          let original = await first.status().node
          let conflict = await second.status().node
          #expect(try await observer.ping(original.address.description).node == original)
          do {
            _ = try await observer.ping(conflict.address.description)
            Issue.record("Expected a duplicate UUID conflict")
          } catch let error as IndraError {
            #expect(error.description.contains("Two live processes"))
          }
          #expect(try await observer.ping(original.id.uuidString).node == original)
          #expect(await observer.status().members.map(\.node) == [original])
        }
      }
    }
  }

  @Test func verifiedMoveFencesLateRepliesAndGossipAtTheOldAddress() throws {
    let id = UUID()
    let first = NodeIdentity(
      id: id, name: "peer", endpoint: ActorEndpoint(host: "localhost", port: 7331, systemID: UUID()), build: nil)
    let second = NodeIdentity(
      id: id, name: "peer", endpoint: ActorEndpoint(host: "localhost", port: 7332, systemID: UUID()), build: nil)
    let advertised = NodeIdentity(id: id, name: first.name, endpoint: first.endpoint, build: .current)
    var directory = PeerDirectory()
    let learned = directory.learn(advertised)
    #expect(learned)
    let verified = try directory.verify(first, observed: directory.verification, checkedIncumbent: nil)
    #expect(!verified.identityChanged)
    #expect(directory.incumbent(for: second) == advertised)
    let beforeMove = directory.verification
    let move = try directory.verify(second, observed: beforeMove, checkedIncumbent: first.endpoint)
    #expect(move.retired == [advertised])
    #expect(move.identityChanged)
    let restored = directory.learn(first)
    #expect(!restored)
    #expect(throws: IndraError.self) {
      try directory.verify(first, observed: beforeMove, checkedIncumbent: nil)
    }
    #expect(directory.accepts(second))
  }

  @Test func diagnosticPingRetriesTheDisconnectAndDiscoversReplacementServices() async throws {
    let configuration = cacheConfiguration(name: "peer", port: 0)
    try await withTestRuntime(configuration: configuration, configurationPath: "/unused", services: []) { replacement in
      let current = await replacement.status().node
      let original = NodeIdentity(
        id: current.id, name: current.name,
        endpoint: ActorEndpoint(host: current.endpoint.host, port: current.endpoint.port, systemID: UUID()), build: nil)
      let listener = try await TCP.listen(host: "127.0.0.1", port: 0)
      let alias = PeerAddress(host: "127.0.0.1", port: try #require(listener.channel.localAddress?.port))
      let server = Task {
        try await serveMembership(
          listener: listener, peer: original, exchanges: 2, includeGossip: true,
          beforeReply: { completed in
            if completed == 1 { throw RPCError.disconnected }
          })
      }
      do {
        try await withTestRuntime(
          configuration: cacheConfiguration(name: "client", port: 0), configurationPath: "/unused", services: []
        ) { client in
          #expect(try await client.ping(alias.description).node == original)
          let registration = ServiceRegistration(
            key: MembershipService.serviceName, actorType: String(reflecting: MembershipService.self),
            address: ActorAddress(endpoint: original.endpoint, id: MembershipService.fixedID))
          let receptionist = client.receptionist
          try await receptionist.accept(
            ServiceSnapshot(
              version: 1, cluster: "cache-tests", node: original, revision: 1, registrations: [registration]),
            from: original)
          #expect(await receptionist.services().contains { $0.node == original })

          #expect(try await client.ping(original.name).node == current)
          await #expect(throws: RPCError.disconnected) { try await server.value }
          let events = await client.monitor().events.filter { $0.peer == current.address }
          #expect(events.map(\.kind) == [.ping, .failure, .ping, .ack])
          #expect(events.first?.sequenceNumber != events.last?.sequenceNumber)
          let members = await client.status().members
          #expect(members.count == 1)
          #expect(members.first?.node == current)
          #expect(members.first?.state == .alive)
          await receptionist.refresh()
          let remoteServices = await receptionist.services().filter { $0.node.id == current.id }
          #expect(remoteServices.count == 2)
          #expect(remoteServices.allSatisfy { $0.registration.address.endpoint == current.endpoint })
        }
      } catch {
        server.cancel()
        try? await listener.channel.close()
        _ = try? await server.value
        throw error
      }
    }
  }

  @Test func diagnosticPingRetryUsesTheOriginalDeadline() async throws {
    let oldListener = try await TCP.listen(host: "127.0.0.1", port: 0)
    let newListener = try await TCP.listen(host: "127.0.0.1", port: 0)
    defer {
      oldListener.channel.close(promise: nil)
      newListener.channel.close(promise: nil)
    }
    let alias = PeerAddress(host: "127.0.0.1", port: try #require(oldListener.channel.localAddress?.port))
    let port = try #require(newListener.channel.localAddress?.port)
    let id = UUID()
    let original = NodeIdentity(
      id: id, name: "peer", endpoint: ActorEndpoint(host: alias.host, port: port, systemID: UUID()), build: nil)
    let replacement = NodeIdentity(
      id: id, name: original.name, endpoint: ActorEndpoint(host: alias.host, port: port, systemID: UUID()), build: nil)
    let oldServer = Task {
      try await serveMembership(
        listener: oldListener, peer: original, exchanges: 2, includeGossip: true,
        beforeReply: { completed in
          if completed == 1 {
            try await Task.sleep(for: .milliseconds(650))
            throw RPCError.disconnected
          }
        })
    }
    let newServer = Task {
      try await serveMembership(
        listener: newListener, peer: replacement, exchanges: 1, includeGossip: true,
        beforeReply: { _ in try await Task.sleep(for: .milliseconds(650)) })
    }
    defer {
      oldServer.cancel()
      newServer.cancel()
    }
    try await withTestRuntime(
      configuration: cacheConfiguration(name: "client", port: 0), configurationPath: "/unused", services: []
    ) { client in
      #expect(try await client.ping(alias.description).node == original)
      do {
        _ = try await client.ping(original.name)
        Issue.record("Expected the original ping deadline to expire")
      } catch let error as IndraError {
        #expect(error.description == "SWIM probe timed out")
      }
      let events = await client.monitor().events.filter { $0.peer == original.address }
      #expect(events.map(\.kind) == [.ping, .failure, .ping, .failure])
      #expect(await client.status().members.first?.node == original)
    }
  }

  @Test(arguments: [false, true])
  func staleGossipCannotReplaceVerifiedProcess(knowPrevious: Bool) async throws {
    try await withTestRuntime(
      configuration: cacheConfiguration(name: "client", port: 0), configurationPath: "/unused", services: []
    ) { client in
      try await withTestRuntime(
        configuration: cacheConfiguration(name: "peer", port: 0), configurationPath: "/unused", services: []
      ) { peer in
        let current = await peer.status().node
        let previous = NodeIdentity(
          id: current.id, name: current.name,
          endpoint: ActorEndpoint(host: current.endpoint.host, port: current.endpoint.port, systemID: UUID()),
          build: nil)
        let reporter = NodeIdentity(
          id: UUID(), name: "reporter", endpoint: ActorEndpoint(host: "127.0.0.1", port: 7332, systemID: UUID()),
          build: nil
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
        host: "127.0.0.1", port: try #require(originalListener.channel.localAddress?.port), systemID: UUID()),
      build: nil)
    let replacement = NodeIdentity(
      id: original.id, name: original.name,
      endpoint: ActorEndpoint(host: original.endpoint.host, port: original.endpoint.port, systemID: UUID()), build: nil)
    let replacementAddress = "127.0.0.1:\(try #require(replacementListener.channel.localAddress?.port))"
    let (started, start) = AsyncStream<Void>.makeStream(bufferingPolicy: .bufferingNewest(1))
    let (released, release) = AsyncStream<Void>.makeStream(bufferingPolicy: .bufferingNewest(1))
    defer {
      start.finish()
      release.finish()
    }
    let originalServer = Task {
      try await serveMembership(
        listener: originalListener, peer: original, exchanges: knowPrevious ? 2 : 1, includeGossip: true,
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
        listener: replacementListener, peer: replacement, exchanges: 2, includeGossip: true, beforeReply: { _ in })
    }
    do {
      try await withTestRuntime(
        configuration: cacheConfiguration(name: "client", port: 0), configurationPath: "/unused", services: []
      ) { client in
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
  listener: Listener, peer: NodeIdentity, exchanges: Int, includeGossip: Bool,
  beforeReply: @Sendable (Int) async throws -> Void
) async throws {
  try await listener.executeThenClose { connections in
    var connections = connections.makeAsyncIterator()
    let connection = try #require(try await connections.next())
    try await replyToMembershipPings(
      connection: connection, peer: peer, exchanges: exchanges, includeGossip: includeGossip, beforeReply: beforeReply)
  }
}

private func replyToMembershipPings(
  connection: Connection, peer: NodeIdentity, exchanges: Int, includeGossip: Bool,
  beforeReply: @Sendable (Int) async throws -> Void
) async throws {
  try await connection.executeThenClose { input, output in
    try await output.write(
      WireMessage.hello(version: WireMessage.version, endpoint: peer.endpoint, capabilities: nil).frame())
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
            payload: includeGossip ? [GossipMember(node: peer, status: .alive(0), protocolPeriod: 0)] : [],
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

func withTestRuntime(
  configuration: NodeConfiguration, configurationPath: String, services: [ServiceInstaller],
  _ body: @Sendable (NodeRuntime) async throws -> Void
) async throws {
  let system = try await TCPActorSystem(
    host: configuration.listenHost, advertisedHost: configuration.advertisedHost, port: configuration.port,
    callTimeout: .milliseconds(NodeRuntime.transportTimeoutMilliseconds(configuration: configuration)),
    connectTimeout: .milliseconds(NodeRuntime.transportTimeoutMilliseconds(configuration: configuration)))
  let runtime = NodeRuntime(
    configuration: configuration, configurationPath: configurationPath, peerHistoryPath: nil, system: system)
  do {
    try await runtime.start(services: services)
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
