import Foundation
import NIOCore
import Networking
import TCPActorSystem
import Testing

@testable import IndraNode

@Suite(.timeLimit(.minutes(1)))
struct NodeTests {
  @Test(arguments: ["127.0.0.1", "localhost"])
  func unavailableSeedRetriesAndRestartKeepsStableNodeIdentity(seedHost: String) async throws {
    let reservation = try await TCP.listen(host: "127.0.0.1", port: 0)
    let port = try #require(reservation.channel.localAddress?.port)
    try await reservation.channel.close()
    let address = PeerAddress(host: seedHost, port: port)
    let first = try await TestDaemon.start(configuration: makeConfiguration(name: "first", seeds: [address]))
    do {
      try await eventually { try await first.status().discoveryErrors[address.description] != nil }
      var configuration = makeConfiguration(name: "second", seeds: [])
      configuration.port = port
      let second = try await TestDaemon.start(configuration: configuration)
      do {
        try await eventually {
          try await first.status().members.first { $0.node.id == second.identity.id }?.state == .alive
        }
        try await second.stop()
        try await eventually {
          try await first.status().members.first { $0.node.id == second.identity.id }?.state == .unreachable
        }
        let replacement = try await TestDaemon.start(configuration: configuration)
        do {
          #expect(replacement.identity.id == second.identity.id)
          #expect(replacement.identity.endpoint.node != second.identity.endpoint.node)
          try await eventually {
            let member = try await first.status().members.first { $0.node.id == replacement.identity.id }
            return member?.state == .alive && member?.node.endpoint == replacement.identity.endpoint
          }
          #expect(try await first.status().members.count == 1)
          try await replacement.stop()
        } catch {
          try? await replacement.stop()
          throw error
        }
      } catch {
        try? await second.stop()
        throw error
      }
      try await first.stop()
    } catch {
      try? await first.stop()
      throw error
    }
  }

  @Test func duplicateNamesRequireUUIDsAndDuplicateIDsAreRejected() async throws {
    let first = try await TestDaemon.start(configuration: makeConfiguration(name: "same", seeds: []))
    do {
      var configuration = makeConfiguration(name: "same", seeds: [first.address])
      let second = try await TestDaemon.start(configuration: configuration)
      do {
        try await eventually { try await first.status().members.first?.state == .alive }
        await #expect(throws: IndraError.self) {
          try await NodeControl.request(.ping("same"), socket: first.socket, timeout: .seconds(3))
        }
        guard
          case .pong(let pong) = try await NodeControl.request(
            .ping(second.identity.id.uuidString.lowercased()), socket: first.socket, timeout: .seconds(3))
        else { throw IndraError("Expected pong") }
        #expect(pong.node.id == second.identity.id)
        configuration.id = first.identity.id
        let duplicate = try await TestDaemon.start(configuration: configuration)
        do {
          try await eventually { try await duplicate.status().discoveryErrors[first.address.description] != nil }
          #expect(try await first.status().members.count == 1)
          #expect(try await duplicate.status().members.isEmpty)
          try await duplicate.stop()
        } catch {
          try? await duplicate.stop()
          throw error
        }
        try await second.stop()
      } catch {
        try? await second.stop()
        throw error
      }
      try await first.stop()
    } catch {
      try? await first.stop()
      throw error
    }
  }

  @Test func gossipDeterminesMembershipAndInvalidMessagesAreAtomic() async throws {
    let configuration = makeConfiguration(name: "first", seeds: [])
    let system = try await TCPActorSystem(
      host: "127.0.0.1", advertisedHost: "127.0.0.1", port: 0, callTimeout: .seconds(1), connectTimeout: .seconds(1))
    let runtime = NodeRuntime(
      configuration: configuration, configurationPath: "/unused", peerHistoryPath: nil, system: system)
    do {
      let peer = NodeIdentity(
        id: UUID(), name: "unverified", endpoint: ActorEndpoint(host: "127.0.0.1", port: 7332, node: UUID()), build: nil
      )
      _ = try await runtime.receive(
        MembershipRequest(
          version: 1, cluster: configuration.cluster, node: peer,
          probe: .ping(
            sequenceNumber: 1,
            payload: [
              GossipMember(node: peer, status: .alive(0), protocolPeriod: 0)
            ])))
      #expect(await runtime.status().members.first?.state == .alive)
      let invalid = NodeIdentity(
        id: UUID(), name: "invalid", endpoint: ActorEndpoint(host: "0.0.0.0", port: 7333, node: UUID()), build: nil)
      await #expect(throws: IndraError.self) {
        try await runtime.receive(
          MembershipRequest(
            version: 1, cluster: configuration.cluster, node: peer,
            probe: .ping(
              sequenceNumber: 2,
              payload: [
                GossipMember(node: invalid, status: .alive(0), protocolPeriod: 0)
              ])))
      }
      #expect(await runtime.status().members.count == 1)
      #expect(await runtime.status().members.first?.node == peer)
      #expect(await runtime.status().members.first?.state == .alive)
      await runtime.shutdown()
    } catch {
      await runtime.shutdown()
      throw error
    }
  }

  @Test func seedsDiscoverTransitivePeersAndPingThroughLocalControl() async throws {
    let first = try await TestDaemon.start(configuration: makeConfiguration(name: "first", seeds: []))
    do {
      let second = try await TestDaemon.start(
        configuration: makeConfiguration(name: "second", seeds: [first.address]))
      do {
        let third = try await TestDaemon.start(
          configuration: makeConfiguration(name: "third", seeds: [second.address]))
        do {
          for daemon in [first, second, third] {
            try await eventually {
              let status = try await daemon.status()
              return status.members.filter { $0.state == .alive }.count == 2
            }
            try await eventually {
              guard
                case .services(let services) = try await NodeControl.request(
                  .services, socket: daemon.socket, timeout: .seconds(3))
              else {
                throw IndraError("Expected service listing")
              }
              return services.count == 6 && Set(services.map(\.node.id)).count == 3
            }
          }
          guard
            case .pong(let ping) = try await NodeControl.request(
              .ping("third"), socket: first.socket, timeout: .seconds(3))
          else {
            throw IndraError("Expected pong")
          }
          #expect(ping.node.id == third.identity.id)
          #expect(ping.milliseconds > 0)
          guard
            case .pong(let byID) = try await NodeControl.request(
              .ping(second.identity.id.uuidString), socket: third.socket, timeout: .seconds(3))
          else { throw IndraError("Expected pong by UUID") }
          #expect(byID.node.id == second.identity.id)
          try await third.stop()
          try await eventually {
            try await first.status().members.first { $0.node.id == third.identity.id }?.state == .unreachable
          }
        } catch {
          try? await third.stop()
          throw error
        }
        try await second.stop()
      } catch {
        try? await second.stop()
        throw error
      }
      try await first.stop()
    } catch {
      try? await first.stop()
      throw error
    }
  }

  @Test func explicitReloadKeepsIdentityAndRejectsRestartChanges() async throws {
    let first = try await TestDaemon.start(configuration: makeConfiguration(name: "first", seeds: []))
    do {
      let second = try await TestDaemon.start(configuration: makeConfiguration(name: "second", seeds: []))
      do {
        var updated = try NodeConfiguration.load(path: first.config)
        updated.name = "renamed"
        updated.seeds = [second.address]
        try updated.save(path: first.config, overwrite: true)
        try await Task.sleep(for: .milliseconds(1100))
        #expect(try await first.status().node.name == "first")
        _ = try await NodeControl.request(.reload, socket: first.socket, timeout: .seconds(3))
        try await eventually {
          let status = try await first.status()
          guard
            case .services(let services) = try await NodeControl.request(
              .services, socket: first.socket, timeout: .seconds(3))
          else {
            throw IndraError("Expected service listing")
          }
          let local = services.filter { $0.node.id == first.identity.id }
          return status.node.name == "renamed" && status.members.contains { $0.node.id == second.identity.id }
            && local.count == 2 && local.allSatisfy { $0.node.name == "renamed" }
        }
        #expect(try await first.status().node.id == first.identity.id)
        var changedPort = updated
        changedPort.port = 9999
        var changedInterval = updated
        changedInterval.probeIntervalMilliseconds = 60_000
        for invalid in [changedPort, changedInterval] {
          try invalid.save(path: first.config, overwrite: true)
          await #expect(throws: IndraError.self) {
            try await NodeControl.request(.reload, socket: first.socket, timeout: .seconds(3))
          }
          #expect(try await first.status().configurationError?.contains("require a restart") == true)
        }
        #expect(try await first.status().node.endpoint == first.identity.endpoint)
        try updated.save(path: first.config, overwrite: true)
        _ = try await NodeControl.request(.reload, socket: first.socket, timeout: .seconds(3))
        #expect(try await first.status().configurationError == nil)
        try await second.stop()
      } catch {
        try? await second.stop()
        throw error
      }
      try await first.stop()
    } catch {
      try? await first.stop()
      throw error
    }
  }

  @Test func addedSeedsAreContactedWhileAlreadyConnected() async throws {
    let first = try await TestDaemon.start(configuration: makeConfiguration(name: "first", seeds: []))
    do {
      let second = try await TestDaemon.start(configuration: makeConfiguration(name: "second", seeds: [first.address]))
      do {
        try await eventually { try await first.status().members.first?.state == .alive }
        let third = try await TestDaemon.start(configuration: makeConfiguration(name: "third", seeds: []))
        do {
          var configuration = try NodeConfiguration.load(path: first.config)
          configuration.seeds = [third.address]
          try configuration.save(path: first.config, overwrite: true)
          _ = try await NodeControl.request(.reload, socket: first.socket, timeout: .seconds(3))
          try await eventually {
            try await first.status().members.contains { $0.node.id == third.identity.id && $0.state == .alive }
          }
          try await third.stop()
        } catch {
          try? await third.stop()
          throw error
        }
        try await second.stop()
      } catch {
        try? await second.stop()
        throw error
      }
      try await first.stop()
    } catch {
      try? await first.stop()
      throw error
    }
  }

  @Test(arguments: [false, true])
  func explicitReloadReportsMalformedAndMissingFiles(removeFile: Bool) async throws {
    let configuration = makeConfiguration(name: "first", seeds: [])
    let daemon = try await TestDaemon.start(configuration: configuration)
    do {
      if removeFile {
        try FileManager.default.removeItem(atPath: daemon.config)
      } else {
        try Data("invalid JSON".utf8).write(to: URL(fileURLWithPath: daemon.config), options: .atomic)
      }
      await #expect(throws: IndraError.self) {
        try await NodeControl.request(.reload, socket: daemon.socket, timeout: .seconds(3))
      }
      #expect(try await daemon.status().configurationError != nil)
      #expect(try await daemon.status().node == daemon.identity)
      var restored = configuration
      restored.name = "restored"
      try restored.save(path: daemon.config, overwrite: true)
      _ = try await NodeControl.request(.reload, socket: daemon.socket, timeout: .seconds(3))
      let status = try await daemon.status()
      #expect(status.configurationError == nil)
      #expect(status.node.name == "restored")
      #expect(status.node.endpoint == daemon.identity.endpoint)
      try await daemon.stop()
    } catch {
      try? await daemon.stop()
      throw error
    }
  }

  @Test func reloadContactsNewSeedsBeforeTheNextProbeTick() async throws {
    let reservation = try await TCP.listen(host: "127.0.0.1", port: 0)
    let port = try #require(reservation.channel.localAddress?.port)
    try await reservation.channel.close()
    let address = PeerAddress(host: "127.0.0.1", port: port)
    var configuration = makeConfiguration(name: "first", seeds: [address])
    configuration.probeIntervalMilliseconds = 60_000
    let first = try await TestDaemon.start(configuration: configuration)
    do {
      try await eventually { try await first.status().discoveryErrors[address.description] != nil }
      var peerConfiguration = makeConfiguration(name: "second", seeds: [])
      peerConfiguration.probeIntervalMilliseconds = 60_000
      let second = try await TestDaemon.start(configuration: peerConfiguration)
      do {
        configuration.seeds = [second.address]
        try configuration.save(path: first.config, overwrite: true)
        _ = try await NodeControl.request(.reload, socket: first.socket, timeout: .seconds(3))
        try await eventually {
          try await first.status().members.first { $0.node.id == second.identity.id }?.state == .alive
        }
        try await eventually {
          guard
            case .services(let services) = try await NodeControl.request(
              .services, socket: first.socket, timeout: .seconds(3))
          else {
            throw IndraError("Expected service listing")
          }
          return services.filter { $0.node.id == second.identity.id }.count == 2
        }
        try await second.stop()
      } catch {
        try? await second.stop()
        throw error
      }
      try await first.stop()
    } catch {
      try? await first.stop()
      throw error
    }
  }

  @Test func joinPersistsSeedsAndWrongClustersAreRejected() async throws {
    let first = try await TestDaemon.start(configuration: makeConfiguration(name: "first", seeds: []))
    do {
      var foreignConfiguration = makeConfiguration(name: "foreign", seeds: [])
      foreignConfiguration.cluster = "elsewhere"
      let foreign = try await TestDaemon.start(configuration: foreignConfiguration)
      do {
        await #expect(throws: IndraError.self) {
          try await NodeControl.request(.join(foreign.address), socket: first.socket, timeout: .seconds(3))
        }
        #expect(try NodeConfiguration.load(path: first.config).seeds == [foreign.address])
        #expect(try await first.status().members.isEmpty)
        #expect(try await foreign.status().members.isEmpty)
        #expect(try await first.status().discoveryErrors[foreign.address.description] != nil)
        try await foreign.stop()
      } catch {
        try? await foreign.stop()
        throw error
      }
      try await first.stop()
    } catch {
      try? await first.stop()
      throw error
    }
  }

  @Test func offlineJoinStaysSavedAndRetriesWhileConnected() async throws {
    let existing = try await TestDaemon.start(configuration: makeConfiguration(name: "existing", seeds: []))
    do {
      let first = try await TestDaemon.start(configuration: makeConfiguration(name: "first", seeds: [existing.address]))
      do {
        try await eventually {
          try await first.status().members.contains { $0.node.id == existing.identity.id && $0.state == .alive }
        }
        let reservation = try await TCP.listen(host: "127.0.0.1", port: 0)
        let address = PeerAddress(host: "127.0.0.1", port: try #require(reservation.channel.localAddress?.port))
        try await reservation.channel.close()
        await #expect(throws: IndraError.self) {
          try await NodeControl.request(.join(address), socket: first.socket, timeout: .seconds(3))
        }
        #expect(try NodeConfiguration.load(path: first.config).seeds == [existing.address, address])
        #expect(try await first.status().discoveryErrors[address.description] != nil)
        var configuration = makeConfiguration(name: "returning", seeds: [])
        configuration.port = address.port
        let returning = try await TestDaemon.start(configuration: configuration)
        do {
          try await eventually {
            let status = try await first.status()
            return [existing.identity.id, returning.identity.id].allSatisfy { id in
              status.members.contains { $0.node.id == id && $0.state == .alive }
            } && status.discoveryErrors[address.description] == nil
          }
          try await returning.stop()
        } catch {
          try? await returning.stop()
          throw error
        }
        try await first.stop()
      } catch {
        try? await first.stop()
        throw error
      }
      try await existing.stop()
    } catch {
      try? await existing.stop()
      throw error
    }
  }

  @Test func duplicateDaemonCannotRemoveLiveSocketAndIdleClientsDoNotBlockShutdown() async throws {
    let daemon = try await TestDaemon.start(configuration: makeConfiguration(name: "first", seeds: []))
    do {
      await #expect(throws: IndraError.self) {
        try await runNode(configurationPath: daemon.config, socket: daemon.socket, onReady: { _ in })
      }
      #expect(try await daemon.status().node.id == daemon.identity.id)
      let idle = try await TCP.connect(unixDomainSocketPath: daemon.socket, connectTimeout: .seconds(1))
      try await daemon.stop()
      try? await idle.channel.close()
      #expect(!FileManager.default.fileExists(atPath: daemon.socket))
    } catch {
      try? await daemon.stop()
      throw error
    }
  }
}

private struct TestDaemon: Sendable {
  let config: String
  let socket: String
  let root: URL
  let identity: NodeIdentity
  let task: Task<Void, any Error>

  var address: PeerAddress { PeerAddress(host: identity.endpoint.host, port: identity.endpoint.port) }

  static func start(configuration: NodeConfiguration) async throws -> Self {
    let root = URL(fileURLWithPath: "/tmp/indra-\(UUID().uuidString)")
    let config = root.appendingPathComponent("config.json").path
    let socket = root.appendingPathComponent("run/control.sock").path
    try configuration.save(path: config, overwrite: false)
    let (events, ready) = AsyncThrowingStream<NodeStatus, any Error>.makeStream(bufferingPolicy: .bufferingNewest(1))
    let task = Task {
      do {
        try await runNode(configurationPath: config, socket: socket) {
          ready.yield($0)
          ready.finish()
        }
      } catch {
        ready.finish(throwing: error)
        throw error
      }
    }
    do {
      var iterator = events.makeAsyncIterator()
      let status = try #require(try await iterator.next())
      return Self(config: config, socket: socket, root: root, identity: status.node, task: task)
    } catch {
      task.cancel()
      _ = try? await task.value
      try? FileManager.default.removeItem(at: root)
      throw error
    }
  }

  func status() async throws -> NodeStatus {
    guard case .status(let status) = try await NodeControl.request(.status, socket: socket, timeout: .seconds(3)) else {
      throw IndraError("Expected status")
    }
    return status
  }

  func stop() async throws {
    task.cancel()
    do { try await task.value } catch is CancellationError {}
    try? FileManager.default.removeItem(at: root)
  }
}

private func eventually(_ condition: @Sendable () async throws -> Bool) async throws {
  let deadline = ContinuousClock.now.advanced(by: .seconds(10))
  while try await !condition() {
    guard ContinuousClock.now < deadline else { throw IndraError("Condition did not become true") }
    try await Task.sleep(for: .milliseconds(25))
  }
}
