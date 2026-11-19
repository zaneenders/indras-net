import Foundation
import IndraHealth
import IndraSetu
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
    let first = try await TestDaemon.start(
      configuration: makeConfiguration(name: "first", seeds: [address]), services: [])
    do {
      try await eventually { try await first.status().discoveryErrors[address.description] != nil }
      var configuration = makeConfiguration(name: "second", seeds: [])
      configuration.port = port
      let second = try await TestDaemon.start(configuration: configuration, services: [])
      do {
        try await eventually {
          try await first.status().members.first { $0.node.id == second.identity.id }?.state == .alive
        }
        try await second.stop()
        try await eventually {
          try await first.status().members.first { $0.node.id == second.identity.id }?.state == .unreachable
        }
        let replacement = try await TestDaemon.start(configuration: configuration, services: [])
        do {
          #expect(replacement.identity.id == second.identity.id)
          #expect(replacement.identity.endpoint.systemID != second.identity.endpoint.systemID)
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
    let first = try await TestDaemon.start(configuration: makeConfiguration(name: "same", seeds: []), services: [])
    do {
      var configuration = makeConfiguration(name: "same", seeds: [first.address])
      let second = try await TestDaemon.start(configuration: configuration, services: [])
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
        let duplicate = try await TestDaemon.start(configuration: configuration, services: [])
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
        id: UUID(), name: "unverified", endpoint: ActorEndpoint(host: "127.0.0.1", port: 7332, systemID: UUID()),
        build: nil
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
        id: UUID(), name: "invalid", endpoint: ActorEndpoint(host: "0.0.0.0", port: 7333, systemID: UUID()), build: nil)
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
    let first = try await TestDaemon.start(
      configuration: makeConfiguration(name: "first", seeds: []), services: [HealthService.install])
    do {
      let second = try await TestDaemon.start(
        configuration: makeConfiguration(name: "second", seeds: [first.address]), services: [HealthService.install])
      do {
        let third = try await TestDaemon.start(
          configuration: makeConfiguration(name: "third", seeds: [second.address]), services: [])
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
              return services.count == 8 && Set(services.map(\.node.id)).count == 3
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
          let reports = try await NodeControl.query(
            HealthService.key.name, returning: HealthMetrics.self, socket: first.socket, timeout: .seconds(4))
          #expect(Set(reports.map(\.node.id)) == Set([first.identity.id, second.identity.id, third.identity.id]))
          let withoutHealth = try #require(reports.first { $0.node.id == third.identity.id })
          guard case .unavailable = withoutHealth.outcome else { throw IndraError("Expected health to be unavailable") }
          for report in reports where report.node.id != third.identity.id {
            guard case .responded(let metrics) = report.outcome else {
              throw IndraError("Expected health metrics from \(report.node.name)")
            }
            #expect(metrics.uptimeSeconds > 0)
            #expect(metrics.freeDiskBytes > 0)
          }
          do {
            _ = try await NodeControl.query(
              "test.missing", returning: HealthMetrics.self, socket: first.socket, timeout: .seconds(4))
            Issue.record("Expected the daemon to reject an unregistered query")
          } catch let error as IndraError {
            #expect(error.description == "Unknown service query test.missing")
          }
          try await third.stop()
          try await eventually {
            try await first.status().members.first { $0.node.id == third.identity.id }?.state == .unreachable
          }
          let afterStop = try await NodeControl.query(
            HealthService.key.name, returning: HealthMetrics.self, socket: first.socket, timeout: .seconds(4))
          let offline = try #require(afterStop.first { $0.node.id == third.identity.id })
          guard case .unreachable = offline.outcome else { throw IndraError("Expected an unreachable peer") }
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

  @Test func serviceReportFrameContainsTypedJSON() async throws {
    let daemon = try await TestDaemon.start(
      configuration: makeConfiguration(name: "report", seeds: []), services: [HealthService.install])
    do {
      let connection = try await TCP.connect(unixDomainSocketPath: daemon.socket, connectTimeout: .seconds(2))
      try await connection.executeThenClose { input, output in
        let payload = try JSONEncoder().encode(ControlRequest.queryService(HealthService.key.name))
        try await output.write(Frame(target: 0x494E_4452, opcode: 1, payload: ByteBuffer(bytes: payload)))
        output.finish()
        var frames = input.makeAsyncIterator()
        let frame = try #require(try await frames.next())
        #expect(frame.target == 0x494E_4452)
        #expect(frame.opcode == 3)
        let reports = try JSONDecoder().decode(
          [ServiceReport<HealthMetrics>].self, from: Data(frame.payload.readableBytesView))
        let report = try #require(reports.first)
        #expect(reports.count == 1)
        #expect(report.node.id == daemon.identity.id)
        guard case .responded = report.outcome else { throw IndraError("Expected health metrics") }
      }
      try await daemon.stop()
    } catch {
      try? await daemon.stop()
      throw error
    }
  }

  @Test func installedSetuServicesDiscoverPeersAndTransferVerifiedFiles() async throws {
    let root = FileManager.default.temporaryDirectory.appendingPathComponent("indra-setu-\(UUID())")
    try FileManager.default.createDirectory(at: root, withIntermediateDirectories: false)
    defer { try? FileManager.default.removeItem(at: root) }
    let source = root.appendingPathComponent("fixture.bin")
    let bytes = Data(repeating: 0xA5, count: 5 * 1024 * 1024 + 13)
    try bytes.write(to: source)

    let first = try await TestDaemon.start(
      configuration: makeConfiguration(name: "sender", seeds: []),
      services: [SetuService.installer(directory: root.appendingPathComponent("sender").path)])
    do {
      let destination = root.appendingPathComponent("receiver")
      let second = try await TestDaemon.start(
        configuration: makeConfiguration(name: "receiver", seeds: [first.address]),
        services: [SetuService.installer(directory: destination.path)])
      do {
        let service = SetuService.key.name
        try await eventually {
          guard
            case .services(let services) = try await NodeControl.request(
              .services, socket: first.socket, timeout: .seconds(3))
          else { throw IndraError("Expected service listing") }
          return services.contains { $0.registration.key == service && $0.node.id == second.identity.id }
        }
        let peer = second.identity.name
        @Sendable func request(_ command: FileCommand) async throws -> FileReply {
          try await NodeControl.command(
            service, input: command, returning: FileReply.self, socket: first.socket, timeout: .seconds(3))
        }
        @Sendable func completed(_ command: FileCommand, expected: FileJob.State) async throws -> FileJob {
          guard case .job(let job) = try await request(command) else { throw IndraError("Expected a file job") }
          try await eventually {
            guard case .job(let current) = try await request(.job(job.id)) else {
              throw IndraError("Expected a file job")
            }
            return current.state != .running
          }
          guard case .job(let final) = try await request(.job(job.id)) else { throw IndraError("Missing file job") }
          try #require(final.state == expected, "\(final.error ?? "unexpected state")")
          return final
        }
        let sent = try await completed(.send(path: source.path, peer: peer), expected: .completed)
        let receipt = try #require(sent.receipt)
        #expect(sent.transferID == receipt.id)
        #expect(receipt.byteCount == bytes.count)
        guard case .receipt(let confirmed) = try await request(.receipt(id: receipt.id, peer: peer)) else {
          throw IndraError("Expected a receipt")
        }
        #expect(confirmed == receipt)
        #expect(try Data(contentsOf: destination.appendingPathComponent(source.lastPathComponent)) == bytes)
        guard case .files(let initial) = try await request(.list(peer: peer)) else {
          throw IndraError("Expected files")
        }
        #expect(initial.map(\.name) == [source.lastPathComponent])
        let names = (0..<1024).map { "entry-\($0)" }
        for name in names.reversed() {
          try Data().write(to: destination.appending(path: name, directoryHint: .notDirectory), options: [])
        }
        guard case .files(let files) = try await request(.list(peer: peer)) else { throw IndraError("Expected files") }
        #expect(files.map(\.name) == names.sorted() + [source.lastPathComponent])
        let output = root.appendingPathComponent("copy")
        let fetched = try await completed(
          .get(name: source.lastPathComponent, peer: peer, output: output.path), expected: .completed)
        #expect(fetched.transferID == fetched.receipt?.id)
        #expect(fetched.receipt?.sha256 == receipt.sha256)
        #expect(try Data(contentsOf: output) == bytes)
        try Data([1]).write(to: source)
        _ = try await completed(.send(path: source.path, peer: peer), expected: .failed)
        #expect(try Data(contentsOf: destination.appendingPathComponent(source.lastPathComponent)) == bytes)
        await #expect(throws: (any Error).self) { try await request(.get(name: "../copy", peer: peer, output: nil)) }
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
    let first = try await TestDaemon.start(configuration: makeConfiguration(name: "first", seeds: []), services: [])
    do {
      let second = try await TestDaemon.start(configuration: makeConfiguration(name: "second", seeds: []), services: [])
      do {
        var updated = try await NodeConfiguration.load(path: first.config)
        updated.name = "renamed"
        updated.seeds = [second.address]
        try await updated.save(path: first.config, overwrite: true)
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
          try await invalid.save(path: first.config, overwrite: true)
          await #expect(throws: IndraError.self) {
            try await NodeControl.request(.reload, socket: first.socket, timeout: .seconds(3))
          }
          #expect(try await first.status().configurationError?.contains("require a restart") == true)
        }
        #expect(try await first.status().node.endpoint == first.identity.endpoint)
        try await updated.save(path: first.config, overwrite: true)
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
    let first = try await TestDaemon.start(configuration: makeConfiguration(name: "first", seeds: []), services: [])
    do {
      let second = try await TestDaemon.start(
        configuration: makeConfiguration(name: "second", seeds: [first.address]), services: [])
      do {
        try await eventually { try await first.status().members.first?.state == .alive }
        let third = try await TestDaemon.start(configuration: makeConfiguration(name: "third", seeds: []), services: [])
        do {
          var configuration = try await NodeConfiguration.load(path: first.config)
          configuration.seeds = [third.address]
          try await configuration.save(path: first.config, overwrite: true)
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
    let daemon = try await TestDaemon.start(configuration: configuration, services: [])
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
      try await restored.save(path: daemon.config, overwrite: true)
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
    let first = try await TestDaemon.start(configuration: configuration, services: [])
    do {
      try await eventually { try await first.status().discoveryErrors[address.description] != nil }
      var peerConfiguration = makeConfiguration(name: "second", seeds: [])
      peerConfiguration.probeIntervalMilliseconds = 60_000
      let second = try await TestDaemon.start(configuration: peerConfiguration, services: [])
      do {
        configuration.seeds = [second.address]
        try await configuration.save(path: first.config, overwrite: true)
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
    let first = try await TestDaemon.start(configuration: makeConfiguration(name: "first", seeds: []), services: [])
    do {
      var foreignConfiguration = makeConfiguration(name: "foreign", seeds: [])
      foreignConfiguration.cluster = "elsewhere"
      let foreign = try await TestDaemon.start(configuration: foreignConfiguration, services: [])
      do {
        await #expect(throws: IndraError.self) {
          try await NodeControl.request(.join(foreign.address), socket: first.socket, timeout: .seconds(3))
        }
        #expect(try await NodeConfiguration.load(path: first.config).seeds == [foreign.address])
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

  @Test func concurrentJoinsPersistBothSeeds() async throws {
    let daemon = try await TestDaemon.start(configuration: makeConfiguration(name: "joining", seeds: []), services: [])
    do {
      let seeds = [PeerAddress(host: "127.0.0.1", port: 1), PeerAddress(host: "127.0.0.1", port: 2)]
      await withTaskGroup(of: Void.self) { tasks in
        for seed in seeds {
          tasks.addTask {
            _ = try? await NodeControl.request(.join(seed), socket: daemon.socket, timeout: .seconds(3))
          }
        }
      }
      #expect(Set(try await NodeConfiguration.load(path: daemon.config).seeds) == Set(seeds))
      #expect(Set(try await daemon.status().seeds) == Set(seeds))
      try await daemon.stop()
    } catch {
      try? await daemon.stop()
      throw error
    }
  }

  @Test func offlineJoinStaysSavedAndRetriesWhileConnected() async throws {
    let existing = try await TestDaemon.start(
      configuration: makeConfiguration(name: "existing", seeds: []), services: [])
    do {
      let first = try await TestDaemon.start(
        configuration: makeConfiguration(name: "first", seeds: [existing.address]), services: [])
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
        #expect(try await NodeConfiguration.load(path: first.config).seeds == [existing.address, address])
        #expect(try await first.status().discoveryErrors[address.description] != nil)
        var configuration = makeConfiguration(name: "returning", seeds: [])
        configuration.port = address.port
        let returning = try await TestDaemon.start(configuration: configuration, services: [])
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
    let daemon = try await TestDaemon.start(configuration: makeConfiguration(name: "first", seeds: []), services: [])
    do {
      await #expect(throws: IndraError.self) {
        try await runNode(configurationPath: daemon.config, socket: daemon.socket, services: [], onReady: { _ in })
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

  static func start(configuration: NodeConfiguration, services: [ServiceInstaller]) async throws -> Self {
    let root = URL(fileURLWithPath: "/tmp/indra-\(UUID().uuidString)")
    let config = root.appendingPathComponent("config.json").path
    let socket = root.appendingPathComponent("run/control.sock").path
    try await configuration.save(path: config, overwrite: false)
    let (events, ready) = AsyncThrowingStream<NodeStatus, any Error>.makeStream(bufferingPolicy: .bufferingNewest(1))
    let task = Task {
      do {
        try await runNode(configurationPath: config, socket: socket, services: services) {
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
