import Foundation
import NIOCore
import Networking
import TCPActorSystem
import Testing
import _NIOFileSystem

@testable import IndraNode

@Suite(.timeLimit(.minutes(1)))
struct PeerHistoryTests {
  @Test func historyKeepsRecentAddressesAndIsScopedToTheNodeAndCluster() async throws {
    let files = FileSystem.shared
    let root = try await files.createTemporaryDirectory(template: files.temporaryDirectory.appending("XXXXXXXX"))
    defer { _ = try? await files.removeItem(at: root, strategy: .sequential, recursively: true) }
    let path = root.appending("history/peers.json").string
    let owner = UUID()
    var history = try await PeerHistory.load(path: path, nodeID: owner, cluster: "test")
    #expect(history.peers.isEmpty)
    for number in 0..<300 {
      let node = historyIdentity(id: UUID(), port: 10_000 + number)
      history.record(node, at: Date(timeIntervalSince1970: Double(number)))
    }
    #expect(history.peers.count == 256)
    #expect(history.addresses.first?.port == 10_299)
    #expect(history.addresses.last?.port == 10_044)
    history.record(historyIdentity(id: owner, port: 7331), at: Date(timeIntervalSince1970: 1000))
    #expect(!history.peers.contains { $0.id == owner })

    let peer = try #require(history.peers.first)
    history.record(historyIdentity(id: peer.id, port: 7332), at: Date(timeIntervalSince1970: 1000))
    #expect(history.addresses.first?.port == 7332)
    #expect(!history.addresses.contains(peer.address))
    let replacement = historyIdentity(id: UUID(), port: 7332)
    history.record(replacement, at: Date(timeIntervalSince1970: 1001))
    #expect(!history.peers.contains { $0.id == peer.id })
    #expect(history.peers.first?.id == replacement.id)

    try await history.save(path: path)
    let restored = try await PeerHistory.load(path: path, nodeID: owner, cluster: "test")
    #expect(restored.addresses == history.addresses)
    #expect(restored.peers.first?.lastSeen == history.peers.first?.lastSeen)
    #expect(try await PeerHistory.load(path: path, nodeID: UUID(), cluster: "test").peers.isEmpty)
    #expect(try await PeerHistory.load(path: path, nodeID: owner, cluster: "other").peers.isEmpty)
    let info = try await files.info(forFileAt: FilePath(path), infoAboutSymbolicLink: true)
    #expect(info?.permissions == .ownerReadWrite)
  }

  @Test func restartFindsLearnedPeerWithoutOriginalSeedAndOutagesPreserveHistory() async throws {
    let files = FileSystem.shared
    let root = try await files.createTemporaryDirectory(template: files.temporaryDirectory.appending("XXXXXXXX"))
    defer { _ = try? await files.removeItem(at: root, strategy: .sequential, recursively: true) }
    let path = root.appending("peers.json").string
    try await withPeerHistoryRuntime(configuration: makeConfiguration(name: "seed", seeds: []), historyPath: nil) {
      seed in
      let seedIdentity = await seed.status().node
      try await withPeerHistoryRuntime(
        configuration: makeConfiguration(name: "survivor", seeds: [seedIdentity.address]), historyPath: nil
      ) { survivor in
        let survivorIdentity = await survivor.status().node
        let configuration = makeConfiguration(name: "joining", seeds: [seedIdentity.address])
        try await withPeerHistoryRuntime(configuration: configuration, historyPath: path) { joining in
          try await historyEventually {
            await joining.status().members.filter { $0.state == .alive }.count == 2
          }
        }
        let saved = try await PeerHistory.load(path: path, nodeID: configuration.id, cluster: configuration.cluster)
        #expect(saved.peers.contains { $0.id == survivorIdentity.id })
        await seed.shutdown()

        var restarted = configuration
        restarted.seeds = []
        try await withPeerHistoryRuntime(configuration: restarted, historyPath: path) { joining in
          try await historyEventually {
            await joining.status().members.contains { $0.node.id == survivorIdentity.id && $0.state == .alive }
          }
          await survivor.shutdown()
          try await historyEventually {
            await joining.status().members.contains { $0.node.id == survivorIdentity.id && $0.state == .unreachable }
          }
        }
        let retained = try await PeerHistory.load(path: path, nodeID: configuration.id, cluster: configuration.cluster)
        #expect(retained.peers.contains { $0.id == survivorIdentity.id })
      }
    }
  }

  @Test(arguments: [false, true])
  func invalidOrUnavailableHistoryFallsBackToSeeds(corrupt: Bool) async throws {
    let files = FileSystem.shared
    let root = try await files.createTemporaryDirectory(template: files.temporaryDirectory.appending("XXXXXXXX"))
    defer { _ = try? await files.removeItem(at: root, strategy: .sequential, recursively: true) }
    let path = root.appending("peers.json").string
    try await withPeerHistoryRuntime(configuration: makeConfiguration(name: "seed", seeds: []), historyPath: nil) {
      seed in
      let seedIdentity = await seed.status().node
      let configuration = makeConfiguration(name: "joining", seeds: [seedIdentity.address])
      if corrupt {
        try await "invalid JSON".write(
          toFileAt: FilePath(path), absoluteOffset: 0, options: .newFile(replaceExisting: false, permissions: nil))
      } else {
        let reservation = try await TCP.listen(host: "127.0.0.1", port: 0)
        let port = try #require(reservation.channel.localAddress?.port)
        try await reservation.channel.close()
        var history = PeerHistory(nodeID: configuration.id, cluster: configuration.cluster)
        history.record(historyIdentity(id: UUID(), port: port), at: .now)
        try await history.save(path: path)
      }
      try await withPeerHistoryRuntime(configuration: configuration, historyPath: path) { joining in
        try await historyEventually {
          await joining.status().members.contains { $0.node.id == seedIdentity.id && $0.state == .alive }
        }
        #expect(await joining.status().members.count == 1)
        try await historyEventually { await joining.status().discoveryErrors["peer-history"] == nil }
      }
      let saved = try await PeerHistory.load(path: path, nodeID: configuration.id, cluster: configuration.cluster)
      #expect(saved.peers.contains { $0.id == seedIdentity.id })
    }
  }

  @Test func obsoleteSeedsAreNotRetriedWhileConnectedThroughHistory() async throws {
    let files = FileSystem.shared
    let root = try await files.createTemporaryDirectory(template: files.temporaryDirectory.appending("XXXXXXXX"))
    defer { _ = try? await files.removeItem(at: root, strategy: .sequential, recursively: true) }
    let path = root.appending("peers.json").string
    let reservation = try await TCP.listen(host: "127.0.0.1", port: 0)
    let obsolete = PeerAddress(host: "127.0.0.1", port: try #require(reservation.channel.localAddress?.port))
    try await reservation.channel.close()
    try await withPeerHistoryRuntime(configuration: makeConfiguration(name: "peer", seeds: []), historyPath: nil) {
      peer in
      let peerIdentity = await peer.status().node
      let configuration = makeConfiguration(name: "joining", seeds: [obsolete])
      var history = PeerHistory(nodeID: configuration.id, cluster: configuration.cluster)
      history.record(peerIdentity, at: .now)
      try await history.save(path: path)
      try await withPeerHistoryRuntime(configuration: configuration, historyPath: path) { joining in
        try await historyEventually {
          let status = await joining.status()
          return status.members.contains { $0.node.id == peerIdentity.id && $0.state == .alive }
            && status.discoveryErrors[obsolete.description] != nil
        }
        let before = await joining.monitor()
        try await historyEventually { await joining.monitor().protocolPeriod >= before.protocolPeriod + 3 }
        let after = await joining.monitor()
        #expect(!after.events.contains { $0.id > (before.events.last?.id ?? 0) && $0.peer == obsolete })
      }
    }
  }

  @Test func unavailableAddressesUseRetryBackoff() async throws {
    let reservation = try await TCP.listen(host: "127.0.0.1", port: 0)
    let address = PeerAddress(host: "127.0.0.1", port: try #require(reservation.channel.localAddress?.port))
    try await reservation.channel.close()
    try await withPeerHistoryRuntime(
      configuration: makeConfiguration(name: "joining", seeds: [address]), historyPath: nil
    ) { joining in
      try await historyEventually { await joining.status().discoveryErrors[address.description] != nil }
      let before = await joining.monitor()
      try await historyEventually { await joining.monitor().protocolPeriod >= before.protocolPeriod + 8 }
      let after = await joining.monitor()
      #expect(after.outbound.failures > before.outbound.failures)
      #expect(after.outbound.failures - before.outbound.failures < 5)
    }
  }
}

private func historyIdentity(id: UUID, port: Int) -> NodeIdentity {
  NodeIdentity(
    id: id, name: "peer", endpoint: ActorEndpoint(host: "127.0.0.1", port: port, systemID: UUID()), build: nil)
}

private func withPeerHistoryRuntime(
  configuration: NodeConfiguration, historyPath: String?, body: @Sendable (NodeRuntime) async throws -> Void
) async throws {
  let system = try await TCPActorSystem(
    host: configuration.listenHost, advertisedHost: configuration.advertisedHost, port: configuration.port,
    callTimeout: .seconds(2), connectTimeout: .seconds(1))
  let runtime = NodeRuntime(
    configuration: configuration, configurationPath: "/unused", peerHistoryPath: historyPath, system: system)
  do {
    try await runtime.start(services: [])
  } catch {
    await runtime.shutdown()
    throw error
  }
  #expect(await runtime.status().members.isEmpty)
  let membership = Task { try await runtime.runMembership() }
  do {
    try await body(runtime)
    membership.cancel()
    await runtime.shutdown()
    do { try await membership.value } catch is CancellationError {}
  } catch {
    membership.cancel()
    await runtime.shutdown()
    _ = try? await membership.value
    throw error
  }
}

private func historyEventually(_ condition: @Sendable () async throws -> Bool) async throws {
  let deadline = ContinuousClock.now.advanced(by: .seconds(15))
  while try await !condition() {
    guard ContinuousClock.now < deadline else { throw IndraError("Peer history condition did not become true") }
    try await Task.sleep(for: .milliseconds(25))
  }
}
