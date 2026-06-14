import Foundation
import Testing

@testable import IndrasNet

/// Async cluster harness: real `Shell` actors wired over a shared in-memory
/// `SimulatedTransport.Mesh`. Election-timeout *values* are deterministic via a
/// seeded RNG per node, and partitions are driven through the mesh — giving the
/// scenario surface of the synchronous `LogicalCluster` but against the real,
/// concurrent `Shell` runtime.
package struct SimulatedCluster: Sendable {
  package let shells: [SimulatedShell]
  package let addresses: [NodeAddress]
  package let mesh: SimulatedTransport.Mesh
  /// Per-node manual clocks, populated only when started with `manualClocks: true`.
  package let clocks: [TestClock]

  private init(
    shells: [SimulatedShell],
    addresses: [NodeAddress],
    mesh: SimulatedTransport.Mesh,
    clocks: [TestClock]
  ) {
    self.shells = shells
    self.addresses = addresses
    self.mesh = mesh
    self.clocks = clocks
  }

  /// Boots `nodeCount` nodes on a shared mesh and starts every node. When `seed`
  /// is set each node derives a deterministic RNG from it. When `manualClocks` is
  /// true each node gets its own ``TestClock`` so timer firing can be driven
  /// deterministically with ``advance(_:by:)``.
  package static func start(
    nodeCount: Int,
    seed: UInt64? = nil,
    timing: NodeTiming = .default,
    manualClocks: Bool = false,
    host: String = "sim",
    basePort: Int = 1
  ) async throws -> SimulatedCluster {
    let mesh = SimulatedTransport.Mesh()
    let addresses = (0..<nodeCount).map { NodeAddress(host: host, port: basePort + $0) }

    let nodeSeeds: [UInt64]
    if let seed {
      var clusterRNG = SeededRandomNumberGenerator(seed: seed)
      nodeSeeds = addresses.map { _ in clusterRNG.next() }
    } else {
      nodeSeeds = []
    }

    let clocks = manualClocks ? (0..<nodeCount).map { _ in TestClock() } : []

    let shells: [SimulatedShell] = addresses.enumerated().map { index, node in
      let rng: any RandomNumberGenerator & Sendable =
        index < nodeSeeds.count
        ? SeededRandomNumberGenerator(seed: nodeSeeds[index])
        : SystemRandomNumberGenerator()
      let timerSleep: @Sendable (Duration) async -> Void
      if manualClocks {
        let clock = clocks[index]
        timerSleep = { try? await clock.sleep(until: clock.now.advanced(by: $0)) }
      } else {
        timerSleep = { try? await Task.sleep(for: $0) }
      }
      return Shell(
        node,
        timing: timing,
        transport: SimulatedTransport(peer: node, mesh: mesh),
        rng: rng,
        timerSleep: timerSleep,
        logger: TestHelpers.quietLogger
      )
    }

    for index in shells.indices {
      let peers = addresses.enumerated().filter { $0.offset != index }.map(\.element)
      _ = try await shells[index].start(with: peers)
    }

    return SimulatedCluster(shells: shells, addresses: addresses, mesh: mesh, clocks: clocks)
  }

  /// Advances node `index`'s manual clock, firing any timer whose delay has elapsed.
  package func advance(_ index: Int, by duration: Duration) {
    clocks[index].advance(by: duration)
  }

  package func waitForLeader(timeout: Duration = .seconds(5)) async throws -> SimulatedShell {
    let shells = self.shells
    await TestHelpers.waitUntil(timeout: timeout) {
      for shell in shells where await shell.instance.role == .leader {
        return true
      }
      return false
    }

    for shell in shells where await shell.instance.role == .leader {
      return shell
    }

    Issue.record("Expected a leader")
    struct MissingLeader: Error {}
    throw MissingLeader()
  }

  package func leaderCount() async -> Int {
    var count = 0
    for shell in shells where await shell.instance.role == .leader {
      count += 1
    }
    return count
  }

  package func disconnect(_ peer: PeerId) async {
    await mesh.disconnect(peer)
  }

  package func reconnect(_ peer: PeerId) async {
    await mesh.reconnect(peer)
  }

  package func disconnect(from sender: PeerId, to recipient: PeerId) async {
    await mesh.disconnect(from: sender, to: recipient)
  }

  package func reconnect(from sender: PeerId, to recipient: PeerId) async {
    await mesh.reconnect(from: sender, to: recipient)
  }

  package func shutdown() async throws {
    for shell in shells {
      try await shell.shutdown()
    }
  }
}
