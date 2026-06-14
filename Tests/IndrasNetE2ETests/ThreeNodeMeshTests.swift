import Foundation
import TestUtils
import Testing

@testable import IndrasNet

@Suite(.timeLimit(.minutes(1))) struct ThreeNodeMeshTests {
  @Test func threeNodesBootAndElectLeader() async throws {
    let binary = try await E2ETestSupport.buildProduct(named: "indras-net")
    let root = try E2ETestSupport.packageRoot()
    let clusterPath = root.appending("Tests/e2e-cluster.json").string
    let cluster = try ClusterConfig.load(from: clusterPath)
    let peers = cluster.peers

    let logs = peers.map { _ in NodeLog() }

    func nodeArguments(peer: NodeAddress) -> [String] {
      [peer.host, String(peer.port), "--cluster", clusterPath, "--log-level", "info"]
    }

    try await withThrowingTaskGroup(of: Void.self) { group in
      for (peer, log) in zip(peers, logs) {
        group.addTask {
          try await E2ETestSupport.runNode(
            binary: binary,
            arguments: nodeArguments(peer: peer),
            log: log,
            workingDirectory: root,
            platformOptions: E2ETestSupport.processPlatformOptions()
          )
        }
      }

      for log in logs {
        try await E2ETestSupport.waitForRunning(log: log, timeout: .seconds(10))
        #expect(await log.hasRunning())
      }

      try await E2ETestSupport.waitForLeaderElected(logs: logs, timeout: .seconds(15))

      var leaderElectedCount = 0
      for log in logs where await log.hasLeaderElected() {
        leaderElectedCount += 1
      }
      #expect(leaderElectedCount == 1)

      group.cancelAll()
      while (try? await group.next()) != nil {}
    }
  }
}
