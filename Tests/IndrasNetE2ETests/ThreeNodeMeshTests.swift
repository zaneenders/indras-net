import Testing
import TestUtils

@testable import IndrasNet

@Suite(.timeLimit(.minutes(1))) struct ThreeNodeMeshTests {
  @Test func threeNodesRequestVoteWithSharedCluster() async throws {
    let binary = try await E2ETestSupport.buildProduct(named: "indras-net")
    let root = try E2ETestSupport.packageRoot()
    let clusterPath = root.appending("Tests/e2e-cluster.json").string
    let cluster = try ClusterConfig.load(from: clusterPath)
    let peers = cluster.peers
    let nodeKeys = peers.map(\.addressKey)

    let logs = peers.map { _ in NodeLog() }

    let minimumRequestVotesReceivedClusterWide = 2
    let minimumLeaderHeartbeats = 2

    func nodeArguments(peer: NodeAddress) -> [String] {
      [peer.host, String(peer.port), "--cluster", clusterPath, "--log-level", "trace"]
    }

    var countsDuringWindow: [(String, MeshEventCounts)] = []

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
      }

      try await E2ETestSupport.waitForMinClusterRequestVoteReceived(
        logs: logs,
        nodes: nodeKeys,
        minimum: minimumRequestVotesReceivedClusterWide,
        timeout: .seconds(15)
      )

      try await E2ETestSupport.waitForMinAppendEntriesSent(
        logs: logs,
        nodes: nodeKeys,
        baselines: Array(repeating: 0, count: logs.count),
        minimum: minimumLeaderHeartbeats,
        timeout: .seconds(30)
      )

      group.cancelAll()
      while (try? await group.next()) != nil {}

      for index in logs.indices {
        let counts = await logs[index].meshEventCounts(node: nodeKeys[index])
        countsDuringWindow.append((nodeKeys[index], counts))
      }
    }

    #expect(countsDuringWindow.count == peers.count)
    let totalAppendEntriesSent = countsDuringWindow.reduce(0) { $0 + $1.1.appendEntriesSent }
    let totalRequestVotesReceived = countsDuringWindow.reduce(0) { $0 + $1.1.requestVoteReceived }
    #expect(totalAppendEntriesSent >= minimumLeaderHeartbeats)
    #expect(totalRequestVotesReceived >= minimumRequestVotesReceivedClusterWide)

  }
}

extension Array {
  fileprivate func asyncMap<T>(_ transform: (Element) async -> T) async -> [T] {
    var results: [T] = []
    results.reserveCapacity(count)
    for element in self {
      results.append(await transform(element))
    }
    return results
  }
}
