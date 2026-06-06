import Testing

@testable import IndrasNet

@Suite(.timeLimit(.minutes(1))) struct ThreeNodeMeshTests {
  @Test func threeNodesRequestVoteWithSharedCluster() async throws {
    let binary = try await E2ETestSupport.buildProduct(named: "indras-net")
    let root = try E2ETestSupport.packageRoot()
    let clusterPath = root.appending("cluster.json").string
    let host = "127.0.0.1"
    let ports = [9001, 9002, 9003]
    let nodeKeys = ports.map { NodeAddress(host: host, port: $0).addressKey }

    let logs = [NodeLog(), NodeLog(), NodeLog()]

    let minimumRequestVoteCount = 2

    func nodeArguments(port: Int) -> [String] {
      [host, String(port), "--cluster", clusterPath]
    }

    var baselines: [Int] = []
    var countsDuringWindow: [(String, MeshEventCounts)] = []

    try await withThrowingTaskGroup(of: Void.self) { group in
      for (port, log) in zip(ports, logs) {
        group.addTask {
          try await E2ETestSupport.runNode(
            binary: binary,
            arguments: nodeArguments(port: port),
            log: log,
            workingDirectory: root,
            platformOptions: E2ETestSupport.processPlatformOptions()
          )
        }
      }

      for (log, key) in zip(logs, nodeKeys) {
        try await E2ETestSupport.waitForRunning(log: log, timeout: .seconds(10))
        try await E2ETestSupport.waitForMinRequestVoteReceived(
          log: log,
          node: key,
          minimum: 1,
          timeout: .seconds(15)
        )
      }

      baselines = await logs.asyncMap { await $0.lineCount() }

      try await E2ETestSupport.waitForAllMinRequestVoteSent(
        logs: logs,
        nodes: nodeKeys,
        baselines: baselines,
        minimum: minimumRequestVoteCount,
        timeout: .seconds(30)
      )

      try await E2ETestSupport.waitForAllMinRequestVoteReceived(
        logs: logs,
        nodes: nodeKeys,
        baselines: baselines,
        minimum: minimumRequestVoteCount,
        timeout: .seconds(30)
      )

      group.cancelAll()
      while (try? await group.next()) != nil {}

      for index in logs.indices {
        let counts = await logs[index].meshEventCounts(
          node: nodeKeys[index],
          since: baselines[index]
        )
        countsDuringWindow.append((nodeKeys[index], counts))
      }
    }

    #expect(countsDuringWindow.count == 3)
    for (key, counts) in countsDuringWindow {
      #expect(
        counts.requestVoteSent >= minimumRequestVoteCount
          && counts.requestVoteReceived >= minimumRequestVoteCount,
        "unexpected counts for \(key) during measurement window: \(counts)"
      )
    }

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
