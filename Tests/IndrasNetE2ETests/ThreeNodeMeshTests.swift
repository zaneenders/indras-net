import Testing

@testable import IndrasNet

@Suite(.timeLimit(.minutes(1))) struct ThreeNodeMeshTests {
  @Test func threeNodesPingPongWithSharedCluster() async throws {
    let binary = try await E2ETestSupport.buildProduct(named: "indras-net")
    let root = try E2ETestSupport.packageRoot()
    let clusterPath = root.appending("cluster.json").string
    let host = "127.0.0.1"
    let ports = [9001, 9002, 9003]
    let nodeKeys = ports.map { ClusterEndpoint.addressKey(host: host, port: $0) }

    let logs = [IndrasNetEventLog(), IndrasNetEventLog(), IndrasNetEventLog()]

    let minimumPingCount = 6

    func nodeArguments(port: Int) -> [String] {
      [
        host, String(port),
        "--cluster", clusterPath,
        "--json-event-log",
      ]
    }

    var baselines: [Int] = []
    var countsDuringWindow: [(String, MeshEventCounts)] = []

    try await withThrowingTaskGroup(of: Void.self) { group in
      for (port, log) in zip(ports, logs) {
        group.addTask {
          try await E2ETestSupport.runNode(
            binary: binary,
            arguments: nodeArguments(port: port),
            eventLog: log,
            workingDirectory: root,
            platformOptions: E2ETestSupport.processPlatformOptions()
          )
        }
      }

      for (log, key) in zip(logs, nodeKeys) {
        try await E2ETestSupport.waitForRunning(eventLog: log, timeout: .seconds(10))
        try await E2ETestSupport.waitForMinPingReceived(
          eventLog: log,
          node: key,
          minimum: 2,
          timeout: .seconds(15)
        )
      }

      baselines = await logs.asyncMap { await $0.eventCount() }

      try await E2ETestSupport.waitForAllMinPingSent(
        eventLogs: logs,
        nodes: nodeKeys,
        baselines: baselines,
        minimum: minimumPingCount,
        timeout: .seconds(30)
      )

      try await E2ETestSupport.waitForAllMinPingReceived(
        eventLogs: logs,
        nodes: nodeKeys,
        baselines: baselines,
        minimum: minimumPingCount,
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
        counts.pingSent >= minimumPingCount && counts.pingReceived >= minimumPingCount,
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
