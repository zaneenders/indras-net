import NIOPosix
import Testing

@testable import IndrasNet

@Suite struct ThreeNodeMeshInProcessTests {
  @Test func threeNodesElectLeaderAndExchangeHeartbeats() async throws {
    try await TestHelpers.withEventLoopGroup { group in
      let host = "127.0.0.1"
      let port = 29_200
      let c = NodeAddress(host: host, port: port)
      let b = NodeAddress(host: host, port: port + 1)
      let a = NodeAddress(host: host, port: port + 2)

      let recorder = ShellActionRecorder()
      func makeShell(_ local: NodeAddress) -> Shell {
        Shell(
          local,
          transport: TCPTransport(
            configuration: local.tcpConfiguration(),
            eventLoopGroup: group,
            logger: TestHelpers.quietLogger
          ),
          logger: TestHelpers.shellLogger(node: local, recorder: recorder)
        )
      }

      let shellA = makeShell(a)
      let shellB = makeShell(b)
      let shellC = makeShell(c)

      _ = try await shellC.start(with: [b, a])
      _ = try await shellB.start(with: [c, a])
      _ = try await shellA.start(with: [b, c])

      await TestHelpers.waitUntil(timeout: .seconds(5)) {
        TestHelpers.electionOccurred(recorder: recorder, minimumOutbound: 2)
      }

      await TestHelpers.waitUntil(timeout: .seconds(5)) {
        TestHelpers.leaderHeartbeatsStarted(recorder: recorder, minimumOutbound: 2)
      }

      #expect(recorder.totalCount(kind: "requestVote", direction: "out") >= 2)
      #expect(recorder.totalCount(kind: "appendEntries", direction: "out") >= 2)

      try await shellA.shutdown()
      try await shellB.shutdown()
      try await shellC.shutdown()
    }
  }
}
