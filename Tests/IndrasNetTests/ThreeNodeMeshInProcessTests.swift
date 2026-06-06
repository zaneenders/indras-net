import NIOPosix
import Testing

@testable import IndrasNet

@Suite struct ThreeNodeMeshInProcessTests {
  @Test func threeNodesExchangeRequestVotes() async throws {
    try await TestHelpers.withEventLoopGroup { group in
      let host = "127.0.0.1"
      let port = 29_200
      let c = NodeAddress(host: host, port: port)
      let b = NodeAddress(host: host, port: port + 1)
      let a = NodeAddress(host: host, port: port + 2)
      let nodes = [a, b, c]

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

      let minimum = 1
      await TestHelpers.waitUntil(timeout: .seconds(30)) {
        TestHelpers.electionTrafficMet(recorder: recorder, nodes: nodes, minimum: minimum)
      }

      for local in nodes {
        for remote in nodes where remote.addressKey != local.addressKey {
          #expect(
            recorder.count(
              selfNode: local.addressKey, kind: "requestVote", direction: "out",
              peer: remote.addressKey
            ) >= minimum
          )
          #expect(
            recorder.count(
              selfNode: remote.addressKey, kind: "requestVote", direction: "in",
              peer: local.addressKey
            ) >= minimum
          )
        }
      }

      try await shellA.shutdown()
      try await shellB.shutdown()
      try await shellC.shutdown()
    }
  }
}
