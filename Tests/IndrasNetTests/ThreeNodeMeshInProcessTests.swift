import Foundation
import NIOPosix
import Testing
import TestUtils

@testable import IndrasNet

@Suite struct ThreeNodeMeshInProcessTests {
  @Test func threeNodesElectLeaderAndExchangeHeartbeats() async throws {
    try await TestHelpers.withEventLoopGroup { group in
      let mesh = try await InProcessMesh.start(
        basePort: 29_200,
        eventLoopGroup: group,
        recordActions: true
      )
      defer { try? await mesh.shutdown() }

      await TestHelpers.waitUntil(timeout: .seconds(5)) {
        TestHelpers.electionOccurred(recorder: mesh.recorder, minimumOutbound: 2)
      }

      await TestHelpers.waitUntil(timeout: .seconds(5)) {
        TestHelpers.leaderHeartbeatsStarted(recorder: mesh.recorder, minimumOutbound: 2)
      }

      #expect(mesh.recorder.totalCount(kind: "requestVote", direction: "out") >= 2)
      #expect(mesh.recorder.totalCount(kind: "appendEntries", direction: "out") >= 2)
    }
  }

  @Test func threeNodesReplicateClientCommand() async throws {
    try await TestHelpers.withEventLoopGroup { group in
      let mesh = try await InProcessMesh.start(
        basePort: 29_210,
        eventLoopGroup: group
      )
      defer { try? await mesh.shutdown() }

      let leader = try await mesh.waitForLeader()
      let command = Data("set z=3".utf8)

      let reply = await leader.submit(command: command)
      #expect(reply.status == .ok)
      #expect(reply.logIndex == 1)

      try await mesh.waitForReplicated(command: command, atIndex: 1)
    }
  }

  @Test func clientSubmitRedirectsFromFollowerToLeader() async throws {
    try await TestHelpers.withEventLoopGroup { group in
      let mesh = try await InProcessMesh.start(
        basePort: 29_220,
        eventLoopGroup: group
      )
      defer { try? await mesh.shutdown() }

      let leader = try await mesh.waitForLeader()
      let follower = try await mesh.waitForFollower()

      let command = Data("set x=1".utf8)
      let redirect = await follower.submit(command: command)
      let leaderID = await leader.instance.id

      #expect(redirect.status == .notLeader)
      #expect(redirect.leaderId == leaderID)

      let committed = await leader.submit(command: command)
      #expect(committed.status == .ok)
      #expect(committed.logIndex == 1)

      try await mesh.waitForReplicated(command: command, atIndex: 1)
    }
  }
}

private struct InProcessMesh {
  let shells: [Shell]
  let recorder: ShellActionRecorder

  static func start(
    basePort: Int,
    eventLoopGroup: MultiThreadedEventLoopGroup,
    recordActions: Bool = false
  ) async throws -> InProcessMesh {
    let host = "127.0.0.1"
    let nodes = (0..<3).map { offset in
      NodeAddress(host: host, port: basePort + offset)
    }

    let recorder = ShellActionRecorder()
    let shells = nodes.map { node in
      Shell(
        node,
        transport: TCPTransport(
          configuration: node.tcpConfiguration(),
          eventLoopGroup: eventLoopGroup,
          logger: TestHelpers.quietLogger
        ),
        logger: recordActions
          ? TestHelpers.shellLogger(node: node, recorder: recorder)
          : TestHelpers.quietLogger
      )
    }

    _ = try await shells[2].start(with: [nodes[1], nodes[0]])
    _ = try await shells[1].start(with: [nodes[2], nodes[0]])
    _ = try await shells[0].start(with: [nodes[1], nodes[2]])

    return InProcessMesh(shells: shells, recorder: recorder)
  }

  func waitForLeader(timeout: Duration = .seconds(5)) async throws -> Shell {
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

  func waitForFollower(timeout: Duration = .seconds(5)) async throws -> Shell {
    await TestHelpers.waitUntil(timeout: timeout) {
      for shell in shells where await shell.instance.role != .leader {
        return true
      }
      return false
    }

    for shell in shells where await shell.instance.role != .leader {
      return shell
    }

    Issue.record("Expected a follower")
    struct MissingFollower: Error {}
    throw MissingFollower()
  }

  func waitForReplicated(command: Data, atIndex index: LogIndex, timeout: Duration = .seconds(5))
    async throws
  {
    await TestHelpers.waitUntil(timeout: timeout) {
      for shell in shells {
        let nodeLog = await shell.instance.log
        guard nodeLog.count > Int(index), nodeLog[Int(index)].command == command else {
          return false
        }
      }
      return true
    }

    for shell in shells {
      let nodeLog = await shell.instance.log
      #expect(nodeLog[Int(index)].command == command)
    }
  }

  func shutdown() async throws {
    for shell in shells {
      try await shell.shutdown()
    }
  }
}
