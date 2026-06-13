import Foundation
import NIOPosix
import TestUtils
import Testing

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
      let leaderID = await leader.instance.id
      let follower = try await mesh.waitForFollower(knownLeader: leaderID)

      let command = Data("set x=1".utf8)
      let redirect = await follower.submit(command: command)

      #expect(redirect.status == .notLeader)
      #expect(redirect.leaderId == leaderID)

      let committed = await leader.submit(command: command)
      #expect(committed.status == .ok)
      #expect(committed.logIndex == 1)

      try await mesh.waitForReplicated(command: command, atIndex: 1)
    }
  }
}
