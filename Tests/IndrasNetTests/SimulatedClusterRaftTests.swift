import Foundation
import TestUtils
import Testing

@testable import IndrasNet

/// Raft safety and liveness scenarios from the [Raft paper](https://raft.github.io/raft.pdf),
/// exercised against real concurrent `Shell` actors on a `SimulatedTransport` mesh.
@Suite struct SimulatedClusterRaftTests {
  private let command = Data("set z=3".utf8)

  // MARK: - Leader election (§5.2)

  @Test func electsExactlyOneLeader() async throws {
    let cluster = try await SimulatedCluster.start(nodeCount: 3, seed: 1, basePort: 300)
    defer { try? await cluster.shutdown() }

    _ = try await cluster.waitForLeader()
    #expect(await cluster.leaderCount() == 1)
  }

  // MARK: - Log replication (§5.3)

  @Test func replicatesClientCommandToAllNodes() async throws {
    let cluster = try await SimulatedCluster.start(nodeCount: 3, seed: 1, basePort: 310)
    defer { try? await cluster.shutdown() }

    let leader = try await cluster.waitForLeader()
    let reply = await leader.submit(command: command)
    #expect(reply.status == .ok)
    #expect(reply.logIndex == 1)

    try await cluster.waitForReplicated(command: command, atIndex: 1)
  }

  @Test func replicatesMultipleEntriesInOrder() async throws {
    let cluster = try await SimulatedCluster.start(nodeCount: 3, seed: 1, basePort: 311)
    defer { try? await cluster.shutdown() }

    let leader = try await cluster.waitForLeader()
    let first = Data("set a=1".utf8)
    let second = Data("set b=2".utf8)

    let firstReply = await leader.submit(command: first)
    #expect(firstReply.status == .ok)
    #expect(firstReply.logIndex == 1)
    try await cluster.waitForReplicated(command: first, atIndex: 1)

    let secondReply = await leader.submit(command: second)
    #expect(secondReply.status == .ok)
    #expect(secondReply.logIndex == 2)
    try await cluster.waitForReplicated(command: second, atIndex: 2)
  }

  @Test func followerRedirectsClientToLeader() async throws {
    let cluster = try await SimulatedCluster.start(nodeCount: 3, seed: 1, basePort: 312)
    defer { try? await cluster.shutdown() }

    let leader = try await cluster.waitForLeader()
    let leaderID = await leader.instance.id
    let follower = try await cluster.waitForFollower(knownLeader: leaderID)

    let redirect = await follower.submit(command: command)
    #expect(redirect.status == .notLeader)
    #expect(redirect.leaderId == leaderID)

    let committed = await leader.submit(command: command)
    #expect(committed.status == .ok)
    #expect(committed.logIndex == 1)

    try await cluster.waitForReplicated(command: command, atIndex: 1)
  }

  @Test func committedEntrySurvivesLeaderLoss() async throws {
    let cluster = try await SimulatedCluster.start(nodeCount: 3, seed: 1, basePort: 313)
    defer { try? await cluster.shutdown() }

    let leader = try await cluster.waitForLeader()
    let leaderID = await leader.instance.id
    let reply = await leader.submit(command: command)
    #expect(reply.status == .ok)
    try await cluster.waitForReplicated(command: command, atIndex: 1)

    await cluster.disconnect(leaderID)
    _ = try await cluster.waitForLeader(timeout: .seconds(10))
    try await cluster.waitForReplicated(command: command, atIndex: 1)
  }

  // MARK: - Partitions and catch-up (§5.4–5.6)

  @Test func minorityPartitionCannotElectLeader() async throws {
    let cluster = try await SimulatedCluster.start(
      nodeCount: 3, seed: 1, manualClocks: true, basePort: 320)
    defer { try? await cluster.shutdown() }

    await cluster.disconnect(cluster.peer(at: 0))
    await cluster.disconnect(cluster.peer(at: 1))

    cluster.advanceAll(by: .seconds(1))
    try? await Task.sleep(for: .milliseconds(50))

    #expect(await cluster.leaderCount() == 0)
  }

  @Test func reconnectingMinorityElectsStableLeader() async throws {
    let cluster = try await SimulatedCluster.start(
      nodeCount: 3, seed: 1, manualClocks: true, basePort: 321)
    defer { try? await cluster.shutdown() }

    await cluster.disconnect(cluster.peer(at: 0))
    await cluster.disconnect(cluster.peer(at: 1))

    cluster.advanceAll(by: .seconds(1))
    try? await Task.sleep(for: .milliseconds(50))
    #expect(await cluster.leaderCount() == 0)

    await cluster.reconnect(cluster.peer(at: 0))
    // Advance only node 0 so it alone times out and wins votes from node 2.
    cluster.advance(0, by: .seconds(1))
    try? await Task.sleep(for: .milliseconds(50))

    let leader = try await cluster.waitForLeader()
    let leaderID = await leader.instance.id

    await cluster.reconnect(cluster.peer(at: 1))
    cluster.advance(1, by: .milliseconds(100))
    try? await Task.sleep(for: .milliseconds(50))

    #expect(await cluster.leaderCount() == 1)
    for shell in cluster.shells where await shell.instance.role == .leader {
      #expect(await shell.instance.id == leaderID)
    }
  }

  @Test func isolatedNodeCatchesUpAfterReconnect() async throws {
    let cluster = try await SimulatedCluster.start(nodeCount: 3, seed: 1, basePort: 330)
    defer { try? await cluster.shutdown() }

    await cluster.disconnect(cluster.peer(at: 0))
    let leader = try await cluster.waitForLeader()

    let reply = await leader.submit(command: command)
    #expect(reply.status == .ok)

    let isolatedLog = await cluster.shells[0].instance.log
    let majorityLog = await cluster.shells[1].instance.log
    #expect(isolatedLog.count == 1)
    #expect(majorityLog.count == 2)

    await cluster.reconnect(cluster.peer(at: 0))
    try await cluster.waitForReplicated(command: command, atIndex: 1, timeout: .seconds(10))
  }

  @Test func minorityPartitionDoesNotReceiveCommittedEntries() async throws {
    let cluster = try await SimulatedCluster.start(nodeCount: 5, seed: 1, basePort: 331)
    defer { try? await cluster.shutdown() }

    await cluster.disconnect(cluster.peer(at: 0))
    await cluster.disconnect(cluster.peer(at: 1))
    let leader = try await cluster.waitForLeader()

    let reply = await leader.submit(command: command)
    #expect(reply.status == .ok)

    let minorityLog = await cluster.shells[0].instance.log
    let majorityLog = await cluster.shells[2].instance.log
    #expect(minorityLog.count == 1)
    #expect(majorityLog.count == 2)

    await cluster.reconnect(cluster.peer(at: 0))
    await cluster.reconnect(cluster.peer(at: 1))
    try await cluster.waitForReplicated(command: command, atIndex: 1, timeout: .seconds(10))
  }

  // MARK: - Figure 8 (§5.4.2)

  /// Deterministic version of Raft Figure 8: an isolated former leader appends an
  /// uncommitted entry at index 2, a new leader in a higher term commits a different
  /// entry at the same index, and the stale entry is overwritten after reconnect.
  @Test func figure8OverwritesUncommittedStaleEntry() async throws {
    let cluster = try await SimulatedCluster.start(nodeCount: 5, seed: 1, basePort: 350)
    defer { try? await cluster.shutdown() }

    let committed = Data("set base=1".utf8)
    let stale = Data("set stale=1".utf8)
    let winner = Data("set winner=1".utf8)

    let leader = try await cluster.waitForLeader()
    let staleLeaderID = await leader.instance.id

    let baseReply = await leader.submit(command: committed)
    #expect(baseReply.status == .ok)
    try await cluster.waitForReplicated(command: committed, atIndex: 1)

    await cluster.disconnect(staleLeaderID)

    Task { await leader.submit(command: stale) }
    await TestHelpers.poll(timeout: .milliseconds(500)) {
      let log = await leader.instance.log
      return log.count > Int(2) && log[Int(2)].command == stale
    }

    let newLeader = try await cluster.waitForLeader(
      otherThan: staleLeaderID, timeout: .seconds(10))
    let winnerReply = await newLeader.submit(command: winner)
    #expect(winnerReply.status == .ok)
    #expect(winnerReply.logIndex == 2)

    try await cluster.waitForReplicated(
      command: winner, atIndex: 2, excluding: [staleLeaderID], timeout: .seconds(10))

    await cluster.reconnect(staleLeaderID)
    try await cluster.waitForReplicated(command: winner, atIndex: 2, timeout: .seconds(10))

    let logs = await cluster.shellLogs()
    for log in logs {
      #expect(log[Int(2)].command == winner)
      #expect(!log.map(\.command).contains(stale))
    }
    #expect(await cluster.leaderCount() == 1)
  }

  // MARK: - Safety invariants

  @Test func allNodesShareCommittedLogPrefix() async throws {
    let cluster = try await SimulatedCluster.start(nodeCount: 3, seed: 1, basePort: 340)
    defer { try? await cluster.shutdown() }

    let leader = try await cluster.waitForLeader()
    let entries = [
      Data("set x=1".utf8),
      Data("set y=2".utf8),
      Data("set z=3".utf8),
    ]

    for (offset, entry) in entries.enumerated() {
      let index = LogIndex(offset + 1)
      let reply = await leader.submit(command: entry)
      #expect(reply.status == .ok)
      #expect(reply.logIndex == index)
      try await cluster.waitForReplicated(command: entry, atIndex: index)
    }

    let logs = await cluster.shellLogs()
    for index in 1...entries.count {
      let commands = Set(logs.map { $0[index].command })
      #expect(commands.count == 1)
      #expect(commands.first == entries[index - 1])
    }
  }

  @Test func disconnectTwoNodesNoLeaderReconnectOneNode() async throws {
    let cluster = try await SimulatedCluster.start(
      nodeCount: 3, seed: 1, manualClocks: true, basePort: 400)
    defer { try? await cluster.shutdown() }

    await cluster.disconnect(cluster.peer(at: 0))
    await cluster.disconnect(cluster.peer(at: 1))

    cluster.advanceAll(by: .seconds(1))
    try? await Task.sleep(for: .milliseconds(50))
    #expect(await cluster.leaderCount() == 0)

    await cluster.reconnect(cluster.peer(at: 0))
    cluster.advance(0, by: .seconds(1))
    try? await Task.sleep(for: .milliseconds(50))

    let leader = try await cluster.waitForLeader()
    let leaderID = await leader.instance.id

    await cluster.reconnect(cluster.peer(at: 1))
    cluster.advance(1, by: .milliseconds(100))
    try? await Task.sleep(for: .milliseconds(50))

    #expect(await cluster.leaderCount() == 1)
    for shell in cluster.shells where await shell.instance.role == .leader {
      #expect(await shell.instance.id == leaderID)
    }
  }

  @Test func disconnectNodeSubmitEntryThenReconnectNode() async throws {
    let cluster = try await SimulatedCluster.start(nodeCount: 3, seed: 1, basePort: 401)
    defer { try? await cluster.shutdown() }

    await cluster.disconnect(cluster.peer(at: 0))
    let leader = try await cluster.waitForLeader(timeout: .seconds(10))
    let reply = await leader.submit(command: command)
    #expect(reply.status == .ok)

    #expect(await cluster.shells[0].instance.log.count == 1)
    #expect(await cluster.shells[1].instance.log.count == 2)
    #expect(await cluster.shells[2].instance.log.count == 2)

    await cluster.reconnect(cluster.peer(at: 0))
    try await cluster.waitForReplicated(command: command, atIndex: 1, timeout: .seconds(10))
  }

  @Test func disconnectTwoNodesSubmitEntryThenReconnectNode() async throws {
    let cluster = try await SimulatedCluster.start(nodeCount: 5, seed: 1, basePort: 402)
    defer { try? await cluster.shutdown() }

    await cluster.disconnect(cluster.peer(at: 0))
    await cluster.disconnect(cluster.peer(at: 1))
    let leader = try await cluster.waitForLeader(timeout: .seconds(10))
    _ = await leader.submit(command: command)

    #expect(await cluster.shells[0].instance.log.count == 1)
    #expect(await cluster.shells[1].instance.log.count == 1)
    #expect(await cluster.shells[2].instance.log.count == 2)
    #expect(await cluster.shells[3].instance.log.count == 2)
    #expect(await cluster.shells[4].instance.log.count == 2)

    await cluster.reconnect(cluster.peer(at: 0))
    await cluster.reconnect(cluster.peer(at: 1))
    try await cluster.waitForReplicated(command: command, atIndex: 1, timeout: .seconds(10))
  }
}
