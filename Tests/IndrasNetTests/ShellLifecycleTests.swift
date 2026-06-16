import Foundation
import TestUtils
import Testing

@testable import IndrasNet

/// Shell orchestration edge cases around shutdown and inflight RPC pairing.
@Suite struct ShellLifecycleTests {
  private actor SubmitOutcome {
    private(set) var reply: ClientSubmit.Reply?

    func complete(_ reply: ClientSubmit.Reply) {
      self.reply = reply
    }
  }

  /// Manual-clock clusters can start timer tasks slightly after `start()` returns.
  /// Advance node 0 until a leader appears so election setup is deterministic
  /// even when this suite runs in parallel with other tests.
  private func electLeader(
    in cluster: SimulatedCluster,
    timeout: Duration = .seconds(2)
  ) async throws -> SimulatedShell {
    let elected = await TestHelpers.poll(timeout: timeout) {
      cluster.advance(0, by: .milliseconds(100))
      try? await Task.sleep(for: .milliseconds(10))
      return await cluster.leaderCount() > 0
    }
    #expect(elected)
    return try await cluster.waitForLeader()
  }

  /// A partitioned leader accepts a write locally but cannot commit; stopping the
  /// shell must resume any suspended `submit(command:)` continuations.
  @Test func stopResumesPendingClientSubmit() async throws {
    let cluster = try await SimulatedCluster.start(nodeCount: 3, seed: 1, basePort: 410)

    let leader = try await cluster.waitForLeader()
    let leaderID = await leader.instance.id

    for shell in cluster.shells {
      let peer = await shell.instance.id
      guard peer != leaderID else { continue }
      await cluster.disconnect(from: leaderID, to: peer)
      await cluster.disconnect(from: peer, to: leaderID)
    }

    let command = Data("set pending=1".utf8)
    let outcome = SubmitOutcome()
    let submitTask = Task {
      await outcome.complete(await leader.submit(command: command))
    }

    let appended = await TestHelpers.poll(timeout: .seconds(2)) {
      let log = await leader.instance.log
      return log.count >= 2 && log[1].command == command
    }
    #expect(appended)

    let stillPending = await TestHelpers.poll(timeout: .milliseconds(300)) {
      await outcome.reply == nil
    }
    #expect(stillPending)

    await leader.stop()

    let completed = await TestHelpers.poll(timeout: .seconds(1)) {
      await outcome.reply != nil
    }
    #expect(completed)

    let reply = await outcome.reply
    #expect(reply?.status == .aborted)
    #expect(reply?.requestId != 0)

    submitTask.cancel()
    for shell in cluster.shells where await shell.instance.id != leaderID {
      try? await shell.shutdown()
    }
  }

  /// Rapid leader heartbeats while replies are still flowing can queue multiple
  /// outbound appendEntries RPCs per peer. A subsequent client write must still
  /// commit once a majority acknowledges replication.
  @Test func multipleHeartbeatsBeforeSubmitStillReplicate() async throws {
    let timing = NodeTiming(
      heartbeatIntervalMs: 10,
      electionTimeoutMinMs: 500,
      electionTimeoutMaxMs: 600
    )
    let cluster = try await SimulatedCluster.start(
      nodeCount: 5,
      seed: 1,
      timing: timing,
      manualClocks: true,
      basePort: 411
    )
    defer { try? await cluster.shutdown() }

    let leader = try await electLeader(in: cluster)
    let leaderID = await leader.instance.id
    var leaderIndex: Int?
    for (index, shell) in cluster.shells.enumerated() {
      if await shell.instance.id == leaderID {
        leaderIndex = index
        break
      }
    }
    guard let leaderIndex else {
      Issue.record("Missing leader")
      return
    }

    for _ in 0..<12 {
      cluster.advance(leaderIndex, by: timing.heartbeatInterval)
      try? await Task.sleep(for: .milliseconds(20))
    }

    let command = Data("set delayed=1".utf8)
    let reply = await leader.submit(command: command)
    #expect(reply.status == .ok)
    #expect(reply.logIndex == 1)

    try await cluster.waitForReplicated(command: command, atIndex: 1)
  }

  /// Failed appendEntries deliveries remove only the matching inflight RPC by
  /// delivery ID. Replication through the remaining majority must still succeed
  /// and the isolated peer should catch up after reconnect.
  @Test func appendEntriesDeliveryFailureDoesNotPreventLaterReplication() async throws {
    let timing = NodeTiming(
      heartbeatIntervalMs: 10,
      electionTimeoutMinMs: 500,
      electionTimeoutMaxMs: 600
    )
    let cluster = try await SimulatedCluster.start(
      nodeCount: 5,
      seed: 1,
      timing: timing,
      manualClocks: true,
      basePort: 412
    )
    defer { try? await cluster.shutdown() }

    let leader = try await electLeader(in: cluster)
    let leaderID = await leader.instance.id
    var leaderIndex: Int?
    for (index, shell) in cluster.shells.enumerated() {
      if await shell.instance.id == leaderID {
        leaderIndex = index
        break
      }
    }
    guard let leaderIndex else {
      Issue.record("Missing leader")
      return
    }

    let isolatedPeer = cluster.peer(at: 0)
    await cluster.disconnect(from: leaderID, to: isolatedPeer)
    await cluster.disconnect(from: isolatedPeer, to: leaderID)

    for _ in 0..<10 {
      cluster.advance(leaderIndex, by: timing.heartbeatInterval)
      try? await Task.sleep(for: .milliseconds(5))
    }

    let command = Data("set majority=1".utf8)
    let reply = await leader.submit(command: command)
    #expect(reply.status == .ok)
    #expect(reply.logIndex == 1)

    try await cluster.waitForReplicated(
      command: command,
      atIndex: 1,
      excluding: [isolatedPeer],
      timeout: .seconds(5)
    )

    await cluster.reconnect(from: leaderID, to: isolatedPeer)
    await cluster.reconnect(from: isolatedPeer, to: leaderID)
    try await cluster.waitForReplicated(command: command, atIndex: 1, timeout: .seconds(5))
  }

  @Test func clientSubmitDedupsRepeatedRequestId() async throws {
    let cluster = try await SimulatedCluster.start(nodeCount: 3, seed: 1, basePort: 420)
    defer { try? await cluster.shutdown() }

    let leader = try await cluster.waitForLeader()
    let client = RaftClient.defaultClientID
    let command = Data("set x=1".utf8)
    let requestId: UInt128 = 42
    let args = ClientSubmit.Args(requestId: requestId, command: command)

    await leader.receiveMessage(message: .clientSubmit(args), from: client)
    await leader.receiveMessage(message: .clientSubmit(args), from: client)

    #expect(await leader.instance.lastLogIndex == 1)
    #expect(await leader.instance.log.filter { $0.command == command }.count == 1)
  }

  @Test func clientSubmitDedupsCompletedRequestId() async throws {
    let cluster = try await SimulatedCluster.start(nodeCount: 3, seed: 1, basePort: 421)
    defer { try? await cluster.shutdown() }

    let leader = try await cluster.waitForLeader()
    let client = RaftClient.defaultClientID
    let command = Data("set x=1".utf8)
    let requestId: UInt128 = 42
    let args = ClientSubmit.Args(requestId: requestId, command: command)

    await leader.receiveMessage(message: .clientSubmit(args), from: client)
    try await cluster.waitForReplicated(command: command, atIndex: 1)

    await leader.receiveMessage(message: .clientSubmit(args), from: client)

    #expect(await leader.instance.lastLogIndex == 1)
    #expect(await leader.instance.log.filter { $0.command == command }.count == 1)
  }

  /// Shell must pair appendEntries replies with the RPC that actually sent them,
  /// not assume inflight queue order matches wire-send order. When a gated earlier
  /// heartbeat is still queued but replication goes out and is acked first, the
  /// leader should commit without waiting for the held send to complete.
  @Test func outOfOrderWireSendCommitsReplicationReply() async throws {
    let mesh = SimulatedTransport.Mesh()
    let leaderAddress = NodeAddress(host: "sim", port: 430)
    let followerAddress = NodeAddress(host: "sim", port: 431)
    let followerPeer = followerAddress.addressKey
    let timing = NodeTiming(
      heartbeatIntervalMs: 20,
      electionTimeoutMinMs: 200,
      electionTimeoutMaxMs: 300
    )

    let leaderClock = TestClock()
    let followerClock = TestClock()
    let leaderTransport = SimulatedTransport(peer: leaderAddress, mesh: mesh)
    let leader = Shell(
      leaderAddress,
      timing: timing,
      transport: leaderTransport,
      rng: SeededRandomNumberGenerator(seed: 1),
      timerSleep: { try? await leaderClock.sleep(until: leaderClock.now.advanced(by: $0)) },
      logger: TestHelpers.quietLogger
    )
    let follower = Shell(
      followerAddress,
      timing: timing,
      transport: SimulatedTransport(peer: followerAddress, mesh: mesh),
      rng: SeededRandomNumberGenerator(seed: 2),
      timerSleep: { try? await followerClock.sleep(until: followerClock.now.advanced(by: $0)) },
      logger: TestHelpers.quietLogger
    )
    defer {
      Task {
        await leaderTransport.releaseHeldSend(to: followerPeer)
        try? await leader.shutdown()
        try? await follower.shutdown()
      }
    }

    _ = try await follower.start(with: [leaderAddress])
    _ = try await leader.start(with: [followerAddress])

    let elected = await TestHelpers.poll(timeout: .seconds(2)) {
      leaderClock.advance(by: .milliseconds(100))
      try? await Task.sleep(for: .milliseconds(10))
      return await leader.instance.role == .leader
    }
    #expect(elected)

    await leaderTransport.holdNextAppendEntriesSend(to: followerPeer)

    // Queue a heartbeat RPC first; its wire send blocks on the gate.
    leaderClock.advance(by: timing.heartbeatInterval)
    let heartbeatHeld = await TestHelpers.poll(timeout: .seconds(1)) {
      await leaderTransport.hasHeldSend(to: followerPeer)
    }
    #expect(heartbeatHeld)

    let command = Data("set out-of-order=1".utf8)
    let submitTask = Task {
      await leader.submit(command: command)
    }

    let replicationOnWire = await TestHelpers.poll(timeout: .seconds(2)) {
      let order = await leaderTransport.appendEntriesWireOrder
      guard let last = order.last, last.count == 1 else { return false }
      return last[0].command == command
    }
    #expect(replicationOnWire)
    #expect(await leaderTransport.hasHeldSend(to: followerPeer))

    let followerReplicated = await TestHelpers.poll(timeout: .seconds(2)) {
      let log = await follower.instance.log
      return log.count >= 2 && log[1].command == command
    }
    #expect(followerReplicated)

    let leaderCommitted = await TestHelpers.poll(timeout: .seconds(1)) {
      await leader.instance.commitIndex >= 1
    }
    guard leaderCommitted else {
      submitTask.cancel()
      #expect(Bool(false), "Leader should commit from replication reply before held heartbeat sends")
      return
    }

    let reply = await submitTask.value
    #expect(reply.status == .ok)
    #expect(reply.logIndex == 1)
  }
}
