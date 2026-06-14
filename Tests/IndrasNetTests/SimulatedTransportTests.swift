import Foundation
import TestUtils
import Testing

@testable import IndrasNet

@Suite struct SimulatedTransportTests {
  private actor ReceivedMessages {
    private(set) var values: [(RaftMessage, PeerId)] = []

    func append(_ message: RaftMessage, from: PeerId) {
      values.append((message, from))
    }
  }

  @Test func deliversMessagesBetweenPeers() async throws {
    let mesh = SimulatedTransport.Mesh()
    let peerA = NodeAddress(host: "sim", port: 1)
    let peerB = NodeAddress(host: "sim", port: 2)

    let received = ReceivedMessages()
    let transportA = SimulatedTransport(peer: peerA, mesh: mesh)
    let transportB = SimulatedTransport(peer: peerB, mesh: mesh)

    try await transportA.start { _, _ in }
    try await transportB.start { message, from in
      await received.append(message, from: from)
    }

    let args = RequestVote.Args(term: 1, candidateId: peerA.addressKey, lastLogIndex: 0, lastLogTerm: 0)
    try await transportA.send(.requestVote(args), to: peerB.addressKey)

    let got = await received.values
    #expect(got.count == 1)
    #expect(got[0].1 == peerA.addressKey)
    if case .requestVote(let reply) = got[0].0 {
      #expect(reply == args)
    } else {
      Issue.record("Expected requestVote")
    }

    try await transportA.shutdown()
    try await transportB.shutdown()
  }

  @Test func partitionedLinkDropsDelivery() async throws {
    let mesh = SimulatedTransport.Mesh()
    let peerA = NodeAddress(host: "sim", port: 10)
    let peerB = NodeAddress(host: "sim", port: 11)

    let transportA = SimulatedTransport(peer: peerA, mesh: mesh)
    let transportB = SimulatedTransport(peer: peerB, mesh: mesh)

    try await transportA.start { _, _ in }
    try await transportB.start { _, _ in }

    await mesh.disconnect(from: peerA.addressKey, to: peerB.addressKey)

    do {
      try await transportA.send(
        .requestVote(.init(term: 1, candidateId: peerA.addressKey, lastLogIndex: 0, lastLogTerm: 0)),
        to: peerB.addressKey)
      Issue.record("Expected send to fail across disconnected link")
    } catch IndrasNetTransportError.peerNotConnected {
      // expected
    }

    try await transportA.shutdown()
    try await transportB.shutdown()
  }

  @Test func threeSimulatedShellsElectLeader() async throws {
    let cluster = try await SimulatedCluster.start(nodeCount: 3, seed: 1, basePort: 100)
    defer { try? await cluster.shutdown() }

    _ = try await cluster.waitForLeader()
    #expect(await cluster.leaderCount() == 1)
  }

  @Test func electionIsDeterministicUnderManualClock() async throws {
    let cluster = try await SimulatedCluster.start(
      nodeCount: 3, seed: 1, manualClocks: true, basePort: 200)
    defer { try? await cluster.shutdown() }

    // No timer has fired yet, so every node is still a follower.
    #expect(await cluster.leaderCount() == 0)

    // Advance only node 0 past any election timeout; with peers' timers still
    // parked, node 0 alone runs and wins the election — a deterministic leader.
    cluster.advance(0, by: .seconds(1))

    let leader = try await cluster.waitForLeader()
    #expect(await leader.instance.id == cluster.addresses[0].addressKey)
  }
}
