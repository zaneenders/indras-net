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
    let mesh = SimulatedTransport.Mesh()
    let nodes = (0..<3).map { NodeAddress(host: "sim", port: 100 + $0) }

    let shells: [SimulatedShell] = nodes.map { node in
      Shell(
        node,
        transport: SimulatedTransport(peer: node, mesh: mesh),
        logger: TestHelpers.quietLogger
      )
    }

    _ = try await shells[2].start(with: [nodes[1], nodes[0]])
    _ = try await shells[1].start(with: [nodes[2], nodes[0]])
    _ = try await shells[0].start(with: [nodes[1], nodes[2]])

    await TestHelpers.waitUntil(timeout: .seconds(5)) {
      for shell in shells where await shell.instance.role == .leader {
        return true
      }
      return false
    }

    let leaders = await shells.asyncFilter { await $0.instance.role == .leader }
    #expect(leaders.count == 1)

    for shell in shells {
      try await shell.shutdown()
    }
  }
}

extension Array {
  fileprivate func asyncFilter(_ predicate: (Element) async -> Bool) async -> [Element] {
    var result: [Element] = []
    for element in self {
      if await predicate(element) {
        result.append(element)
      }
    }
    return result
  }
}
