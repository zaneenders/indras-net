import Foundation
import RaftCore
import Testing

struct Figure8Tests {
  private static let first = ClientSubmit.Args(requestId: 1, command: Data([1]), traceHeaders: [:])
  private static let competing = ClientSubmit.Args(requestId: 3, command: Data([3]), traceHeaders: [:])

  @Test func figure8dALaterLeaderOverwritesAnUncommittedEntryStoredOnAMajority() throws {
    var cluster = try figure8c()
    try crashS1AndCampaignWithS5(&cluster)
    try #require(cluster.nodes["s5"]?.role == .leader)
    try #require(cluster.nodes["s5"]?.currentTerm == 5)
    try cluster.restart("s1")
    try cluster.tick("s5")
    try cluster.deliverPending()

    for peer in cluster.peers {
      let node = try #require(cluster.nodes[peer])
      #expect(node.entry(at: 2)?.term == 3)
      #expect(node.entry(at: 2)?.command == Self.competing.command)
      #expect(node.commitIndex == 3)
      #expect(
        cluster.applied[peer]?.filter { !$0.isNoOp }.map(\.command) == [
          Self.first.command, Self.competing.command
        ])
    }
  }

  @Test func figure8eCommittingATermFourEntryPreservesItsPrefixAcrossFailover() throws {
    var cluster = try figure8c()
    let oldEntry = try #require(cluster.nodes["s1"]?.entry(at: 2))
    let barrier = try #require(cluster.nodes["s1"]?.entry(at: 3))
    try exchangeNext(&cluster, from: "s1", to: "s2")
    #expect(cluster.nodes["s1"]?.commitIndex == 1)
    try exchangeNext(&cluster, from: "s1", to: "s3")
    try #require(cluster.nodes["s1"]?.commitIndex == 3)
    #expect(cluster.applied["s1"]?.dropFirst().elementsEqual([oldEntry, barrier]) == true)
    for peer in ["s2", "s3"] {
      #expect(cluster.nodes[peer]?.entry(at: 3) == barrier)
      #expect(cluster.nodes[peer]?.commitIndex == 1)
    }

    try crashS1AndCampaignWithS5(&cluster)
    try #require(cluster.nodes["s5"]?.role == .candidate)
    #expect(cluster.nodes["s5"]?.currentTerm == 5)
    for peer in ["s2", "s3"] {
      #expect(cluster.nodes[peer]?.votedFor == nil)
      #expect(cluster.nodes[peer]?.commitIndex == 1)
    }
    #expect(cluster.nodes["s4"]?.votedFor == "s5")

    try cluster.tick("s2")
    try cluster.deliverPending()
    try #require(cluster.nodes["s2"]?.role == .leader)
    #expect(cluster.nodes["s2"]?.currentTerm == 6)
    try cluster.restart("s1")
    try cluster.tick("s2")
    try cluster.deliverPending()
    for peer in cluster.peers {
      let node = try #require(cluster.nodes[peer])
      #expect(node.entry(at: 2) == oldEntry)
      #expect(node.entry(at: 3) == barrier)
      #expect(node.commitIndex == 4)
      #expect(
        cluster.applied[peer]?.filter { !$0.isNoOp }.map(\.command) == [
          Self.first.command, oldEntry.command
        ])
    }
  }

  private func figure8c() throws -> RaftSimulation {
    var cluster = try RaftSimulation.empty(voters: ["s1", "s2", "s3", "s4", "s5"], seed: 8)
    try cluster.tick("s1")
    try cluster.deliverPending()
    try cluster.submit(Self.first, to: "s1")
    try cluster.deliverPending()

    cluster.crash("s1")
    try cluster.restart("s1")
    cluster.elapse(.milliseconds(150))
    try cluster.tick("s1")
    try cluster.deliverPending()
    try #require(cluster.nodes["s1"]?.role == .leader)
    #expect(cluster.nodes["s1"]?.currentTerm == 2)

    // A full replication batch lets S3 receive index 2 separately from the term-four barrier.
    let request = RequestIdentity(client: "client", requestId: 2)
    let empty = LogEntry(term: 2, command: Data(), request: request, isNoOp: false)
    let available =
      Int(RaftLimits.maximumPayloadLength) - AppendEntries.Args.basePayloadLength(leaderId: "s1")
      - empty.encodedLength
    let original = ClientSubmit.Args(requestId: 2, command: Data(repeating: 2, count: available), traceHeaders: [:])
    cluster.partition = ["s1", "s2"]
    try cluster.submit(original, to: "s1")
    try cluster.deliverPending()
    let termTwoEntry = try #require(cluster.nodes["s1"]?.entry(at: 2))
    try #require(cluster.nodes["s2"]?.entry(at: 2) == termTwoEntry)
    #expect(cluster.nodes["s1"]?.commitIndex == 1)

    cluster.crash("s1")
    cluster.partition = ["s3", "s4", "s5"]
    cluster.elapse(.milliseconds(150))
    try cluster.tick("s5")
    try cluster.deliverPending()
    try #require(cluster.nodes["s5"]?.role == .leader)
    #expect(cluster.nodes["s5"]?.currentTerm == 3)
    cluster.partition = ["s5"]
    try cluster.submit(Self.competing, to: "s5")
    try cluster.deliverPending()
    #expect(cluster.nodes["s5"]?.entry(at: 2)?.term == 3)
    #expect(cluster.nodes["s5"]?.commitIndex == 1)

    cluster.crash("s5")
    try cluster.restart("s1")
    cluster.partition = []
    cluster.elapse(.milliseconds(150))
    try cluster.tick("s1")
    try cluster.deliverPending()
    try #require(cluster.nodes["s1"]?.role == .candidate)
    try cluster.tick("s1")
    try exchangeNext(&cluster, from: "s1", to: "s2")
    try exchangeNext(&cluster, from: "s1", to: "s3")
    try #require(cluster.nodes["s1"]?.role == .leader)
    try #require(cluster.nodes["s1"]?.currentTerm == 4)
    try #require(cluster.nodes["s1"]?.entry(at: 3)?.term == 4)
    #expect(cluster.nodes["s1"]?.entry(at: 3)?.isNoOp == true)

    try exchangeNext(&cluster, from: "s1", to: "s3")
    #expect(cluster.nodes["s3"]?.lastLogIndex == 1)
    try exchangeNext(&cluster, from: "s1", to: "s3")
    // S2 already stores index 2. S3 and S4 acknowledge it in term four without receiving the barrier.
    try exchangeNext(&cluster, from: "s1", to: "s4")  // Vote request and reply.
    try exchangeNext(&cluster, from: "s1", to: "s4")  // Reject the probe at index 2.
    try exchangeNext(&cluster, from: "s1", to: "s4")  // Accept the batch ending at index 2.
    for peer in ["s1", "s2", "s3", "s4"] {
      try #require(cluster.nodes[peer]?.entry(at: 2) == termTwoEntry)
      try #require(cluster.nodes[peer]?.commitIndex == 1)
      #expect(cluster.applied[peer]?.count == 1)
    }
    #expect(cluster.nodes["s2"]?.lastLogIndex == 2)
    #expect(cluster.nodes["s3"]?.lastLogIndex == 2)
    #expect(cluster.nodes["s4"]?.lastLogIndex == 2)
    return cluster
  }

  private func crashS1AndCampaignWithS5(_ cluster: inout RaftSimulation) throws {
    cluster.crash("s1")
    cluster.discardPending()
    try cluster.restart("s5")
    cluster.elapse(.milliseconds(150))
    try cluster.tick("s5")
    try cluster.deliverPending()
    try #require(cluster.nodes["s5"]?.role == .candidate)
    try cluster.tick("s5")
    try cluster.deliverPending()
  }

  private func exchangeNext(_ cluster: inout RaftSimulation, from source: PeerId, to target: PeerId) throws {
    try cluster.deliverNext(from: source, to: target)
    try cluster.deliverNext(from: target, to: source)
  }
}
