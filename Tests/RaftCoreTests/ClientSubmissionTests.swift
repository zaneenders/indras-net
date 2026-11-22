import Foundation
import OrderedCollections
import RaftCore
import Testing

struct ClientSubmissionTests {
  @Test func followerRedirectsToTheLastKnownLeader() {
    var follower = RaftFixture.follower(membership: RaftFixture.threeVoters, term: 1, entries: [], commitIndex: 0)
    let request = ClientSubmit.Args(requestId: 1, command: Data([1]), traceHeaders: [:])
    let initial = follower.persistentState
    let noLeader = follower.receiveClientSubmit("client", request)
    #expect(
      noLeader == [
        .sendClientSubmitReply(
          to: "client", reply: .init(requestId: 1, status: .notLeader, leaderId: nil, logIndex: 0, result: nil))
      ])
    _ = follower.receiveAppendEntries(
      "b",
      .init(
        term: 1, leaderId: "b", prevLogIndex: 0, prevLogTerm: 0, entries: [], leaderCommit: 0, rpcId: 0,
        traceHeaders: [:]))
    let knownLeader = follower.receiveClientSubmit("client", request)
    #expect(
      knownLeader == [
        .sendClientSubmitReply(
          to: "client", reply: .init(requestId: 1, status: .notLeader, leaderId: "b", logIndex: 0, result: nil))
      ])
    #expect(follower.persistentState == initial)
  }

  @Test func retriesReuseThePendingAndAppliedIndexForTheSameClient() throws {
    var leader = RaftFixture.follower(membership: RaftFixture.threeVoters, term: 0, entries: [], commitIndex: 0)
    _ = try RaftFixture.elect(&leader)
    let request = ClientSubmit.Args(requestId: 7, command: Data([1]), traceHeaders: [:])
    let registered = ClientSubmit.Args.Action.clientWriteAppended(logIndex: 1, requestId: 7, client: "first")
    let first = leader.receiveClientSubmit("first", request)
    #expect(Array(first.prefix(2)) == [.persist, registered])
    let pending = leader.persistentState
    let pendingRetry = leader.receiveClientSubmit("first", request)
    #expect(pendingRetry.first == registered)
    #expect(leader.persistentState == pending)
    #expect(leader.commitIndex == 0)

    let replication = try RaftFixture.replicationProbe(to: "b", from: &leader)
    _ = RaftFixture.acknowledge(replication, from: "b", on: &leader)
    #expect(leader.lastApplied == 1)
    let committed = leader.persistentState
    let appliedRetry = leader.receiveClientSubmit("first", request)
    #expect(
      appliedRetry == [
        .sendClientSubmitReply(
          to: "first", reply: .init(requestId: 7, status: .ok, leaderId: nil, logIndex: 1, result: nil))
      ])
    #expect(leader.persistentState == committed)

    let otherClient = leader.receiveClientSubmit("second", request)
    #expect(
      Array(otherClient.prefix(2)) == [.persist, .clientWriteAppended(logIndex: 2, requestId: 7, client: "second")])
    #expect(leader.entry(at: 1)?.request == RequestIdentity(client: "first", requestId: 7))
    #expect(leader.entry(at: 2)?.request == RequestIdentity(client: "second", requestId: 7))
  }

  @Test func payloadLimitIncludesTheRequestIdentityAndLongestMemberID() throws {
    let reader = "longer-reader-id"
    let membership = ClusterMembership(voters: ["a", "b", "c"], oldVoters: nil, readers: [reader])
    var leader = RaftFixture.follower(membership: membership, term: 0, entries: [], commitIndex: 0)
    _ = try RaftFixture.elect(&leader)
    let identity = RequestIdentity(client: "longer-client-id", requestId: 1)
    let empty = LogEntry(term: 1, command: Data(), request: identity, isNoOp: false)
    let available =
      Int(RaftLimits.maximumPayloadLength) - AppendEntries.Args.basePayloadLength(leaderId: reader)
      - empty.encodedLength
    let initial = leader.persistentState
    let oversize = ClientSubmit.Args(requestId: 1, command: Data(repeating: 1, count: available + 1), traceHeaders: [:])
    let rejected = leader.receiveClientSubmit(identity.client, oversize)
    #expect(
      rejected == [
        .sendClientSubmitReply(
          to: identity.client,
          reply: .init(requestId: 1, status: .commandTooLarge, leaderId: nil, logIndex: 0, result: nil))
      ])
    #expect(leader.persistentState == initial)

    let fits = ClientSubmit.Args(requestId: 1, command: Data(repeating: 1, count: available), traceHeaders: [:])
    let accepted = leader.receiveClientSubmit(identity.client, fits)
    #expect(
      Array(accepted.prefix(2)) == [.persist, .clientWriteAppended(logIndex: 1, requestId: 1, client: identity.client)])
    #expect(leader.entry(at: 1)?.command == fits.command)
    #expect(leader.commitIndex == 0)
  }
}
