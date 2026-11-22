import OrderedCollections
import RaftCore
import Testing

enum RaftFixture {
  static let threeVoters = ClusterMembership(voters: ["a", "b", "c"], oldVoters: nil, readers: [])

  static func follower(
    membership: ClusterMembership, term: Term, entries: [LogEntry], commitIndex: LogIndex
  ) -> Instance {
    let state = PersistentState(
      currentTerm: term, votedFor: nil, log: .sentinel + entries, commitIndex: commitIndex,
      logBaseIndex: 0, snapshot: nil, initialMembership: membership)
    return restore(id: "a", state: state, lastApplied: commitIndex)
  }

  static func restore(id: PeerId, state: PersistentState, lastApplied: LogIndex) -> Instance {
    Instance(
      id: id, peers: [], role: .follower, currentTerm: state.currentTerm, votedFor: state.votedFor, votes: [:],
      commitIndex: state.commitIndex, lastApplied: lastApplied, log: state.log, logBaseIndex: state.logBaseIndex,
      snapshot: state.snapshot, initialMembership: state.initialMembership,
      timing: NodeTiming(heartbeatIntervalMs: 50, electionTimeoutMinMs: 150, electionTimeoutMaxMs: 151),
      rng: FixedRandomNumberGenerator())
  }

  static func elect(_ node: inout Instance) throws -> [RequestVote.Reply.Action] {
    let requests = node.onTimerTick().compactMap { directive -> RequestVote.Args? in
      guard case .requestVote(let peer, let args) = directive, peer == "b" else { return nil }
      return args
    }
    let request = try #require(requests.first)
    let actions = node.receiveRequestVoteReply("b", request, .init(granted: true, term: request.term))
    try #require(node.role == .leader)
    return actions
  }

  static func replicationProbe(to peer: PeerId, from leader: inout Instance) throws -> AppendEntries.Args {
    let probes = leader.onTimerTick().compactMap { directive -> AppendEntries.Args? in
      guard case .sendAppendEntry(let target, let args) = directive, target == peer else { return nil }
      return args
    }
    return try #require(probes.first)
  }

  static func acknowledge(
    _ sent: AppendEntries.Args, from peer: PeerId, on leader: inout Instance
  ) -> [AppendEntries.Reply.Action] {
    leader.receiveAppendEntriesReply(
      peer, sent, .init(term: sent.term, success: true, rpcId: sent.rpcId, compactedPrefix: nil))
  }

  private struct FixedRandomNumberGenerator: RandomNumberGenerator, Sendable {
    func next() -> UInt64 { UInt64.max }
  }
}
