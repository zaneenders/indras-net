import OrderedCollections
import RaftCore
import Testing

struct RaftSimulation {
  private enum RPC {
    case requestVote(RequestVote.Args)
    case voteReply(RequestVote.Args, RequestVote.Reply)
    case append(AppendEntries.Args)
    case appendReply(AppendEntries.Args, AppendEntries.Reply)
  }

  private struct Message {
    let source: PeerId
    let target: PeerId
    let rpc: RPC
  }

  private struct Commit {
    let term: Term
    let entry: LogEntry
  }

  let peers: [PeerId]
  private(set) var nodes: [PeerId: Instance] = [:]
  private(set) var applied: [PeerId: [LogEntry]] = [:]
  private var durable: [PeerId: PersistentState] = [:]
  private var leaders: [Term: PeerId] = [:]
  private var committed: [LogIndex: Commit] = [:]
  private var pending: [Message] = []
  private var random: DeliveryRandomNumberGenerator
  var partition: Set<PeerId> = []

  static func empty(voters: [PeerId], seed: UInt64) throws -> Self {
    let state = PersistentState(
      currentTerm: 0, votedFor: nil, log: .sentinel, commitIndex: 0, logBaseIndex: 0, snapshot: nil,
      initialMembership: ClusterMembership(voters: OrderedSet(voters), oldVoters: nil, readers: []))
    return try Self(states: Dictionary(uniqueKeysWithValues: voters.map { ($0, state) }), seed: seed)
  }

  init(states: [PeerId: PersistentState], seed: UInt64) throws {
    peers = states.keys.sorted()
    random = DeliveryRandomNumberGenerator(state: seed)
    for peer in peers {
      let state = try #require(states[peer])
      try state.validate(node: peer, previouslyValidatedSnapshot: nil)
      try #require(state.logBaseIndex == 0)
      try checkLogMatching(state, on: peer)
      durable[peer] = state
      nodes[peer] = RaftFixture.restore(id: peer, state: state, lastApplied: state.commitIndex)
      applied[peer] = []
      for entry in state.log.dropFirst().prefix(Int(state.commitIndex)) {
        try apply(entry, at: LogIndex(applied[peer, default: []].count) + 1, on: peer)
      }
    }
  }

  mutating func crash(_ peer: PeerId) {
    nodes.removeValue(forKey: peer)
  }

  mutating func restart(_ peer: PeerId) throws {
    try #require(nodes[peer] == nil)
    let state = try #require(durable[peer])
    nodes[peer] = RaftFixture.restore(id: peer, state: state, lastApplied: LogIndex(applied[peer, default: []].count))
  }

  mutating func elapse(_ duration: Duration) {
    for peer in peers {
      nodes[peer]?.elapse(duration)
    }
  }

  mutating func tick(_ peer: PeerId) throws {
    try step(peer) {
      $0.onTimerTick()
    } handle: { cluster, action in
      switch action {
      case .persist: try cluster.persist(peer)
      case .apply(let entry, let index): try cluster.apply(entry, at: index, on: peer)
      case .requestVote(let target, let args): try cluster.send(.requestVote(args), from: peer, to: target)
      case .sendAppendEntry(let target, let args): try cluster.send(.append(args), from: peer, to: target)
      case .scheduleNext: break
      }
    }
  }

  mutating func submit(_ args: ClientSubmit.Args, to peer: PeerId) throws {
    try step(peer) {
      $0.receiveClientSubmit("client", args)
    } handle: { cluster, action in
      switch action {
      case .persist: try cluster.persist(peer)
      case .apply(let entry, let index): try cluster.apply(entry, at: index, on: peer)
      case .sendAppendEntry(let target, let args): try cluster.send(.append(args), from: peer, to: target)
      case .sendClientSubmitReply(_, let reply): try #require(reply.status == .ok)
      case .clientWriteAppended: try cluster.checkPersisted(peer)
      }
    }
  }

  mutating func deliverPending() throws {
    var duplicatesRemaining = 8
    for _ in 0..<1_024 {
      if pending.isEmpty { return }
      let index = Int.random(in: pending.indices, using: &random)
      let message = pending.remove(at: index)
      guard canDeliver(message) else { continue }
      if duplicatesRemaining > 0, Int.random(in: 0..<4, using: &random) == 0 {
        pending.append(message)
        duplicatesRemaining -= 1
      }
      try deliver(message)
    }
    try #require(pending.isEmpty, "Message delivery exceeded the simulation limit")
  }

  mutating func deliverNext(from source: PeerId, to target: PeerId) throws {
    let index = try #require(pending.firstIndex { $0.source == source && $0.target == target })
    let message = pending.remove(at: index)
    try #require(canDeliver(message))
    try deliver(message)
  }

  mutating func discardPending() {
    pending.removeAll()
  }

  private func canDeliver(_ message: Message) -> Bool {
    nodes[message.target] != nil && partition.contains(message.source) == partition.contains(message.target)
  }

  private mutating func deliver(_ message: Message) throws {
    let (source, target) = (message.source, message.target)
    switch message.rpc {
    case .requestVote(let request):
      try step(target) {
        $0.receiveRequestVote(source, request)
      } handle: { cluster, action in
        switch action {
        case .persist: try cluster.persist(target)
        case .sendRequestVoteReply(let destination, let term, let granted):
          try cluster.send(.voteReply(request, .init(granted: granted, term: term)), from: target, to: destination)
        case .scheduleNext: break
        }
      }
    case .voteReply(let request, let reply):
      try step(target) {
        $0.receiveRequestVoteReply(source, request, reply)
      } handle: { cluster, action in
        switch action {
        case .persist: try cluster.persist(target)
        case .sendAppendEntry(let destination, let args):
          try cluster.send(.append(args), from: target, to: destination)
        case .scheduleNext: break
        }
      }
    case .append(let request):
      try step(target) {
        $0.receiveAppendEntries(source, request)
      } handle: { cluster, action in
        switch action {
        case .persist: try cluster.persist(target)
        case .apply(let entry, let index): try cluster.apply(entry, at: index, on: target)
        case .sendAppendEntriesReply(let destination, let term, let success, let compactedPrefix):
          let reply = AppendEntries.Reply(
            term: term, success: success, rpcId: request.rpcId, compactedPrefix: compactedPrefix)
          try cluster.send(.appendReply(request, reply), from: target, to: destination)
        case .scheduleNext: break
        }
      }
    case .appendReply(let request, let reply):
      try step(target) {
        $0.receiveAppendEntriesReply(source, request, reply)
      } handle: { cluster, action in
        switch action {
        case .persist: try cluster.persist(target)
        case .apply(let entry, let index): try cluster.apply(entry, at: index, on: target)
        case .sendAppendEntry(let destination, let args):
          try cluster.send(.append(args), from: target, to: destination)
        case .scheduleNext: break
        }
      }
    }
  }

  private mutating func step<Action>(
    _ peer: PeerId, transition: (inout Instance) -> [Action], handle: (inout Self, Action) throws -> Void
  ) throws {
    let previous = try #require(nodes[peer])
    var node = previous
    let actions = transition(&node)
    nodes[peer] = node
    if node.role == .leader {
      let previousLeader = leaders.updateValue(peer, forKey: node.currentTerm)
      try #require(previousLeader == nil || previousLeader == peer, "Two leaders in term \(node.currentTerm)")
      if previousLeader == nil {
        for (index, commit) in committed where commit.term < node.currentTerm {
          try #require(node.entry(at: index) == commit.entry, "Leader \(peer) lacks committed entry \(index)")
        }
      }
      if previous.role == .leader, previous.currentTerm == node.currentTerm {
        try #require(node.log.starts(with: previous.log), "Leader \(peer) rewrote its log")
      }
    }
    for action in actions {
      try handle(&self, action)
    }
    try checkPersisted(peer)
  }

  private func checkPersisted(_ peer: PeerId) throws {
    try #require(durable[peer] == nodes[peer]?.persistentState, "Undurable state on \(peer)")
  }

  private mutating func send(_ rpc: RPC, from source: PeerId, to target: PeerId) throws {
    try checkPersisted(source)
    pending.append(Message(source: source, target: target, rpc: rpc))
  }

  private mutating func persist(_ peer: PeerId) throws {
    let previous = try #require(durable[peer])
    let state = try #require(nodes[peer]?.persistentState)
    try state.validate(node: peer, previouslyValidatedSnapshot: nil)
    try #require(state.currentTerm >= previous.currentTerm)
    try #require(state.commitIndex >= previous.commitIndex)
    let committedCount = Int(previous.commitIndex) + 1
    try #require(state.log.prefix(committedCount) == previous.log.prefix(committedCount))
    if state.currentTerm == previous.currentTerm, let priorVote = previous.votedFor {
      try #require(state.votedFor == priorVote)
    }
    try checkLogMatching(state, on: peer)
    durable[peer] = state
  }

  private func checkLogMatching(_ state: PersistentState, on peer: PeerId) throws {
    for (otherPeer, other) in durable where otherPeer != peer {
      var matchingPrefix = true
      for (index, entries) in zip(state.log, other.log).enumerated() {
        let (entry, otherEntry) = entries
        matchingPrefix = matchingPrefix && entry == otherEntry
        if entry.term == otherEntry.term {
          try #require(matchingPrefix, "Logs on \(peer) and \(otherPeer) disagree through index \(index)")
        }
      }
    }
  }

  private mutating func apply(_ entry: LogEntry, at index: LogIndex, on peer: PeerId) throws {
    try checkPersisted(peer)
    let state = try #require(durable[peer])
    try #require(index <= state.commitIndex)
    try #require(state.log[Int(index)] == entry)
    try #require(index == LogIndex(applied[peer, default: []].count) + 1, "Out-of-order apply on \(peer)")
    if let previous = committed[index] {
      try #require(previous.entry == entry, "Replicas applied different entries at index \(index)")
    }
    if committed[index].map({ state.currentTerm < $0.term }) ?? true {
      committed[index] = Commit(term: state.currentTerm, entry: entry)
    }
    applied[peer, default: []].append(entry)
  }

  private struct DeliveryRandomNumberGenerator: RandomNumberGenerator {
    var state: UInt64

    mutating func next() -> UInt64 {
      state &+= 0x9e37_79b9_7f4a_7c15
      var result = state
      result = (result ^ (result >> 30)) &* 0xbf58_476d_1ce4_e5b9
      result = (result ^ (result >> 27)) &* 0x94d0_49bb_1331_11eb
      return result ^ (result >> 31)
    }
  }
}
