import Foundation
import Testing

@testable import IndrasNet

public struct TestCluster {
  public private(set) var nodes: [PeerId: Instance]
  private var disconnectedLinks: Set<Link> = []

  public init(nodes: [PeerId: Instance]) {
    self.nodes = nodes
  }

  /// Fresh cluster: every node is a follower in term 0 with only the sentinel log entry.
  /// Each node's `peers` is the full mesh minus itself.
  /// When `seed` is set, each node gets a deterministic RNG derived from it.
  public init(peers: [PeerId], seed: UInt64? = nil) {
    let peerSet = Set(peers)
    let nodeSeeds: [UInt64]
    if let seed {
      var clusterRNG = SeededRandomNumberGenerator(seed: seed)
      nodeSeeds = peers.map { _ in clusterRNG.next() }
    } else {
      nodeSeeds = []
    }

    self.nodes = Dictionary(
      uniqueKeysWithValues: peers.enumerated().map { index, id in
        let peers = peerSet.subtracting([id])
        if index < nodeSeeds.count {
          let instance = Instance(
            id: id,
            peers: peers,
            rng: SeededRandomNumberGenerator(seed: nodeSeeds[index]))
          return (id, instance)
        }
        return (id, Instance(id: id, peers: peers))
      })
  }

  public var leader: PeerId? {
    for node in nodes {
      if node.value.role == .leader {
        return node.key
      }
    }
    return nil
  }

  public mutating func fireTimer(_ node: PeerId, at now: ContinuousClock.Instant = .now) {
    var nodeInstance = nodes[node]!
    let directives = nodeInstance.onTimerTick(at: now)
    nodes[node] = nodeInstance
    processTimerDirectives(from: node, directives)
  }

  public func isConnected(from sender: PeerId, to recipient: PeerId) -> Bool {
    sender == recipient || !disconnectedLinks.contains(Link(sender, recipient))
  }

  public mutating func disconnect(from sender: PeerId, to recipient: PeerId) {
    disconnectedLinks.insert(Link(sender, recipient))
  }

  public mutating func disconnect(_ peer: PeerId) {
    for other in nodes.keys where other != peer {
      disconnect(from: peer, to: other)
    }
  }

  public mutating func reconnect(from sender: PeerId, to recipient: PeerId) {
    disconnectedLinks.remove(Link(sender, recipient))
  }

  public mutating func reconnect(_ peer: PeerId) {
    disconnectedLinks = disconnectedLinks.filter { !$0.involves(peer) }
  }

  public mutating func reconnectAll() {
    disconnectedLinks.removeAll()
  }

  public mutating func submit(
    command: Data,
    to node: PeerId,
    requestId: UInt128 = 1,
    client: PeerId = RaftClient.defaultClientID
  ) -> ClientSubmit.Reply? {
    var reply: ClientSubmit.Reply?
    var nodeInstance = nodes[node]!
    let actions = nodeInstance.receiveClientSubmit(
      client, ClientSubmit.Args(requestId: requestId, command: command))
    nodes[node] = nodeInstance
    processClientActions(from: node, actions, reply: &reply)
    return reply
  }

  private mutating func processTimerDirectives(
    from nodeID: PeerId,
    _ directives: [TimerDirective]
  ) {
    for directive in directives {
      switch directive {
      case .scheduleNext:
        break
      case .requestVote(let peer, let args):
        deliverRequestVote(from: nodeID, to: peer, args: args)
      case .sendAppendEntry(let peer, let args):
        deliverAppendEntries(from: nodeID, to: peer, args: args)
      }
    }
  }

  private mutating func processClientActions(
    from nodeID: PeerId,
    _ actions: [ClientSubmit.Args.Action],
    reply: inout ClientSubmit.Reply?
  ) {
    for action in actions {
      switch action {
      case .sendClientSubmitReply(_, let clientReply):
        reply = clientReply
      case .sendAppendEntry(let peer, let args):
        deliverAppendEntries(from: nodeID, to: peer, args: args, reply: &reply)
      case .persist:
        break
      }
    }
  }

  private mutating func deliverRequestVote(from sender: PeerId, to recipient: PeerId, args: RequestVote.Args) {
    guard isConnected(from: sender, to: recipient) else { return }

    var recipientNode = nodes[recipient]!
    let actions = recipientNode.receiveRequestVote(sender, args)
    nodes[recipient] = recipientNode
    processRequestVoteActions(candidate: sender, sent: args, from: recipient, actions)
  }

  private mutating func processRequestVoteActions(
    candidate: PeerId,
    sent: RequestVote.Args,
    from voter: PeerId,
    _ actions: [RequestVote.Args.Action]
  ) {
    for action in actions {
      switch action {
      case .sendRequestVoteReply(let peer, let term, let voteGranted):
        deliverRequestVoteReply(from: voter, to: peer, sent: sent, term: term, voteGranted: voteGranted)
      case .scheduleNext, .persist:
        break
      }
    }
  }

  private mutating func deliverRequestVoteReply(
    from sender: PeerId,
    to recipient: PeerId,
    sent: RequestVote.Args,
    term: Term,
    voteGranted: Bool
  ) {
    guard isConnected(from: sender, to: recipient) else { return }

    var recipientNode = nodes[recipient]!
    let actions = recipientNode.receiveRequestVoteReply(
      sender, sent, .init(granted: voteGranted, term: term))
    nodes[recipient] = recipientNode
    processRequestVoteReplyActions(from: recipient, actions)
  }

  private mutating func processRequestVoteReplyActions(
    from candidate: PeerId,
    _ actions: [RequestVote.Reply.Action]
  ) {
    for action in actions {
      switch action {
      case .sendAppendEntry(let peer, let args):
        deliverAppendEntries(from: candidate, to: peer, args: args)
      case .scheduleNext:
        break
      }
    }
  }

  private mutating func deliverAppendEntries(
    from sender: PeerId,
    to recipient: PeerId,
    args: AppendEntries.Args
  ) {
    var reply: ClientSubmit.Reply?
    deliverAppendEntries(from: sender, to: recipient, args: args, reply: &reply)
  }

  private mutating func deliverAppendEntries(
    from sender: PeerId,
    to recipient: PeerId,
    args: AppendEntries.Args,
    reply: inout ClientSubmit.Reply?
  ) {
    guard isConnected(from: sender, to: recipient) else { return }

    var recipientNode = nodes[recipient]!
    let followerActions = recipientNode.receiveAppendEntries(sender, args)
    nodes[recipient] = recipientNode
    processFollowerActions(leader: sender, peer: recipient, sent: args, followerActions, reply: &reply)
  }

  private mutating func processFollowerActions(
    leader: PeerId,
    peer: PeerId,
    sent: AppendEntries.Args,
    _ actions: [AppendEntries.Args.Action],
    reply: inout ClientSubmit.Reply?
  ) {
    let success = !actions.contains {
      if case .sendAppendEntriesReply(_, _, let ok) = $0 { return !ok }
      return false
    }
    guard isConnected(from: peer, to: leader) else { return }

    var leaderNode = nodes[leader]!
    let leaderActions = leaderNode.receiveAppendEntriesReply(
      peer, sent, .init(term: leaderNode.currentTerm, success: success))
    nodes[leader] = leaderNode
    processLeaderReplyActions(from: leader, leaderActions, reply: &reply)
  }

  private mutating func processLeaderReplyActions(
    from leader: PeerId,
    _ actions: [AppendEntries.Reply.Action],
    reply: inout ClientSubmit.Reply?
  ) {
    for action in actions {
      switch action {
      case .notifyClient(let requestId, let logIndex, _):
        reply = ClientSubmit.Reply(requestId: requestId, status: .ok, logIndex: logIndex)
      case .sendAppendEntry(let peer, let args):
        deliverAppendEntries(from: leader, to: peer, args: args, reply: &reply)
      case .apply, .persist, .scheduleNext:
        break
      }
    }
  }
}
