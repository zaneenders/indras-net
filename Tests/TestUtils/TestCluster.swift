import Foundation
import Testing

@testable import IndrasNet

public struct TestCluster {
  public private(set) var nodes: [PeerId: Instance]

  public init(nodes: [PeerId: Instance]) {
    self.nodes = nodes
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
        var peerNode = nodes[peer]!
        let followerActions = peerNode.receiveAppendEntries(nodeID, args)
        nodes[peer] = peerNode
        processFollowerActions(leader: nodeID, peer: peer, followerActions, reply: &reply)
      case .persist:
        break
      }
    }
  }

  private mutating func processFollowerActions(
    leader: PeerId,
    peer: PeerId,
    _ actions: [AppendEntries.Args.Action],
    reply: inout ClientSubmit.Reply?
  ) {
    let success = !actions.contains {
      if case .sendAppendEntriesReply(_, _, let ok) = $0 { return !ok }
      return false
    }
    var leaderNode = nodes[leader]!
    let leaderActions = leaderNode.receiveAppendEntriesReply(
      peer, .init(term: leaderNode.currentTerm, success: success))
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
        var peerNode = nodes[peer]!
        let followerActions = peerNode.receiveAppendEntries(leader, args)
        nodes[peer] = peerNode
        processFollowerActions(leader: leader, peer: peer, followerActions, reply: &reply)
      case .apply, .persist, .scheduleNext:
        break
      }
    }
  }
}
