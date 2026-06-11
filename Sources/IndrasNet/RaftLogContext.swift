import Logging

struct RaftLogContext {
  let kind: String
  let direction: String
  let peer: PeerId
  let term: Term
  var granted: Bool?
  var success: Bool?

  fileprivate init(
    kind: String, direction: String, peer: PeerId, term: Term, granted: Bool? = nil, success: Bool? = nil
  ) {
    self.kind = kind
    self.direction = direction
    self.peer = peer
    self.term = term
    self.granted = granted
    self.success = success
  }

  var level: Logger.Level {
    switch kind {
    case "appendEntries", "appendEntriesResponse":
      if success == false { return .info }
      return .trace
    default:
      return .info
    }
  }

  var metadata: Logger.Metadata {
    var metadata: Logger.Metadata = [
      ShellLogKey.kind: .string(kind),
      ShellLogKey.direction: .string(direction),
      ShellLogKey.peer: .string(peer),
    ]
    metadata["shell.term"] = .stringConvertible(term)
    if let granted {
      metadata["shell.granted"] = .stringConvertible(granted)
    }
    if let success {
      metadata["shell.success"] = .stringConvertible(success)
    }
    return metadata
  }

  func message(selfNode: PeerId) -> String {
    let arrow = direction == "out" ? "->" : "<-"
    var text = "[\(selfNode)] \(kind) \(arrow) \(peer) term=\(term)"
    if let granted {
      text += granted ? " granted" : " denied"
    }
    if success == false {
      text += " rejected"
    }
    return text
  }

  static func requestVote(direction: String, peer: PeerId, term: Term) -> RaftLogContext {
    RaftLogContext(kind: "requestVote", direction: direction, peer: peer, term: term)
  }

  static func requestVoteResponse(
    direction: String,
    peer: PeerId,
    term: Term,
    granted: Bool
  ) -> RaftLogContext {
    RaftLogContext(kind: "requestVoteResponse", direction: direction, peer: peer, term: term, granted: granted)
  }

  static func appendEntries(direction: String, peer: PeerId, term: Term) -> RaftLogContext {
    RaftLogContext(kind: "appendEntries", direction: direction, peer: peer, term: term)
  }

  static func appendEntriesResponse(
    direction: String,
    peer: PeerId,
    term: Term,
    success: Bool
  ) -> RaftLogContext {
    RaftLogContext(kind: "appendEntriesResponse", direction: direction, peer: peer, term: term, success: success)
  }

  static func clientSubmitResponse(peer: PeerId) -> RaftLogContext {
    RaftLogContext(kind: "clientSubmitResponse", direction: "out", peer: peer, term: 0)
  }
}
