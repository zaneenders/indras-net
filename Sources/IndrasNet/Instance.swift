import NIOCore

typealias Term = Int

struct Instance {
  let id: PeerId
  var role: Role
  var currentTerm: Term
  var votedFor: PeerId?
  var peers: Set<PeerId>
  var votes: [PeerId: Bool] = [:]

  init(_ peerID: PeerId) {
    self.id = peerID
    self.role = .follower
    self.currentTerm = 0
    self.votedFor = nil
    self.peers = []
  }

  mutating func onRequestVote(_ peer: PeerId, _ requst: RequestVote.Args) -> [RequestVote.Args.Action] {
    var actions: [RequestVote.Args.Action] = []
    var grantVote = false
    if requst.term < currentTerm {
      actions.append(.sendRequestVoteReply(to: peer, term: currentTerm, voteGranted: grantVote))
      return actions
    }

    if votedFor == nil || votedFor == requst.candidateId {
      // TODO: At least as upto date
      grantVote = true
    }
    actions.append(.sendRequestVoteReply(to: peer, term: currentTerm, voteGranted: grantVote))
    return actions
  }

  mutating func onElectionTimeOut() -> [ElectionTimeoutAction] {
    var actions: [ElectionTimeoutAction] = []

    self.role = .candidate
    self.votes[self.id] = true

    for peer in peers {
      actions.append(
        .requstVote(
          to: peer,
          args: RequestVote.Args(term: self.currentTerm, candidateId: self.id, lostLogIndex: 0, lastLogTerm: 0)))
    }
    return actions
  }

  mutating func onRequestVoteReply(_ peer: PeerId, _ reply: RequestVote.Reply) -> [RequestVote.Reply.Action] {
    var actions: [RequestVote.Reply.Action] = []

    if reply.term > currentTerm {
      self.role = .follower
      self.currentTerm = reply.term
      return actions
    }

    self.votes[peer] = reply.granted
    if self.votes.isLeader(peers.count) {
      self.role = .leader
      print("LEADER: \(self.id)")
      for peer in peers {
        actions.append(.sendAppendEntry(peer))
      }
    }

    return actions
  }
}

extension [PeerId: Bool] {
  func isLeader(_ peers: Int) -> Bool {
    let votes = self.values.reduce(
      into: 0,
      { count, votedFor in
        if votedFor {
          count += 1
        }
      })
    print(#function, votes, " peers: ", peers)

    return votes > peers / 2
  }
}

enum ElectionTimeoutAction {
  case requstVote(to: PeerId, args: RequestVote.Args)
}

enum RequestVote {
  struct Reply: Equatable, Sendable {
    let granted: Bool
    let term: Term

    enum Action {
      case sendAppendEntry(PeerId)
    }

    init(granted: Bool, term: Term) {
      self.granted = granted
      self.term = term
    }

    func toMessage() -> Message {
      var payload = ByteBuffer()
      payload.writeInteger(Int64(term))
      payload.writeInteger(granted ? UInt8(1) : UInt8(0))
      return Message(type: .requestVoteResponse, payload: payload)
    }

    init?(from message: Message) {
      guard message.type == .requestVoteResponse else { return nil }
      var payload = message.payload
      guard
        let term = payload.readInteger(as: Int64.self),
        let voteGranted = payload.readInteger(as: UInt8.self)
      else { return nil }
      self.granted = voteGranted != 0
      self.term = Int(term)
    }
  }

  struct Args: Equatable, Sendable {
    let term: Int
    let candidateId: PeerId
    let lostLogIndex: Int
    let lastLogTerm: Int

    enum Action {
      case sendRequestVoteReply(to: PeerId, term: Term, voteGranted: Bool)
      case roleChanged(Role)
      case resetElectionTimeout
      case persist
    }

    init(term: Int, candidateId: PeerId, lostLogIndex: Int, lastLogTerm: Int) {
      self.term = term
      self.candidateId = candidateId
      self.lostLogIndex = lostLogIndex
      self.lastLogTerm = lastLogTerm
    }

    func toMessage() -> Message {
      var payload = ByteBuffer()
      payload.writeInteger(Int64(term))
      payload.writeInteger(Int64(lostLogIndex))
      payload.writeInteger(Int64(lastLogTerm))
      payload.writePeerId(candidateId)
      return Message(type: .requestVote, payload: payload)
    }

    init?(from message: Message) {
      guard message.type == .requestVote else { return nil }
      var payload = message.payload
      guard
        let term = payload.readInteger(as: Int64.self),
        let lostLogIndex = payload.readInteger(as: Int64.self),
        let lastLogTerm = payload.readInteger(as: Int64.self),
        let candidateId = payload.readPeerId()
      else { return nil }
      self.term = Int(term)
      self.lostLogIndex = Int(lostLogIndex)
      self.lastLogTerm = Int(lastLogTerm)
      self.candidateId = candidateId
    }
  }
}

enum Role {
  case follower
  case leader
  case candidate
}

extension ByteBuffer {
  fileprivate mutating func writePeerId(_ peerId: PeerId) {
    writeInteger(UInt32(peerId.utf8.count))
    writeString(peerId)
  }

  fileprivate mutating func readPeerId() -> PeerId? {
    guard let length = readInteger(as: UInt32.self) else { return nil }
    return readString(length: Int(length))
  }
}
