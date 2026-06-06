import NIOCore

typealias Term = Int

struct Instance {
  let id: PeerId
  private(set) var role: Role
  private(set) var currentTerm: Term
  private(set) var votedFor: PeerId?
  private(set) var peers: Set<PeerId>
  private(set) var votes: [PeerId: Bool]

  init(
    id: PeerId,
    peers: Set<PeerId> = [],
    role: Role = .follower,
    currentTerm: Term = 0,
    votedFor: PeerId? = nil,
    votes: [PeerId: Bool] = [:]
  ) {
    self.id = id
    self.peers = peers
    self.role = role
    self.currentTerm = currentTerm
    self.votedFor = votedFor
    self.votes = votes
  }

  mutating func onRequestVote(_ peer: PeerId, _ requst: RequestVote.Args) -> [RequestVote.Args.Action] {
    var actions: [RequestVote.Args.Action] = []
    var grantVote = false

    if requst.term > currentTerm {
      currentTerm = requst.term
      votedFor = nil
      votes = [:]
      role = .follower
    }

    if requst.term < currentTerm {
      actions.append(.sendRequestVoteReply(to: peer, term: currentTerm, voteGranted: grantVote))
      return actions
    }

    if role == .leader || (role == .candidate && requst.candidateId != id) {
      actions.append(.sendRequestVoteReply(to: peer, term: currentTerm, voteGranted: grantVote))
      return actions
    }

    if votedFor == nil || votedFor == requst.candidateId {
      // TODO: At least as upto date
      grantVote = true
      votedFor = requst.candidateId
    }
    actions.append(.sendRequestVoteReply(to: peer, term: currentTerm, voteGranted: grantVote))
    return actions
  }

  mutating func onElectionTimeOut() -> [ElectionTimeoutAction] {
    var actions: [ElectionTimeoutAction] = []
    guard role == .follower else { return actions }

    self.currentTerm += 1
    self.role = .candidate
    self.votedFor = self.id
    self.votes = [self.id: true]

    for peer in peers {
      actions.append(
        .requstVote(
          to: peer,
          args: RequestVote.Args(
            term: self.currentTerm, candidateId: self.id, lostLogIndex: 0, lastLogTerm: 0)))
    }
    return actions
  }

  mutating func onRequestVoteReply(_ peer: PeerId, _ reply: RequestVote.Reply) -> [RequestVote.Reply.Action] {
    var actions: [RequestVote.Reply.Action] = []

    if reply.term > currentTerm {
      self.role = .follower
      self.currentTerm = reply.term
      self.votedFor = nil
      self.votes = [:]
      return actions
    }

    guard role == .candidate, reply.term == currentTerm else { return actions }

    self.votes[peer] = reply.granted
    if self.votes.isLeader(peers.count) {
      self.role = .leader
      let heartbeat = AppendEntries.Args(term: currentTerm, leaderId: id)
      for peer in peers {
        actions.append(.sendAppendEntry(to: peer, args: heartbeat))
      }
    }

    return actions
  }

  mutating func onAppendEntries(_ leader: PeerId, _ args: AppendEntries.Args) -> [AppendEntries.Args.Action] {
    var actions: [AppendEntries.Args.Action] = []

    if args.term < currentTerm {
      return actions
    }

    if args.term > currentTerm {
      currentTerm = args.term
      votedFor = nil
      votes = [:]
    }

    role = .follower
    actions.append(.resetElectionTimeout)
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
    return votes > peers / 2
  }
}

enum ElectionTimeoutAction: Equatable {
  case requstVote(to: PeerId, args: RequestVote.Args)
}

enum RequestVote {
  struct Reply: Equatable, Sendable {
    let granted: Bool
    let term: Term

    enum Action: Equatable {
      case sendAppendEntry(to: PeerId, args: AppendEntries.Args)
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

    enum Action: Equatable {
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

enum AppendEntries {
  struct Args: Equatable, Sendable {
    let term: Term
    let leaderId: PeerId

    enum Action: Equatable {
      case resetElectionTimeout
    }

    init(term: Term, leaderId: PeerId) {
      self.term = term
      self.leaderId = leaderId
    }

    func toMessage() -> Message {
      var payload = ByteBuffer()
      payload.writeInteger(Int64(term))
      payload.writePeerId(leaderId)
      return Message(type: .appendEntries, payload: payload)
    }

    init?(from message: Message) {
      guard message.type == .appendEntries else { return nil }
      var payload = message.payload
      guard
        let term = payload.readInteger(as: Int64.self),
        let leaderId = payload.readPeerId()
      else { return nil }
      self.term = Int(term)
      self.leaderId = leaderId
    }
  }
}

enum Role: Equatable {
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
