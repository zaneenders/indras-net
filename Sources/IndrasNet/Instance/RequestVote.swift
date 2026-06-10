import NIOCore

enum RequestVote {
  struct Reply: Equatable, Sendable {
    let granted: Bool
    let term: Term

    enum Action: Equatable {
      case sendAppendEntry(to: PeerId, args: AppendEntries.Args)
      case scheduleNext(delay: Duration)
    }

    init(granted: Bool, term: Term) {
      self.granted = granted
      self.term = term
    }

    func toMessage() -> Message {
      var payload = ByteBuffer()
      payload.writeInteger(term)
      payload.writeInteger(granted ? UInt8(1) : UInt8(0))
      return Message(type: .requestVoteResponse, payload: payload)
    }

    init?(from message: Message) {
      guard message.type == .requestVoteResponse else { return nil }
      var payload = message.payload
      guard
        let term = payload.readInteger(as: Term.self),
        let voteGranted = payload.readInteger(as: UInt8.self)
      else { return nil }
      self.granted = voteGranted != 0
      self.term = term
    }
  }

  struct Args: Equatable, Sendable {
    let term: Term
    let candidateId: PeerId
    let lostLogIndex: Int
    let lastLogTerm: Int

    enum Action: Equatable {
      case sendRequestVoteReply(to: PeerId, term: Term, voteGranted: Bool)
      case scheduleNext(delay: Duration)
      case persist
    }

    init(term: Term, candidateId: PeerId, lostLogIndex: Int, lastLogTerm: Int) {
      self.term = term
      self.candidateId = candidateId
      self.lostLogIndex = lostLogIndex
      self.lastLogTerm = lastLogTerm
    }

    func toMessage() -> Message {
      var payload = ByteBuffer()
      payload.writeInteger(term)
      payload.writeInteger(Int64(lostLogIndex))
      payload.writeInteger(Int64(lastLogTerm))
      payload.writePeerId(candidateId)
      return Message(type: .requestVote, payload: payload)
    }

    init?(from message: Message) {
      guard message.type == .requestVote else { return nil }
      var payload = message.payload
      guard
        let term = payload.readInteger(as: Term.self),
        let lostLogIndex = payload.readInteger(as: Int64.self),
        let lastLogTerm = payload.readInteger(as: Int64.self),
        let candidateId = payload.readPeerId()
      else { return nil }
      self.term = term
      self.lostLogIndex = Int(lostLogIndex)
      self.lastLogTerm = Int(lastLogTerm)
      self.candidateId = candidateId
    }
  }
}
