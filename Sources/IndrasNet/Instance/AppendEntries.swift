import NIOCore

enum AppendEntries {
  struct Reply: Equatable, Sendable {
    let term: Term
    let success: Bool

    enum Action: Equatable {}

    init(term: Term, success: Bool) {
      self.term = term
      self.success = success
    }

    func toMessage() -> Message {
      var payload = ByteBuffer()
      payload.writeInteger(Int64(term))
      payload.writeInteger(success ? UInt8(1) : UInt8(0))
      return Message(type: .appendEntriesResponse, payload: payload)
    }

    init?(from message: Message) {
      guard message.type == .appendEntriesResponse else { return nil }
      var payload = message.payload
      guard
        let term = payload.readInteger(as: Int64.self),
        let success = payload.readInteger(as: UInt8.self)
      else { return nil }
      self.term = Int(term)
      self.success = success != 0
    }
  }

  struct Args: Equatable, Sendable {
    let term: Term
    let leaderId: PeerId

    enum Action: Equatable {
      case sendAppendEntriesReply(to: PeerId, term: Term, success: Bool)
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
