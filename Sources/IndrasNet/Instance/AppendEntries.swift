import NIOCore

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
