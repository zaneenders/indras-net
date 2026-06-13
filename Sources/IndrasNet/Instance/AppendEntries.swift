import NIOCore

enum AppendEntries {
  struct Reply: Equatable, Sendable {
    let term: Term
    let success: Bool

    enum Action: Equatable {
      case scheduleNext(delay: Duration)
      case sendAppendEntry(to: PeerId, args: Args)
      case apply(entry: LogEntry)
      case notifyClient(requestId: UInt128, logIndex: LogIndex, to: PeerId)
      case persist
    }

    init(term: Term, success: Bool) {
      self.term = term
      self.success = success
    }

    func toMessage() -> Message {
      var payload = ByteBuffer()
      payload.writeInteger(term)
      payload.writeInteger(success ? UInt8(1) : UInt8(0))
      return Message(type: .appendEntriesResponse, payload: payload)
    }

    init?(from message: Message) {
      guard message.type == .appendEntriesResponse else { return nil }
      var payload = message.payload
      guard
        let term = payload.readInteger(as: Term.self),
        let success = payload.readInteger(as: UInt8.self)
      else { return nil }
      self.term = term
      self.success = success != 0
    }
  }

  struct Args: Equatable, Sendable {
    let term: Term
    let leaderId: PeerId
    let prevLogIndex: LogIndex
    let prevLogTerm: Term
    let entries: [LogEntry]
    let leaderCommit: LogIndex

    enum Action: Equatable {
      case sendAppendEntriesReply(to: PeerId, term: Term, success: Bool)
      case scheduleNext(delay: Duration)
      case apply(entry: LogEntry)
      case persist
    }

    init(
      term: Term,
      leaderId: PeerId,
      prevLogIndex: LogIndex = 0,
      prevLogTerm: Term = 0,
      entries: [LogEntry] = [],
      leaderCommit: LogIndex = 0
    ) {
      self.term = term
      self.leaderId = leaderId
      self.prevLogIndex = prevLogIndex
      self.prevLogTerm = prevLogTerm
      self.entries = entries
      self.leaderCommit = leaderCommit
    }

    func toMessage() -> Message {
      var payload = ByteBuffer()
      payload.writeInteger(term)
      payload.writePeerId(leaderId)
      payload.writeInteger(prevLogIndex)
      payload.writeInteger(prevLogTerm)
      payload.writeInteger(leaderCommit)
      payload.writeLogEntries(entries)
      return Message(type: .appendEntries, payload: payload)
    }

    init?(from message: Message) {
      guard message.type == .appendEntries else { return nil }
      var payload = message.payload
      guard
        let term = payload.readInteger(as: Term.self),
        let leaderId = payload.readPeerId(),
        let prevLogIndex = payload.readInteger(as: LogIndex.self),
        let prevLogTerm = payload.readInteger(as: Term.self),
        let leaderCommit = payload.readInteger(as: LogIndex.self),
        let entries = payload.readLogEntries()
      else { return nil }
      self.term = term
      self.leaderId = leaderId
      self.prevLogIndex = prevLogIndex
      self.prevLogTerm = prevLogTerm
      self.entries = entries
      self.leaderCommit = leaderCommit
    }

    var replicatedThrough: LogIndex {
      prevLogIndex + LogIndex(entries.count)
    }
  }
}
