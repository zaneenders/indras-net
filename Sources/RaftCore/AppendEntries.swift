public enum AppendEntries: Sendable {
  public struct CompactedPrefix: Codable, Equatable, Sendable {
    public let index: LogIndex
    public let term: Term

    public init(index: LogIndex, term: Term) {
      self.index = index
      self.term = term
    }
  }

  public struct Reply: Codable, Equatable, Sendable {
    public let term: Term
    public let success: Bool
    public let rpcId: UInt64
    public let compactedPrefix: CompactedPrefix?

    public enum Action: Equatable, Sendable {
      case scheduleNext(delay: Duration)
      case sendAppendEntry(to: PeerId, args: Args)
      case apply(entry: LogEntry, atIndex: LogIndex)
      case persist
    }

    public init(term: Term, success: Bool, rpcId: UInt64, compactedPrefix: CompactedPrefix?) {
      self.term = term
      self.success = success
      self.rpcId = rpcId
      self.compactedPrefix = compactedPrefix
    }
  }

  public struct Args: Codable, Equatable, Sendable {
    public let term: Term
    public let leaderId: PeerId
    public let prevLogIndex: LogIndex
    public let prevLogTerm: Term
    public let entries: [LogEntry]
    public let leaderCommit: LogIndex
    public let rpcId: UInt64
    public let traceHeaders: [String: String]

    public enum Action: Equatable, Sendable {
      case sendAppendEntriesReply(
        to: PeerId, term: Term, success: Bool, compactedPrefix: CompactedPrefix?)
      case scheduleNext(delay: Duration)
      case apply(entry: LogEntry, atIndex: LogIndex)
      case persist
    }

    public init(
      term: Term,
      leaderId: PeerId,
      prevLogIndex: LogIndex,
      prevLogTerm: Term,
      entries: [LogEntry],
      leaderCommit: LogIndex,
      rpcId: UInt64,
      traceHeaders: [String: String]
    ) {
      self.term = term
      self.leaderId = leaderId
      self.prevLogIndex = prevLogIndex
      self.prevLogTerm = prevLogTerm
      self.entries = entries
      self.leaderCommit = leaderCommit
      self.traceHeaders = traceHeaders
      self.rpcId = rpcId
    }

    public static func basePayloadLength(leaderId: PeerId) -> Int {
      48 + leaderId.utf8.count
    }

    public func withRPCId(_ rpcId: UInt64, traceHeaders: [String: String]) -> Self {
      Self(
        term: term, leaderId: leaderId, prevLogIndex: prevLogIndex, prevLogTerm: prevLogTerm,
        entries: entries, leaderCommit: leaderCommit, rpcId: rpcId, traceHeaders: traceHeaders)
    }

    public var replicatedThrough: LogIndex {
      prevLogIndex + LogIndex(entries.count)
    }
  }
}
