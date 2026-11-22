public enum RequestVote: Sendable {
  public struct Reply: Codable, Equatable, Sendable {
    public let granted: Bool
    public let term: Term

    public enum Action: Equatable, Sendable {
      case persist
      case sendAppendEntry(to: PeerId, args: AppendEntries.Args)
      case scheduleNext(delay: Duration)
    }

    public init(granted: Bool, term: Term) {
      self.granted = granted
      self.term = term
    }
  }

  public struct Args: Codable, Equatable, Sendable {
    public let term: Term
    public let candidateId: PeerId
    public let lastLogIndex: LogIndex
    public let lastLogTerm: Term

    public enum Action: Equatable, Sendable {
      case sendRequestVoteReply(to: PeerId, term: Term, voteGranted: Bool)
      case scheduleNext(delay: Duration)
      case persist
    }

    public init(term: Term, candidateId: PeerId, lastLogIndex: LogIndex, lastLogTerm: Term) {
      self.term = term
      self.candidateId = candidateId
      self.lastLogIndex = lastLogIndex
      self.lastLogTerm = lastLogTerm
    }
  }
}
