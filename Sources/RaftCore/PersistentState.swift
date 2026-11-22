import Foundation
import OrderedCollections

public struct PersistentState: Codable, Equatable, Sendable {
  public let currentTerm: Term
  public let votedFor: PeerId?
  public let log: [LogEntry]
  public let commitIndex: LogIndex
  public let logBaseIndex: LogIndex
  public let snapshot: RaftSnapshot?
  public let initialMembership: ClusterMembership?

  public init(
    currentTerm: Term, votedFor: PeerId?, log: [LogEntry], commitIndex: LogIndex,
    logBaseIndex: LogIndex, snapshot: RaftSnapshot?, initialMembership: ClusterMembership?
  ) {
    self.currentTerm = currentTerm
    self.votedFor = votedFor
    self.log = log
    self.commitIndex = commitIndex
    self.logBaseIndex = logBaseIndex
    self.snapshot = snapshot
    self.initialMembership = initialMembership
  }

  private enum CodingKeys: String, CodingKey {
    case currentTerm, votedFor, log, commitIndex, logBaseIndex, snapshot, initialMembership
  }

  public init(from decoder: any Decoder) throws {
    let container = try decoder.container(keyedBy: CodingKeys.self)
    currentTerm = try container.decode(Term.self, forKey: .currentTerm)
    votedFor = try container.decodeIfPresent(PeerId.self, forKey: .votedFor)
    log = try container.decode([LogEntry].self, forKey: .log)
    commitIndex = try container.decodeIfPresent(LogIndex.self, forKey: .commitIndex) ?? 0
    logBaseIndex = try container.decodeIfPresent(LogIndex.self, forKey: .logBaseIndex) ?? 0
    snapshot = try container.decodeIfPresent(RaftSnapshot.self, forKey: .snapshot)
    initialMembership = try container.decodeIfPresent(ClusterMembership.self, forKey: .initialMembership)
  }
}

extension PersistentState {
  /// Storage may reuse an unchanged snapshot from a previously validated state.
  public func validate(node: PeerId, previouslyValidatedSnapshot: RaftSnapshot?) throws {
    try initialMembership?.validate()
    guard initialMembership?.isJoint != true else { throw StateValidationError.invalidState }
    guard !log.isEmpty, logBaseIndex < LogIndex.max - LogIndex(log.count),
      logBaseIndex <= commitIndex, commitIndex <= logBaseIndex + LogIndex(log.count - 1),
      votedFor == nil || (currentTerm > 0 && votedFor?.isEmpty == false)
    else { throw StateValidationError.invalidState }
    if let snapshot {
      guard initialMembership == nil || snapshot.membership != nil else { throw StateValidationError.invalidState }
      if snapshot != previouslyValidatedSnapshot { try snapshot.validate() }
      guard snapshot.lastIncludedIndex >= logBaseIndex, snapshot.lastIncludedIndex <= commitIndex,
        log[Int(snapshot.lastIncludedIndex - logBaseIndex)].term == snapshot.lastIncludedTerm,
        snapshot.lastIncludedTerm <= currentTerm
      else { throw StateValidationError.invalidState }
    } else if logBaseIndex != 0 {
      throw StateValidationError.invalidState
    }
    let boundaryTerm = logBaseIndex == 0 ? 0 : log[0].term
    guard logBaseIndex == 0 || (boundaryTerm > 0 && boundaryTerm <= snapshot!.lastIncludedTerm) else {
      throw StateValidationError.invalidState
    }
    let boundary = log[0]
    guard boundary.term == boundaryTerm, boundary.command.isEmpty, boundary.request == nil, !boundary.isNoOp,
      logBaseIndex > 0 || boundary.membership == nil
    else {
      throw StateValidationError.invalidState
    }
    try boundary.membership?.validate()
    var configuration =
      boundary.membership
      ?? (snapshot?.lastIncludedIndex == logBaseIndex ? snapshot?.membership : nil)
      ?? initialMembership
    var configurationIndex = logBaseIndex
    var previousTerm = boundaryTerm
    for (offset, entry) in log.enumerated().dropFirst() {
      guard entry.term > 0, entry.term >= previousTerm, entry.term <= currentTerm,
        AppendEntries.Args.basePayloadLength(leaderId: node) + entry.encodedLength
          <= Int(RaftLimits.maximumPayloadLength)
      else { throw StateValidationError.invalidState }
      if let membership = entry.membership {
        try membership.validate()
        guard entry.command.isEmpty, entry.request == nil, !entry.isNoOp,
          let previous = configuration
        else { throw StateValidationError.invalidState }
        if let oldVoters = membership.oldVoters {
          guard !previous.isJoint, oldVoters == previous.voters,
            membership.voters.subtracting(previous.voters).isSubset(of: previous.readers)
          else { throw StateValidationError.invalidState }
        } else if previous.isJoint {
          guard membership.voters == previous.voters, membership.readers == previous.readers,
            configurationIndex <= commitIndex
          else { throw StateValidationError.invalidState }
        } else {
          guard membership.voters == previous.voters else { throw StateValidationError.invalidState }
        }
        configuration = membership
        configurationIndex = logBaseIndex + LogIndex(offset)
      }
      if let snapshot, logBaseIndex + LogIndex(offset) == snapshot.lastIncludedIndex,
        let snapshotMembership = snapshot.membership, snapshotMembership != configuration
      {
        throw StateValidationError.invalidState
      }
      previousTerm = entry.term
    }
    if let snapshot, snapshot.lastIncludedIndex == logBaseIndex,
      let snapshotMembership = snapshot.membership, boundary.membership != nil,
      snapshotMembership != boundary.membership
    {
      throw StateValidationError.invalidState
    }
  }
}
