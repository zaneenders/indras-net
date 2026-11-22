import Foundation
public import OrderedCollections

/// A stable configuration, or the joint old/new configuration from Raft §6.
/// Readers replicate the log but never campaign or count toward this configuration's quorums.
/// They may grant votes to candidates whose newer configuration has promoted them.
public struct ClusterMembership: Codable, Equatable, Sendable {
  public let voters: OrderedSet<PeerId>
  public let oldVoters: OrderedSet<PeerId>?
  public let readers: OrderedSet<PeerId>

  public init(
    voters: OrderedSet<PeerId>, oldVoters: OrderedSet<PeerId>?,
    readers: OrderedSet<PeerId>
  ) {
    self.voters = OrderedSet(voters.sorted())
    self.oldVoters = oldVoters.map { OrderedSet($0.sorted()) }
    self.readers = OrderedSet(readers.sorted())
  }

  private enum CodingKeys: String, CodingKey { case voters, oldVoters, readers }

  public init(from decoder: any Decoder) throws {
    let values = try decoder.container(keyedBy: CodingKeys.self)
    let voters = try values.decode([PeerId].self, forKey: .voters)
    let oldVoters = try values.decodeIfPresent([PeerId].self, forKey: .oldVoters)
    let readers = try values.decode([PeerId].self, forKey: .readers)
    guard Set(voters).count == voters.count, Set(readers).count == readers.count,
      oldVoters.map({ Set($0).count == $0.count }) ?? true
    else { throw StateValidationError.invalidState }
    self.init(voters: OrderedSet(voters), oldVoters: oldVoters.map(OrderedSet.init), readers: OrderedSet(readers))
    try validate()
  }

  public var allVoters: OrderedSet<PeerId> { (oldVoters ?? []).union(voters) }
  public var members: OrderedSet<PeerId> { allVoters.union(readers) }
  public var isJoint: Bool { oldVoters != nil }

  public func hasQuorum(_ acknowledgements: Set<PeerId>) -> Bool {
    func majority(_ voters: OrderedSet<PeerId>) -> Bool {
      voters.filter { acknowledgements.contains($0) }.count > voters.count / 2
    }
    return majority(voters) && (oldVoters.map(majority) ?? true)
  }

  public func validate() throws {
    guard !voters.isEmpty, oldVoters?.isEmpty != true,
      oldVoters == nil || oldVoters != voters,
      readers.isDisjoint(with: allVoters), members.count <= 1_024,
      members.allSatisfy({
        (1...512).contains($0.utf8.count) && $0.utf8.allSatisfy { (33...126).contains($0) }
      })
    else { throw StateValidationError.invalidState }
  }

  public var encodedLength: Int {
    9 + voters.reduce(0) { $0 + 4 + $1.utf8.count }
      + readers.reduce(0) { $0 + 4 + $1.utf8.count }
      + (oldVoters.map { 4 + $0.reduce(0) { $0 + 4 + $1.utf8.count } } ?? 0)
  }
}

public enum MembershipChangeError: Error, Equatable {
  case notLeader
  case changeInProgress
  case invalidConfiguration
  case readerNotCaughtUp(PeerId)
}
