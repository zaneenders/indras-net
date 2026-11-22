public import Foundation

public enum InstallSnapshot: Sendable {
  public struct Args: Codable, Equatable, Sendable {
    public let term: Term
    public let rpcId: UInt64
    public let leaderId: PeerId
    public let lastIncludedIndex: LogIndex
    public let lastIncludedTerm: Term
    public let offset: UInt64
    public let totalLength: UInt64
    /// The shell verifies this digest; the transition core treats it as opaque.
    public let checksum: Data
    public let data: Data

    public static let checksumLength = 32
    public static func basePayloadLength(leaderId: PeerId) -> Int { 56 + checksumLength + leaderId.utf8.count }

    public init(
      term: Term, rpcId: UInt64, leaderId: PeerId, lastIncludedIndex: LogIndex,
      lastIncludedTerm: Term, offset: UInt64, totalLength: UInt64, checksum: Data, data: Data
    ) {
      self.term = term
      self.rpcId = rpcId
      self.leaderId = leaderId
      self.lastIncludedIndex = lastIncludedIndex
      self.lastIncludedTerm = lastIncludedTerm
      self.offset = offset
      self.totalLength = totalLength
      self.checksum = checksum
      self.data = data
    }
  }

  public struct Reply: Codable, Equatable, Sendable {
    public let term: Term
    public let rpcId: UInt64
    /// Echoed request boundary, checked before the leader credits replication.
    public let lastIncludedIndex: LogIndex
    public let nextOffset: UInt64
    public let installed: Bool

    public init(term: Term, rpcId: UInt64, lastIncludedIndex: LogIndex, nextOffset: UInt64, installed: Bool) {
      self.term = term
      self.rpcId = rpcId
      self.lastIncludedIndex = lastIncludedIndex
      self.nextOffset = nextOffset
      self.installed = installed
    }
  }
}
