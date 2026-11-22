public import Foundation

public struct AppliedRequest: Codable, Equatable, Sendable {
  public let identity: RequestIdentity
  public let index: LogIndex
  public let result: Data

  public init(identity: RequestIdentity, index: LogIndex, result: Data) {
    self.identity = identity
    self.index = index
    self.result = result
  }
}

/// Application state and retry results at one committed log boundary.
public struct RaftSnapshot: Codable, Equatable, Sendable {
  public let version: Int
  public let lastIncludedIndex: LogIndex
  public let lastIncludedTerm: Term
  public let applicationState: Data
  public let requests: [AppliedRequest]
  public let membership: ClusterMembership?

  public init(
    lastIncludedIndex: LogIndex, lastIncludedTerm: Term, applicationState: Data, requests: [AppliedRequest],
    membership: ClusterMembership?
  ) {
    self.version = 1
    self.lastIncludedIndex = lastIncludedIndex
    self.lastIncludedTerm = lastIncludedTerm
    self.applicationState = applicationState
    self.requests = requests
    self.membership = membership
  }

  public func validate() throws {
    guard version == 1, lastIncludedIndex > 0, lastIncludedIndex < LogIndex.max, lastIncludedTerm > 0 else {
      throw StateValidationError.invalidState
    }
    try membership?.validate()
    var identities: Set<RequestIdentity> = []
    for request in requests {
      guard request.index > 0, request.index <= lastIncludedIndex,
        request.result.count <= Int(RaftLimits.maximumPayloadLength) - 35,
        identities.insert(request.identity).inserted
      else { throw StateValidationError.invalidState }
    }
  }
}
