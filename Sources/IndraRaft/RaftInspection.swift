package import Foundation
import IndraNode
import RaftCore

package struct RaftInspection: Codable, Sendable, Equatable {
  package static let commandName = "indra.raft.inspect.v1"

  package enum Query: Codable, Sendable, Equatable {
    case log(before: UInt64?)
    case snapshot(offset: Int, at: UInt64?)
  }

  package let peer: String
  package let group: String
  package let query: Query

  package init(peer: String, group: String, query: Query) {
    self.peer = peer
    self.group = group
    self.query = query
  }
}

package enum RaftInspectionReply: Codable, Sendable, Equatable {
  case log(RaftLogPage)
  case snapshot(RaftSnapshotPage?)
}

package struct RaftLogPage: Codable, Sendable, Equatable {
  package let compactedThrough: UInt64
  package let lastIndex: UInt64
  package let commitIndex: UInt64
  package let appliedIndex: UInt64
  package let entries: [RaftLogRecord]
  package let olderBefore: UInt64?
}

package struct RaftLogRecord: Codable, Sendable, Equatable {
  package enum Operation: Codable, Sendable, Equatable {
    case put(key: String, value: RaftValue)
    case delete(key: String)
    case membership(RaftConfigurationStatus)
    case noOp
  }

  package let index: UInt64
  package let term: UInt64
  package let operation: Operation
}

package struct RaftSnapshotPage: Codable, Sendable, Equatable {
  package let index: UInt64
  package let term: UInt64
  package let configuration: RaftConfigurationStatus?
  package let receiptCount: Int
  package let valueCount: Int
  package let offset: Int
  package let values: [RaftSnapshotValue]
  package let nextOffset: Int?
}

package struct RaftSnapshotValue: Codable, Sendable, Equatable {
  package let key: String
  package let value: RaftValue
}

package enum RaftClient {
  package static func inspect(_ request: RaftInspection, socket: String) async throws -> RaftInspectionReply {
    try await NodeControl.command(
      RaftInspection.commandName, input: request, returning: RaftInspectionReply.self, socket: socket,
      timeout: .seconds(8))
  }

  package static func put(
    key: String, value: RaftValue, group: String, requestID: UUID, socket: String
  ) async throws -> UInt64 {
    let reply = try await NodeControl.command(
      RaftForward.commandName,
      input: RaftForward(peer: nil, group: group, request: .put(key: key, value: value, requestID: requestID)),
      returning: RaftReply.self, socket: socket, timeout: .seconds(10))
    guard case .applied(let result) = reply, result.requestID == requestID else {
      throw IndraError("Unexpected daemon response")
    }
    return result.index
  }
}

extension RaftConfigurationStatus {
  init(membership: ClusterMembership) {
    self.init(
      voters: Array(membership.voters), previousVoters: membership.oldVoters.map(Array.init),
      readers: Array(membership.readers))
  }
}

extension RaftSnapshotPage {
  @concurrent static func read(_ snapshot: RaftSnapshot, offset: Int) async throws -> Self {
    let records = try JSONDecoder().decode([RaftSnapshotValue].self, from: snapshot.applicationState)
    try Task.checkCancellation()
    let start = min(offset, records.count)
    let end = start + min(RaftHost.inspectionPageSize, records.count - start)
    return Self(
      index: snapshot.lastIncludedIndex, term: snapshot.lastIncludedTerm,
      configuration: snapshot.membership.map(RaftConfigurationStatus.init(membership:)),
      receiptCount: snapshot.requests.count, valueCount: records.count, offset: start, values: Array(records[start..<end]),
      nextOffset: end < records.count ? end : nil)
  }
}
