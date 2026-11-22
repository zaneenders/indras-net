import BTree
import Crypto
import Foundation
import RaftCore
import _NIOFileSystem

#if canImport(Darwin)
import Darwin
#elseif canImport(Musl)
import Musl
#else
import Glibc
#endif

enum RaftServiceError: Error, CustomStringConvertible, Equatable {
  case storeInUse
  case wrongReplica
  case invalidState
  case stopped
  case requestConflict
  case discoveringGroup
  case noLeader
  case snapshotTooLarge
  case snapshotChanged
  case commandTooLarge

  var description: String {
    switch self {
    case .storeInUse: "The Raft directory is already in use"
    case .wrongReplica: "The Raft directory belongs to a different node or cluster"
    case .invalidState: "Invalid Raft storage state"
    case .stopped: "The Raft service is stopped"
    case .requestConflict: "The request ID was already used for a different write"
    case .discoveringGroup: "Waiting for Raft group discovery"
    case .noLeader: "No Raft leader with a quorum responded"
    case .snapshotTooLarge: "The Raft snapshot exceeds the supported size"
    case .snapshotChanged: "The replica has a newer snapshot. Press R to reload."
    case .commandTooLarge: "The Raft command exceeds the supported size"
    }
  }
}

enum RaftMutation: Codable, Sendable {
  case put(key: String, value: RaftValue)
  case delete(key: String)

  func encode() throws -> Data {
    let encoder = JSONEncoder()
    encoder.outputFormatting = [.sortedKeys]
    return try encoder.encode(self)
  }
}

struct RaftReceipt: Codable, Sendable {
  let digest: Data
  let index: UInt64
  let removed: Bool?

  func matches(_ command: Data) -> Bool { digest == Data(SHA256.hash(data: command)) }
}

struct RaftStorage: Sendable {
  static let snapshotLimit = 32 * 1024 * 1024
  private static let recordBytes = 1_024

  struct SnapshotContents: Sendable {
    fileprivate let records: [RaftSnapshotValue]
    fileprivate let receipts: [String: RaftReceipt]
  }

  private struct Header: Codable, Sendable {
    let node: String
    let cluster: String
    let term: Term
    let votedFor: PeerId?
    let commitIndex: LogIndex
    let lastIndex: LogIndex
    let baseIndex: LogIndex?
    let initialMembership: ClusterMembership?
    let snapshotBytes: Int?
  }

  private enum Record: DiskBTreeValue {
    static let diskSchemaID = "indras:raft-service:v1"

    case header(Header)
    case log(LogEntry)
    case applied(LogIndex)
    case value(RaftValue)
    case receipt(RaftReceipt)
    case snapshotChunk(Data)

    var appliedIndex: LogIndex? {
      if case .applied(let index) = self { return index }
      return nil
    }

    var storedValue: RaftValue? {
      if case .value(let value) = self { return value }
      return nil
    }

    var storedReceipt: RaftReceipt? {
      if case .receipt(let receipt) = self { return receipt }
      return nil
    }
  }

  private typealias Tree = DiskBTree<String, String, Record>
  private let tree: Tree
  private let lock: ReadWriteFileHandle
  let node: String
  private let cluster: String

  static func open(in directory: FilePath, node: String, cluster: String) async throws -> Self {
    let lock = try await FileSystem.shared.openFile(
      forReadingAndWritingAt: directory.appending("raft.lock"),
      options: .init(
        existingFile: .open, newFile: .init(permissions: .ownerReadWrite, transactionalCreation: false),
        followSymbolicLinks: false, closeOnExec: true))
    do {
      guard try await lock.info().type == .regular else { throw RaftServiceError.invalidState }
      try await lock.withUnsafeDescriptor { fd in
        while flock(fd.rawValue, LOCK_EX | LOCK_NB) != 0 {
          let cause = Errno(rawValue: errno)
          if cause == .interrupted { continue }
          if cause == .wouldBlock { throw RaftServiceError.storeInUse }
          throw cause
        }
      }
      let tree: Tree
      do {
        tree = try await Tree.open(in: directory)
      } catch .storeNotFound {
        tree = try await Tree.create(in: directory)
      }
      return Self(tree: tree, lock: lock, node: node, cluster: cluster)
    } catch {
      try? await withTaskCancellationShield { try await lock.close() }
      throw error
    }
  }

  func close() async throws {
    try await withTaskCancellationShield { try await lock.close() }
  }

  func load() async throws -> (state: PersistentState, applied: LogIndex) {
    try await tree.withReadSnapshot { view in
      let applied = try Self.readApplied(try await view.value(for: "meta", sortKey: "applied"))
      let header: Header
      switch try await view.value(for: "meta", sortKey: "raft") {
      case nil:
        let first = try await view.scan(partition: "log", after: nil, limit: 1)
        guard applied == 0, first.entries.isEmpty else { throw RaftServiceError.invalidState }
        header = Header(
          node: node, cluster: cluster, term: 0, votedFor: nil, commitIndex: 0, lastIndex: 0,
          baseIndex: nil, initialMembership: nil, snapshotBytes: nil)
      case .header(let stored):
        guard stored.node == node, stored.cluster == cluster else { throw RaftServiceError.wrongReplica }
        header = stored
      default:
        throw RaftServiceError.invalidState
      }

      let base = header.baseIndex ?? 0
      guard base <= applied, applied <= header.commitIndex, header.commitIndex <= header.lastIndex,
        header.lastIndex < LogIndex.max
      else { throw RaftServiceError.invalidState }
      var snapshot: RaftSnapshot?
      var log: [LogEntry] = .sentinel
      if let count = header.snapshotBytes {
        guard base > 0, (1...Self.snapshotLimit).contains(count) else { throw RaftServiceError.invalidState }
        var data = Data()
        data.reserveCapacity(count)
        for offset in stride(from: 0, to: count, by: Self.recordBytes) {
          guard case .snapshotChunk(let chunk) = try await view.value(
            for: "snapshot", sortKey: Self.logKey(LogIndex(offset))),
            chunk.count == min(Self.recordBytes, count - offset)
          else { throw RaftServiceError.invalidState }
          data.append(chunk)
        }
        let restored = try JSONDecoder().decode(RaftSnapshot.self, from: data)
        guard restored.lastIncludedIndex == base, let membership = restored.membership else {
          throw RaftServiceError.invalidState
        }
        snapshot = restored
        log = [LogEntry(term: restored.lastIncludedTerm, membership: membership)]
      }
      var after: String?
      repeat {
        let page = try await view.scan(partition: "log", after: after, limit: 128)
        for record in page.entries {
          let index = base + LogIndex(log.count)
          guard index <= header.lastIndex, record.sortKey == Self.logKey(index),
            case .log(let entry) = record.value
          else { throw RaftServiceError.invalidState }
          log.append(entry)
        }
        after = page.nextAfter
      } while after != nil
      guard base + LogIndex(log.count - 1) == header.lastIndex else { throw RaftServiceError.invalidState }
      let state = PersistentState(
        currentTerm: header.term, votedFor: header.votedFor, log: log, commitIndex: header.commitIndex,
        logBaseIndex: base, snapshot: snapshot, initialMembership: header.initialMembership)
      try state.validate(node: node, previouslyValidatedSnapshot: nil)
      return (state, applied)
    }
  }

  func persist(_ state: PersistentState, replacing previous: PersistentState, install: SnapshotContents?) async throws {
    try state.validate(node: node, previouslyValidatedSnapshot: previous.snapshot)
    let base = state.logBaseIndex
    let priorBase = previous.logBaseIndex
    let last = base + LogIndex(state.log.count - 1)
    let priorLast = priorBase + LogIndex(previous.log.count - 1)
    guard base >= priorBase, state.commitIndex >= previous.commitIndex,
      state.currentTerm >= previous.currentTerm,
      state.snapshot?.lastIncludedIndex == (base == 0 ? nil : base)
    else { throw RaftServiceError.invalidState }
    let overlap = max(base, priorBase) + 1
    for index in overlap..<max(overlap, previous.commitIndex + 1) {
      guard index <= last,
        state.log[Int(index - base)] == previous.log[Int(index - priorBase)]
      else { throw RaftServiceError.invalidState }
    }
    let data = try (base == priorBase ? nil : state.snapshot.map(Self.encodeSnapshot))
    if let data, data.count > Self.snapshotLimit { throw RaftServiceError.snapshotTooLarge }
    try await tree.withWriteTransaction { transaction in
      let old = try await transaction.value(for: "meta", sortKey: "raft")
      let priorBytes: Int?
      switch old {
      case nil:
        guard priorLast == 0, previous.currentTerm == 0 else { throw RaftServiceError.invalidState }
        priorBytes = nil
      case .header(let saved):
        guard saved.node == node, saved.cluster == cluster, (saved.baseIndex ?? 0) == priorBase,
          saved.lastIndex == priorLast, saved.term == previous.currentTerm, saved.commitIndex == previous.commitIndex
        else { throw RaftServiceError.invalidState }
        priorBytes = saved.snapshotBytes
      default:
        throw RaftServiceError.invalidState
      }
      if let install {
        guard base > previous.commitIndex else { throw RaftServiceError.invalidState }
        try await Self.clear("data", in: &transaction)
        try await Self.clear("request", in: &transaction)
        for record in install.records {
          try await transaction.updateValue(.value(record.value), for: "data", sortKey: record.key)
        }
        for (id, receipt) in install.receipts {
          try await transaction.updateValue(.receipt(receipt), for: "request", sortKey: id)
        }
        try await transaction.updateValue(.applied(base), for: "meta", sortKey: "applied")
      } else if base > priorBase {
        let applied = try Self.readApplied(try await transaction.value(for: "meta", sortKey: "applied"))
        guard base <= applied else { throw RaftServiceError.invalidState }
      }
      for index in (priorBase + 1)..<(priorLast + 1) where index <= base || index > last {
        try await transaction.removeValue(for: "log", sortKey: Self.logKey(index))
      }
      for index in (base + 1)..<(last + 1) {
        let entry = state.log[Int(index - base)]
        if index > priorLast || previous.log[Int(index - priorBase)] != entry {
          try await transaction.updateValue(.log(entry), for: "log", sortKey: Self.logKey(index))
        }
      }
      if let data {
        try await Self.clear("snapshot", in: &transaction)
        for offset in stride(from: 0, to: data.count, by: Self.recordBytes) {
          let chunk = data.subdata(in: offset..<min(offset + Self.recordBytes, data.count))
          try await transaction.updateValue(.snapshotChunk(chunk), for: "snapshot", sortKey: Self.logKey(LogIndex(offset)))
        }
      }
      let header = Header(
        node: node, cluster: cluster, term: state.currentTerm, votedFor: state.votedFor,
        commitIndex: state.commitIndex, lastIndex: last, baseIndex: base, initialMembership: state.initialMembership,
        snapshotBytes: data?.count ?? priorBytes)
      try await transaction.updateValue(.header(header), for: "meta", sortKey: "raft")
    }
  }

  private static func clear(_ partition: String, in transaction: inout Tree.WriteTransaction) async throws {
    var after: String?
    repeat {
      let page = try await transaction.scan(partition: partition, after: after, limit: 128)
      for entry in page.entries { try await transaction.removeValue(for: partition, sortKey: entry.sortKey) }
      after = page.nextAfter
    } while after != nil
  }

  static func encodeSnapshot(_ snapshot: RaftSnapshot) throws -> Data {
    let encoder = JSONEncoder()
    encoder.outputFormatting = [.sortedKeys]
    return try encoder.encode(snapshot)
  }

  func makeSnapshot(index: LogIndex, term: Term, membership: ClusterMembership) async throws -> RaftSnapshot {
    try await tree.withReadSnapshot { view in
      guard try Self.readApplied(try await view.value(for: "meta", sortKey: "applied")) == index else {
        throw RaftServiceError.invalidState
      }
      var records: [RaftSnapshotValue] = []
      var receipts: [AppliedRequest] = []
      let encoder = JSONEncoder()
      encoder.outputFormatting = [.sortedKeys]
      var estimatedBytes = 0
      var after: String?
      repeat {
        let page = try await view.scan(partition: "data", after: after, limit: 128)
        for record in page.entries {
          guard let value = record.value.storedValue else { throw RaftServiceError.invalidState }
          estimatedBytes += record.sortKey.utf8.count + (try encoder.encode(value)).count + 16
          guard estimatedBytes <= Self.snapshotLimit else { throw RaftServiceError.snapshotTooLarge }
          records.append(RaftSnapshotValue(key: record.sortKey, value: value))
        }
        after = page.nextAfter
      } while after != nil
      repeat {
        let page = try await view.scan(partition: "request", after: after, limit: 128)
        for record in page.entries {
          guard let receipt = record.value.storedReceipt else { throw RaftServiceError.invalidState }
          let result = try encoder.encode(receipt)
          estimatedBytes += record.sortKey.utf8.count + result.count + 32
          guard estimatedBytes <= Self.snapshotLimit else { throw RaftServiceError.snapshotTooLarge }
          receipts.append(AppliedRequest(
            identity: RequestIdentity(client: record.sortKey, requestId: 0), index: receipt.index, result: result))
        }
        after = page.nextAfter
      } while after != nil
      return RaftSnapshot(
        lastIncludedIndex: index, lastIncludedTerm: term, applicationState: try encoder.encode(records),
        requests: receipts, membership: membership)
    }
  }

  static func contents(of snapshot: RaftSnapshot) throws -> SnapshotContents {
    try snapshot.validate()
    let records = try JSONDecoder().decode([RaftSnapshotValue].self, from: snapshot.applicationState)
    guard Set(records.map(\.key)).count == records.count else { throw RaftServiceError.invalidState }
    for record in records { try Tree.validateRecord(.value(record.value), for: "data", sortKey: record.key) }
    var receipts: [String: RaftReceipt] = [:]
    for request in snapshot.requests {
      let receipt = try JSONDecoder().decode(RaftReceipt.self, from: request.result)
      guard request.identity.requestId == 0, let id = UUID(uuidString: request.identity.client),
        receipt.index == request.index, receipt.digest.count == 32,
        receipts.updateValue(receipt, forKey: id.uuidString) == nil
      else { throw RaftServiceError.invalidState }
    }
    return SnapshotContents(records: records, receipts: receipts)
  }

  static func validate(_ mutation: RaftMutation, entry: LogEntry, at index: LogIndex) throws {
    switch mutation {
    case .put(let key, let value):
      try Tree.validateRecord(.value(value), for: "data", sortKey: key)
    case .delete(let key):
      try Tree.validateRecord(.value(.string("")), for: "data", sortKey: key)
    }
    try validateLog(entry, index: index)
  }

  static func validateLog(_ entry: LogEntry, index: LogIndex) throws {
    try Tree.validateRecord(.log(entry), for: "log", sortKey: logKey(index))
  }

  func apply(
    _ entry: LogEntry, at index: LogIndex, beforeCommit: (@Sendable () throws -> Void)?
  ) async throws -> RaftReceipt? {
    try await tree.withWriteTransaction { transaction in
      let header = try await transaction.value(for: "meta", sortKey: "raft")
      let persisted = try await transaction.value(for: "log", sortKey: Self.logKey(index))
      guard case .header(let state) = header, index <= state.commitIndex,
        case .log(let saved) = persisted, saved == entry
      else { throw RaftServiceError.invalidState }
      let applied = try Self.readApplied(try await transaction.value(for: "meta", sortKey: "applied"))
      guard index <= applied + 1 else { throw RaftServiceError.invalidState }
      let receipt: RaftReceipt?
      if entry.isNoOp || entry.membership != nil {
        receipt = nil
      } else {
        guard let request = entry.request, request.requestId == 0, let id = UUID(uuidString: request.client) else {
          throw RaftServiceError.invalidState
        }
        let saved = try await transaction.value(for: "request", sortKey: id.uuidString)
        if let saved {
          guard let prior = saved.storedReceipt, prior.index <= applied else { throw RaftServiceError.invalidState }
          guard prior.matches(entry.command) else { throw RaftServiceError.requestConflict }
          receipt = prior
        } else {
          guard index > applied else { throw RaftServiceError.invalidState }
          let mutation = try JSONDecoder().decode(RaftMutation.self, from: entry.command)
          let removed: Bool?
          switch mutation {
          case .put(let key, let value):
            try await transaction.updateValue(.value(value), for: "data", sortKey: key)
            removed = nil
          case .delete(let key):
            removed = try await transaction.removeValue(for: "data", sortKey: key) != nil
          }
          let result = RaftReceipt(digest: Data(SHA256.hash(data: entry.command)), index: index, removed: removed)
          try await transaction.updateValue(.receipt(result), for: "request", sortKey: id.uuidString)
          receipt = result
        }
      }
      if index > applied {
        try await transaction.updateValue(.applied(index), for: "meta", sortKey: "applied")
        try beforeCommit?()
      }
      return receipt
    }
  }

  func value(for key: String) async throws -> RaftValue? {
    guard let record = try await tree.value(for: "data", sortKey: key) else { return nil }
    guard let value = record.storedValue else { throw RaftServiceError.invalidState }
    return value
  }

  func receipt(for id: UUID) async throws -> RaftReceipt? {
    guard let record = try await tree.value(for: "request", sortKey: id.uuidString) else { return nil }
    guard let receipt = record.storedReceipt else { throw RaftServiceError.invalidState }
    return receipt
  }

  private static func readApplied(_ record: Record?) throws -> LogIndex {
    guard let record else { return 0 }
    guard let index = record.appliedIndex, index < LogIndex.max else { throw RaftServiceError.invalidState }
    return index
  }

  private static func logKey(_ index: LogIndex) -> String {
    let digits = String(index, radix: 16, uppercase: false)
    return String(repeating: "0", count: 16 - digits.utf8.count) + digits
  }
}
