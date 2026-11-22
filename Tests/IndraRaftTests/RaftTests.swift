import BTree
import Foundation
import OrderedCollections
import RaftCore
import Testing
import _NIOFileSystem

@testable import IndraRaft

#if canImport(Darwin)
import Darwin
#elseif canImport(Musl)
import Musl
#else
import Glibc
#endif

struct RaftTests {
  private static let node = "node"
  private static let cluster = "cluster"

  @Test func dictionariesRoundTripAndTextKeepsItsEncoding() throws {
    let json = #"{"count":18446744073709551615,"list":[null,true,1.25],"nested":{"name":"Zane"}}"#
    let document = try RaftValue(documentJSON: json)
    #expect(document == .document([
      "count": .uint64(UInt64.max), "list": .list([.null, .bool(true), .float64(1.25)]),
      "nested": .map(["name": .string("Zane")]),
    ]))
    #expect(try document.formatted(asJSON: true) == json)
    #expect(try RaftValue.string(json).formatted(asJSON: false) == json)
    #expect(try RaftValue.string(json).formatted(asJSON: true) != json)
    #expect(throws: DecodingError.self) { try RaftValue(documentJSON: #"["array"]"#) }

    let first = try RaftMutation.put(key: "legacy", value: .string("before")).encode()
    #expect(first == Data(#"{"put":{"key":"legacy","value":"before"}}"#.utf8))
    let second = try RaftValue(documentJSON: #"{"nested":{"name":"Zane"},"list":[null,true,1.25],"count":18446744073709551615}"#)
    #expect(try RaftMutation.put(key: "document", value: second).encode() ==
      RaftMutation.put(key: "document", value: document).encode())
  }

  @Test func discoveryWithAnEmptyStoreWaitsForAnExistingGroup() async throws {
    try await FileSystem.shared.withTemporaryDirectory(
      prefix: nil, options: .init(followSymbolicLinks: false, closeOnExec: true)
    ) { _, path in
      await #expect(throws: RaftServiceError.discoveringGroup) {
        let host = try await RaftHost.open(in: path, node: Self.node, cluster: Self.cluster, mode: .discover)
        await host.stop()
      }
      let storage = try await RaftStorage.open(in: path, node: Self.node, cluster: Self.cluster)
      defer { try? await storage.close() }
      let initial = try await storage.load()
      #expect(initial.state.currentTerm == 0)
      #expect(initial.state.initialMembership == nil)
    }
  }

  @Test func replacesAnUncommittedSuffixWithoutChangingCommittedEntries() async throws {
    try await FileSystem.shared.withTemporaryDirectory(
      prefix: nil, options: .init(followSymbolicLinks: false, closeOnExec: true)
    ) { _, path in
      let storage = try await RaftStorage.open(in: path, node: Self.node, cluster: Self.cluster)
      defer { try? await storage.close() }
      let empty = try await storage.load().state
      let committed = LogEntry(term: 1, command: Data(), request: nil, isNoOp: true)
      let old = PersistentState(
        currentTerm: 1, votedFor: Self.node, log: .sentinel + [committed, committed, committed],
        commitIndex: 1, logBaseIndex: 0, snapshot: nil, initialMembership: nil)
      try await storage.persist(old, replacing: empty, install: nil)
      _ = try await storage.apply(committed, at: 1, beforeCommit: nil)

      let replacement = LogEntry(term: 2, command: Data(), request: nil, isNoOp: true)
      let repaired = PersistentState(
        currentTerm: 2, votedFor: nil, log: .sentinel + [committed, replacement],
        commitIndex: 1, logBaseIndex: 0, snapshot: nil, initialMembership: nil)
      try await storage.persist(repaired, replacing: old, install: nil)
      let conflicting = PersistentState(
        currentTerm: 2, votedFor: nil, log: .sentinel + [replacement, replacement],
        commitIndex: 1, logBaseIndex: 0, snapshot: nil, initialMembership: nil)
      await #expect(throws: RaftServiceError.invalidState) {
        try await storage.persist(conflicting, replacing: repaired, install: nil)
      }
      let loaded = try await storage.load()
      #expect(loaded.state == repaired)
      #expect(loaded.applied == 1)
    }
  }

  @Test
  func restartPreservesApplicationAndWriteReceipts() async throws {
    try await FileSystem.shared.withTemporaryDirectory(
      prefix: nil, options: .init(followSymbolicLinks: false, closeOnExec: true)
    ) { _, path in
      let put = RaftRequest.put(key: "key", value: .string("first"), requestID: UUID())
      let deletion = RaftRequest.delete(key: "key", requestID: UUID())
      let host = try await RaftHost.open(in: path, node: Self.node, cluster: Self.cluster, mode: .bootstrap)
      defer { await host.stop() }
      let first = try await host.execute(put)
      #expect(try await host.execute(.get(key: "key")) == .value(.string("first")))
      let deleted = try await host.execute(deletion)
      guard case .applied(let receipt) = deleted else {
        Issue.record("Expected a deletion receipt")
        return
      }
      #expect(receipt.removed == true)
      #expect(try await host.execute(.get(key: "key")) == .value(nil))
      _ = try await host.execute(.put(key: "key", value: .string("second"), requestID: UUID()))
      await host.stop()

      let reopened = try await RaftHost.open(in: path, node: Self.node, cluster: Self.cluster, mode: .discover)
      defer { await reopened.stop() }
      #expect(try await reopened.execute(put) == first)
      #expect(try await reopened.execute(deletion) == deleted)
      #expect(try await reopened.execute(.get(key: "key")) == .value(.string("second")))
      await #expect(throws: RaftServiceError.requestConflict) {
        try await reopened.execute(.delete(key: "different", requestID: receipt.requestID))
      }
      let status = try await Self.status(of: reopened)
      #expect(status.term == 2)
      #expect(status.commitIndex == 3)
      #expect(status.appliedIndex == 3)
      #expect(status.voters == 1)
    }
  }

  @Test(arguments: [false, true])
  func replaysACommittedDeleteAfterProcessExit(duringApply: Bool) async throws {
    try await FileSystem.shared.withTemporaryDirectory(
      prefix: nil, options: .init(followSymbolicLinks: false, closeOnExec: true)
    ) { _, path in
      let host = try await RaftHost.open(in: path, node: Self.node, cluster: Self.cluster, mode: .bootstrap)
      defer { await host.stop() }
      _ = try await host.execute(.put(key: "key", value: .string("value"), requestID: UUID()))
      await host.stop()

      let request = UUID()
      let storePath = path.string
      try await #require(processExitsWith: .exitCode(42)) {
        [storePath = storePath as String, request = request as UUID, duringApply = duringApply as Bool] in
        let storage = try await RaftStorage.open(in: FilePath(storePath), node: Self.node, cluster: Self.cluster)
        let (state, applied) = try await storage.load()
        var instance = Instance(
          id: Self.node, peers: [], role: .follower, currentTerm: state.currentTerm, votedFor: state.votedFor,
          votes: [:], commitIndex: state.commitIndex, lastApplied: applied, log: state.log, logBaseIndex: 0,
          snapshot: nil, initialMembership: nil, timing: .default, rng: SystemRandomNumberGenerator())
        _ = instance.onTimerTick()
        let elected = instance.persistentState
        try await storage.persist(elected, replacing: state, install: nil)
        let command = try RaftMutation.delete(key: "key").encode()
        _ = instance.receiveClientSubmit(request.uuidString, .init(requestId: 0, command: command, traceHeaders: [:]))
        try await storage.persist(instance.persistentState, replacing: elected, install: nil)
        if !duringApply { _exit(42) }
        let entry = try #require(instance.entry(at: instance.commitIndex))
        _ = try await storage.apply(entry, at: instance.commitIndex, beforeCommit: { _exit(42) })
      }

      let beforeReplay = try await RaftStorage.open(in: path, node: Self.node, cluster: Self.cluster)
      do {
        let (state, applied) = try await beforeReplay.load()
        #expect(state.commitIndex == 2)
        #expect(applied == 1)
        #expect(try await beforeReplay.value(for: "key") == .string("value"))
        #expect(try await beforeReplay.receipt(for: request) == nil)
      }
      try await beforeReplay.close()

      let reopened = try await RaftHost.open(in: path, node: Self.node, cluster: Self.cluster, mode: .bootstrap)
      defer { await reopened.stop() }
      #expect(try await reopened.execute(.get(key: "key")) == .value(nil))
      let status = try await Self.status(of: reopened)
      #expect(status.commitIndex == 2)
      #expect(status.appliedIndex == 2)
      let retry = try await reopened.execute(.delete(key: "key", requestID: request))
      #expect(retry == .applied(RaftWriteResult(requestID: request, index: 2, removed: true)))
    }
  }

  @Test
  func serializesConcurrentWrites() async throws {
    try await FileSystem.shared.withTemporaryDirectory(
      prefix: nil, options: .init(followSymbolicLinks: false, closeOnExec: true)
    ) { _, path in
      let host = try await RaftHost.open(in: path, node: Self.node, cluster: Self.cluster, mode: .bootstrap)
      defer { await host.stop() }
      let count: UInt64 = 16
      let indexes = try await withThrowingTaskGroup(of: UInt64.self, returning: Set<UInt64>.self) { tasks in
        for number in 1...count {
          tasks.addTask {
            let reply = try await host.execute(.put(key: String(number), value: .string("value"), requestID: UUID()))
            guard case .applied(let receipt) = reply else { throw RaftServiceError.invalidState }
            return receipt.index
          }
        }
        var indexes = Set<UInt64>()
        for try await index in tasks { indexes.insert(index) }
        return indexes
      }
      #expect(indexes == Set(1...count))
      for number in 1...count {
        #expect(try await host.execute(.get(key: String(number))) == .value(.string("value")))
      }
      let status = try await Self.status(of: host)
      #expect(status.commitIndex == count)
      #expect(status.appliedIndex == count)
    }
  }

  @Test
  func rejectsOversizeWriteBeforeItEntersTheLog() async throws {
    try await FileSystem.shared.withTemporaryDirectory(
      prefix: nil, options: .init(followSymbolicLinks: false, closeOnExec: true)
    ) { _, path in
      let host = try await RaftHost.open(in: path, node: Self.node, cluster: Self.cluster, mode: .bootstrap)
      defer { await host.stop() }
      let id = UUID()
      await #expect(throws: DiskBTreeError.self) {
        try await host.execute(.put(key: "key", value: .string(String(repeating: "A", count: 100_000)), requestID: id))
      }
      let initial = try await Self.status(of: host)
      #expect(initial.commitIndex == 0)
      #expect(initial.appliedIndex == 0)
      let reply = try await host.execute(.put(key: "key", value: .string("valid"), requestID: id))
      #expect(reply == .applied(RaftWriteResult(requestID: id, index: 1, removed: nil)))
    }
  }

  @Test
  func storeAllowsOnlyOneOwnerAndBindsToNodeAndCluster() async throws {
    try await FileSystem.shared.withTemporaryDirectory(
      prefix: nil, options: .init(followSymbolicLinks: false, closeOnExec: true)
    ) { _, path in
      let host = try await RaftHost.open(in: path, node: Self.node, cluster: Self.cluster, mode: .bootstrap)
      defer { await host.stop() }
      await #expect(throws: RaftServiceError.storeInUse) {
        let second = try await RaftHost.open(in: path, node: Self.node, cluster: Self.cluster, mode: .bootstrap)
        await second.stop()
      }
      await host.stop()
      for (node, cluster) in [("other", Self.cluster), (Self.node, "other")] {
        await #expect(throws: RaftServiceError.wrongReplica) {
          let second = try await RaftHost.open(in: path, node: node, cluster: cluster, mode: .bootstrap)
          await second.stop()
        }
      }
      let reopened = try await RaftHost.open(in: path, node: Self.node, cluster: Self.cluster, mode: .bootstrap)
      await reopened.stop()
    }
  }

  private static func status(of host: RaftHost) async throws -> RaftStatus {
    guard case .status(let status) = try await host.execute(.status) else { throw RaftServiceError.invalidState }
    return status
  }
}
