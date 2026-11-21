import BTree
import _NIOFileSystem

enum Workload: String, CaseIterable, Sendable {
  case insert
  case lookup
  case scan
  case delete

  var changesTree: Bool {
    self == .insert || self == .delete
  }
}

struct Benchmark: Sendable {
  typealias Tree = DiskBTree<Int, Int, Int>

  struct Result: Sendable {
    let operations: Int
    let checksum: Int?
  }

  private struct Record: Sendable {
    let partition: Int
    let sortKey: Int
    let value: Int
  }

  private let records: [Record]
  private let partitionCount: Int
  private let scanLimit: Int
  private let directory: FilePath
  private let checksum: Int

  init(options: BenchmarkOptions) {
    partitionCount = options.partitions
    scanLimit = options.scanLimit
    directory = FilePath(options.directory)
    var rows = (0..<options.entries).map { id in
      Record(partition: id % options.partitions, sortKey: id / options.partitions, value: id)
    }
    var random = SplitMix64(state: 74)
    rows.shuffle(using: &random)
    records = rows
    checksum = rows.reduce(0) { $0 &+ $1.value }
  }

  func withPreparedStore<Output: Sendable>(
    for workload: Workload, execute: @Sendable (Tree) async throws -> Output
  ) async throws -> Output {
    try await FileSystem.shared.withTemporaryDirectory(
      prefix: directory, options: .init(followSymbolicLinks: false, closeOnExec: true)
    ) { _, path in
      let tree = try await Tree.create(in: path)
      if workload != .insert {
        try await tree.withWriteTransaction { transaction in
          try await insert(into: &transaction)
        }
      }
      return try await execute(tree)
    }
  }

  func run(_ workload: Workload, in tree: Tree) async throws -> Result {
    switch workload {
    case .insert:
      try await tree.withWriteTransaction { transaction in
        try await insert(into: &transaction)
      }
      return Result(operations: records.count, checksum: nil)
    case .lookup:
      let checksum = try await lookup(in: tree)
      return Result(operations: records.count, checksum: checksum)
    case .scan:
      return try await scan(in: tree)
    case .delete:
      let checksum = try await tree.withWriteTransaction { transaction in
        try await delete(from: &transaction)
      }
      return Result(operations: records.count, checksum: checksum)
    }
  }

  func check(_ result: Result, for workload: Workload, in tree: Tree) async throws {
    if workload == .insert {
      let witness = records[records.count / 2]
      let value = try await tree.value(for: witness.partition, sortKey: witness.sortKey)
      precondition(value == witness.value)
    } else {
      precondition(result.checksum == checksum, "\(workload.rawValue) returned unexpected values")
    }
  }

  @inline(never)
  private func insert(into transaction: inout Tree.WriteTransaction) async throws(DiskBTreeError) {
    for record in records {
      try await transaction.updateValue(record.value, for: record.partition, sortKey: record.sortKey)
    }
  }

  @inline(never)
  private func lookup(in tree: Tree) async throws -> Int {
    var checksum = 0
    for record in records.reversed() {
      checksum &+= try await tree.value(for: record.partition, sortKey: record.sortKey) ?? -1
    }
    return checksum
  }

  @inline(never)
  private func scan(in tree: Tree) async throws -> Result {
    try await tree.withReadSnapshot { snapshot in
      var checksum = 0
      var scans = 0
      for partition in 0..<partitionCount {
        var cursor: Int?
        var remaining = (records.count - 1 - partition) / partitionCount + 1
        while remaining > 0 {
          let page = try await snapshot.scan(partition: partition, after: cursor, limit: scanLimit)
          let expectedCount = min(scanLimit, remaining)
          precondition(page.entries.count == expectedCount, "Scan returned an unexpected number of records")
          for entry in page.entries {
            checksum &+= entry.value
          }
          scans += 1
          remaining -= expectedCount
          cursor = page.nextAfter
          precondition((cursor != nil) == (remaining > 0), "Scan returned an unexpected cursor")
        }
      }
      return Result(operations: scans, checksum: checksum)
    }
  }

  @inline(never)
  private func delete(from transaction: inout Tree.WriteTransaction) async throws(DiskBTreeError) -> Int {
    var checksum = 0
    for record in records.reversed() {
      checksum &+= try await transaction.removeValue(for: record.partition, sortKey: record.sortKey) ?? -1
    }
    return checksum
  }
}

struct SplitMix64: RandomNumberGenerator {
  var state: UInt64

  mutating func next() -> UInt64 {
    state &+= 0x9e37_79b9_7f4a_7c15
    var value = state
    value = (value ^ (value >> 30)) &* 0xbf58_476d_1ce4_e5b9
    value = (value ^ (value >> 27)) &* 0x94d0_49bb_1331_11eb
    return value ^ (value >> 31)
  }
}
