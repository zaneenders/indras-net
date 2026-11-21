import ArgumentParser
import BTree
import Foundation
import _NIOFileSystem

struct Stress: AsyncParsableCommand {
  static let configuration = CommandConfiguration(
    abstract: "Stress a file index with concurrent readers and a writer.")

  @Option(help: "Number of documents to seed.") var entries = 100_000
  @Option(help: "Number of timed operations, in multiples of 20.") var operations = 20_000
  @Option(help: "Number of concurrent reader tasks.") var readers = 4
  @Option(help: "Maximum mutations per write transaction.") var writeBatchSize = 8
  @Option(help: "Maximum records per scan page.") var scanLimit = 32
  @Option(help: "Seed for document order and key selection.") var seed: UInt64 = 74
  @Option(help: "Existing directory in which to create the temporary store.") var directory = "."

  func validate() throws {
    guard entries >= 5 else { throw ValidationError("Seed at least five documents.") }
    guard operations > 0, operations.isMultiple(of: 20) else {
      throw ValidationError("Use a positive operation count in multiples of 20.")
    }
    guard entries <= Int.max - operations / 20 else { throw ValidationError("Too many document IDs.") }
    guard readers > 0, writeBatchSize > 0, scanLimit > 0 else {
      throw ValidationError("Use positive reader, write batch, and scan limit values.")
    }
    guard !directory.isEmpty else { throw ValidationError("Provide a directory path.") }
  }

  func run() async throws {
    try await FileIndexStress(
      entries: entries, rounds: operations / 20, readers: min(readers, operations / 20),
      writeBatchSize: writeBatchSize, scanLimit: scanLimit, seed: seed, directory: FilePath(directory)
    ).run()
  }
}

private struct FileIndexStress: Sendable {
  private typealias Tree = DiskBTree<String, String, Document>

  private struct Key: Sendable {
    let id: Int
    let partition: String
    let sortKey: String

    var attributeID: AttributeValue { .uint64(UInt64(id)) }

    func document(enlarged: Bool) -> Document {
      let noteSizes = [64, 64, 64, 64, 200, 200, 360, 360, 760, 1_200]
      let noteSize = enlarged ? 1_300 : noteSizes[id % noteSizes.count]
      return [
        "id": attributeID,
        "name": .string(sortKey),
        "size": .uint64(UInt64(id + 1) &* 19_137),
        "contentHash": .bytes(Data(repeating: UInt8(truncatingIfNeeded: id), count: 32)),
        "modified": .uint64(1_795_100_000 + UInt64(id) % 31_536_000),
        "details": .map([
          "mime": .string("text/plain"),
          "verified": .bool(id.isMultiple(of: 3)),
          "tags": .list([.string("shared"), .string("indexed")])
        ]),
        "notes": .string(String(repeating: enlarged ? "y" : "x", count: noteSize))
      ]
    }
  }

  private enum Mutation: Sendable {
    case insert(Key, Document)
    case replace(Key, Document)
    case delete(Key)
  }

  let entries: Int
  let rounds: Int
  let readers: Int
  let writeBatchSize: Int
  let scanLimit: Int
  let seed: UInt64
  let directory: FilePath

  private var hotEntries: Int { entries - entries / 5 * 2 }
  private var churnEntries: Int { min(rounds, max(1, (entries - hotEntries) / 2)) }

  func run() async throws {
    print(
      "entries=\(entries) operations=\(rounds * 20) readers=\(readers) write_batch_size=\(writeBatchSize) "
        + "scan_limit=\(scanLimit) seed=\(seed) directory=\(directory)")
    let mutations = makeMutations()
    try await FileSystem.shared.withTemporaryDirectory(
      prefix: directory, options: .init(followSymbolicLinks: false, closeOnExec: true)
    ) { _, path in
      let tree = try await Tree.create(in: path)
      try await seedStore(tree)
      let seededSize = try await pagesSize(in: path)
      let removed = key(for: entries - churnEntries)
      let inserted = key(for: entries + rounds - 1)

      let start = ContinuousClock.now
      let metrics = try await tree.withReadSnapshot { pinned in
        let original = try await pinned.value(for: removed.partition, sortKey: removed.sortKey)
        guard original?["id"] == removed.attributeID else { throw ValidationError("Missing snapshot witness.") }
        let metrics = try await withThrowingTaskGroup(of: StressMetrics.self) { group in
          group.addTask { try await self.write(mutations, in: tree) }
          for reader in 0..<readers {
            group.addTask { try await self.read(worker: reader, in: tree) }
          }
          var combined = StressMetrics()
          for try await result in group { combined.merge(result) }
          return combined
        }
        let previous = try await pinned.value(for: removed.partition, sortKey: removed.sortKey)
        guard previous == original else { throw ValidationError("The pinned snapshot changed during the workload.") }
        return metrics
      }
      let elapsed = start.duration(to: .now)

      let reopened = try await Tree.open(in: path)
      let latestInsert = try await reopened.value(for: inserted.partition, sortKey: inserted.sortKey)
      let latestRemoval = try await reopened.value(for: removed.partition, sortKey: removed.sortKey)
      guard latestInsert?["id"] == inserted.attributeID, latestRemoval == nil else {
        throw ValidationError("The reopened index has unexpected values.")
      }

      let finalSize = try await pagesSize(in: path)
      print("pages_file_bytes_before=\(seededSize) pages_file_bytes_after=\(finalSize)")
      let seconds = elapsed / .seconds(1)
      let elapsedText = String(format: "%.3f", locale: Locale(identifier: "en_US_POSIX"), seconds)
      let rate = StressMetrics.format(Double(rounds * 20) / seconds)
      print("elapsed_s=\(elapsedText) operations_per_s=\(rate)")
      metrics.report()
    }
  }

  private func key(for id: Int) -> Key {
    let account = id < hotEntries ? id % 5 : 5 + id % 95
    let segment = ["projects", "café", "archive", "photos"][id % 4]
    return Key(id: id, partition: "account-\(account)", sortKey: "\(segment)/\(id % 31)/\(id)-reference-material.txt")
  }

  private func existingID(using random: inout SplitMix64) -> Int {
    let useHotAccount = Int.random(in: 0..<10, using: &random) < 8
    let indices = useHotAccount ? 0..<hotEntries : hotEntries..<(entries - churnEntries)
    return Int.random(in: indices, using: &random)
  }

  private func seedStore(_ tree: Tree) async throws {
    var random = SplitMix64(state: seed)
    let ids = (0..<entries).shuffled(using: &random)
    for start in stride(from: 0, to: entries, by: 256) {
      let batch = ids[start..<(start + min(256, entries - start))]
      try await tree.withWriteTransaction { transaction in
        for id in batch {
          let key = self.key(for: id)
          try await transaction.updateValue(key.document(enlarged: false), for: key.partition, sortKey: key.sortKey)
        }
      }
    }
  }

  private func makeMutations() -> [Mutation] {
    var random = SplitMix64(state: seed ^ 0x9e37_79b9_7f4a_7c15)
    var enlarged: Set<Int> = []
    var mutations: [Mutation] = []
    mutations.reserveCapacity(rounds * 4)
    for round in 0..<rounds {
      for _ in 0..<2 {
        let id = existingID(using: &random)
        let key = key(for: id)
        let grows = enlarged.insert(id).inserted
        if !grows { enlarged.remove(id) }
        mutations.append(.replace(key, key.document(enlarged: grows)))
      }
      let incoming = key(for: entries + round)
      mutations.append(.insert(incoming, incoming.document(enlarged: false)))
      mutations.append(.delete(key(for: entries - churnEntries + round)))
    }
    return mutations
  }

  private func read(worker: Int, in tree: Tree) async throws -> StressMetrics {
    var metrics = StressMetrics()
    for round in stride(from: worker, to: rounds, by: readers) {
      var random = SplitMix64(state: seed &+ UInt64(round))
      let miss = key(for: existingID(using: &random))
      let missingSortKey = miss.sortKey + ".missing"
      let hits = (0..<12).map { _ in key(for: existingID(using: &random)) }
      let partition = key(for: Int.random(in: 0..<hotEntries, using: &random)).partition
      let sample = try await tree.withReadSnapshot { snapshot in
        var results = StressMetrics()
        let missStart = ContinuousClock.now
        let missing = try await snapshot.value(for: miss.partition, sortKey: missingSortKey)
        let missDuration = missStart.duration(to: .now)
        guard missing == nil else { throw ValidationError("A miss returned a document.") }
        results.record(.lookupMiss, duration: missDuration, pageWrites: nil)
        for key in hits {
          let start = ContinuousClock.now
          let document = try await snapshot.value(for: key.partition, sortKey: key.sortKey)
          let duration = start.duration(to: .now)
          guard document?["id"] == key.attributeID else { throw ValidationError("A hit returned the wrong document.") }
          results.record(.lookupHit, duration: duration, pageWrites: nil)
        }

        var cursor: String?
        for _ in 0..<3 {
          let start = ContinuousClock.now
          let page = try await snapshot.scan(partition: partition, after: cursor, limit: scanLimit)
          let duration = start.duration(to: .now)
          if let cursor, let first = page.entries.first, first.sortKey <= cursor {
            throw ValidationError("A scan repeated its cursor.")
          }
          results.record(.scan, duration: duration, pageWrites: nil)
          results.scannedRows += page.entries.count
          cursor = page.nextAfter
        }
        return results
      }
      metrics.merge(sample)
    }
    return metrics
  }

  private func write(_ mutations: [Mutation], in tree: Tree) async throws -> StressMetrics {
    var metrics = StressMetrics()
    for offset in stride(from: 0, to: mutations.count, by: writeBatchSize) {
      let batch = mutations[offset..<(offset + min(writeBatchSize, mutations.count - offset))]
      let start = ContinuousClock.now
      let sample = try await tree.withWriteTransaction { transaction in
        var results = StressMetrics()
        for mutation in batch {
          let before = transaction.pageWriteCount
          let start = ContinuousClock.now
          let operation: StressMetrics.Operation
          let previous: Document?
          let expected: AttributeValue?
          switch mutation {
          case .insert(let key, let value):
            operation = .insert
            expected = nil
            previous = try await transaction.updateValue(value, for: key.partition, sortKey: key.sortKey)
          case .replace(let key, let value):
            operation = .update
            expected = key.attributeID
            previous = try await transaction.updateValue(value, for: key.partition, sortKey: key.sortKey)
          case .delete(let key):
            operation = .delete
            expected = key.attributeID
            previous = try await transaction.removeValue(for: key.partition, sortKey: key.sortKey)
          }
          let duration = start.duration(to: .now)
          guard previous?["id"] == expected else { throw ValidationError("A mutation returned the wrong document.") }
          results.record(operation, duration: duration, pageWrites: transaction.pageWriteCount - before)
        }
        return results
      }
      let duration = start.duration(to: .now)
      metrics.record(.transaction, duration: duration, pageWrites: sample.pageWrites.values.reduce(0, +))
      metrics.merge(sample)
    }
    return metrics
  }

  private func pagesSize(in path: FilePath) async throws -> Int64 {
    guard
      let info = try await FileSystem.shared.info(
        forFileAt: path.appending("pages"), infoAboutSymbolicLink: false
      )
    else { throw ValidationError("The index has no pages file.") }
    return info.size
  }
}

private struct StressMetrics: Sendable {
  enum Operation: String, CaseIterable, Sendable {
    case lookupHit = "lookup-hit"
    case lookupMiss = "lookup-miss"
    case scan = "scan-page"
    case insert
    case update
    case delete
    case transaction = "write-transaction"
  }

  private var durations: [Operation: [Duration]] = [:]
  private(set) var pageWrites: [Operation: Int] = [:]
  var scannedRows = 0

  mutating func record(_ operation: Operation, duration: Duration, pageWrites writes: Int?) {
    durations[operation, default: []].append(duration)
    if let writes { pageWrites[operation, default: 0] += writes }
  }

  mutating func merge(_ other: Self) {
    for (operation, samples) in other.durations { durations[operation, default: []].append(contentsOf: samples) }
    for (operation, count) in other.pageWrites { pageWrites[operation, default: 0] += count }
    scannedRows += other.scannedRows
  }

  func report() {
    print("scanned_rows=\(scannedRows)")
    print("operation\tcount\tp50_us\tp95_us\tp99_us\tpage_writes/op")
    for operation in Operation.allCases {
      guard let samples = durations[operation]?.sorted(), !samples.isEmpty else { continue }
      let percentiles = [0.50, 0.95, 0.99].map { percentile in
        let index = Int((Double(samples.count) * percentile).rounded(.up)) - 1
        return Self.format(samples[index] / .microseconds(1))
      }
      let writes = pageWrites[operation].map { Self.format(Double($0) / Double(samples.count)) } ?? "-"
      print("\(operation.rawValue)\t\(samples.count)\t\(percentiles.joined(separator: "\t"))\t\(writes)")
    }
  }

  static func format(_ value: Double) -> String {
    String(format: "%.1f", locale: Locale(identifier: "en_US_POSIX"), value)
  }
}
