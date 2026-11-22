package import struct SystemPackage.FilePath

enum DiskCommitStep: Equatable, Sendable {
  case writePage(index: Int)
  case syncPages
  case writeManifest
  case syncManifest
  case syncDirectoryBeforeRename
  case renameManifest
  case syncDirectoryAfterRename
  case syncDirectoryAfterRollback
}

package struct DiskBTree<Partition: DiskBTreeKey, SortKey: DiskBTreeKey, Value: DiskBTreeValue>: Sendable {
  private typealias Reader = DiskTreeReader<Partition, SortKey, Value>

  private let path: FilePath
  private let schema: DiskTreeSchema

  package static func validateRecord(
    _ value: Value, for partition: Partition, sortKey: SortKey
  ) throws(DiskBTreeError) {
    _ = try DiskPage<Partition, SortKey>.record(for: .init(partition: partition, sortKey: sortKey), value: value)
  }

  package static func create(in directory: FilePath) async throws(DiskBTreeError) -> Self {
    let schema = Self.persistedSchema
    try await DiskPager.create(at: directory, schema: schema)
    return Self(path: directory, schema: schema)
  }

  package static func open(in directory: FilePath) async throws(DiskBTreeError) -> Self {
    let schema = Self.persistedSchema
    try await DiskPager.performingIO {
      try await DiskPager.withReader(at: directory, schema: schema) { view in
        try await Reader(view: view).validateRoot()
      }
    }
    return Self(path: directory, schema: schema)
  }

  static var persistedSchema: DiskTreeSchema {
    DiskTreeSchema(
      partitionOrderingID: Partition.diskOrderingID,
      sortKeyOrderingID: SortKey.diskOrderingID,
      valueSchemaID: Value.diskSchemaID)
  }

  package func withWriteTransaction<Result: Sendable>(
    execute: @Sendable (inout WriteTransaction) async throws -> Result
  ) async throws -> Result {
    try await DiskPager.withWriter(at: path, schema: schema) { pager in
      try await Reader(view: pager.view).validateRoot()
      var transaction = WriteTransaction(pager: pager)
      let result = try await execute(&transaction)
      let published = try await transaction.pager.finish()
      return (result, published)
    }
  }

  package func withReadSnapshot<Result: Sendable>(
    execute: @Sendable (borrowing ReadSnapshot) async throws -> Result
  ) async throws -> Result {
    try await DiskPager.withReader(at: path, schema: schema) { view in
      try await execute(ReadSnapshot(view: view))
    }
  }

  package func value(for partition: Partition, sortKey: SortKey) async throws(DiskBTreeError) -> Value? {
    try await DiskPager.performingIO {
      try await DiskPager.withReader(at: path, schema: schema) { view in
        try await Reader(view: view).value(for: partition, sortKey: sortKey)
      }
    }
  }
}

extension DiskBTree {
  package struct ScanPage: Sendable {
    package struct Entry: Sendable {
      package let sortKey: SortKey
      package let value: Value
    }

    package let entries: [Entry]
    package let nextAfter: SortKey?
  }

  package struct ReadSnapshot: ~Escapable, ~Sendable {
    private let reader: Reader

    @_lifetime(copy view)
    fileprivate init(view: DiskPager.View) {
      reader = Reader(view: view)
    }

    package func value(for partition: Partition, sortKey: SortKey) async throws(DiskBTreeError) -> Value? {
      try await reader.value(for: partition, sortKey: sortKey)
    }

    package func scan(partition: Partition, after: SortKey?, limit: Int) async throws(DiskBTreeError) -> ScanPage {
      try await reader.scan(partition: partition, after: after, limit: limit)
    }
  }

  package struct WriteTransaction: ~Copyable, ~Escapable, ~Sendable {
    private typealias Page = DiskPage<Partition, SortKey>
    private typealias Leaf = Page.Leaf
    private typealias Key = Page.Key
    private typealias Format = DiskPageFormat

    private struct Replacement {
      let left: Int64
      let split: (separator: Key, right: Int64)?
    }

    fileprivate var pager: DiskPager

    @_lifetime(borrow pager)
    fileprivate init(pager: borrowing DiskPager) {
      self.pager = copy pager
    }

    private var reader: Reader {
      @_lifetime(borrow self)
      borrowing get { Reader(view: pager.view) }
    }

    var beforeCommitStep: (@Sendable (DiskCommitStep) async throws -> Void)? {
      get { pager.beforeCommitStep }
      set { pager.beforeCommitStep = newValue }
    }

    /// Pages written by successful mutations in this transaction.
    package var pageWriteCount: Int { pager.pageWriteCount }

    package func value(for partition: Partition, sortKey: SortKey) async throws(DiskBTreeError) -> Value? {
      try await reader.value(for: partition, sortKey: sortKey)
    }

    package func scan(partition: Partition, after: SortKey?, limit: Int) async throws(DiskBTreeError) -> ScanPage {
      try await reader.scan(partition: partition, after: after, limit: limit)
    }

    @discardableResult
    package mutating func updateValue(
      _ value: Value, for partition: Partition, sortKey: SortKey
    ) async throws(DiskBTreeError) -> Value? {
      try await replaceValue(.some(value), for: Key(partition: partition, sortKey: sortKey))
    }

    @discardableResult
    package mutating func removeValue(for partition: Partition, sortKey: SortKey) async throws(DiskBTreeError) -> Value?
    {
      try await replaceValue(nil, for: Key(partition: partition, sortKey: sortKey))
    }

    private mutating func replaceValue(_ value: Value?, for key: Key) async throws(DiskBTreeError) -> Value? {
      let (leaf, path) = try await reader.lookupLeaf(for: key)
      var replacement = leaf
      let record = try value.map { value throws(DiskBTreeError) in try Page.record(for: key, value: value) }
      let old = replacement.replace(record, for: key)
      guard record != nil || old != nil else { return nil }
      let previous = try old.map { record throws(DiskBTreeError) in try reader.decode(record, path: path) }
      try await apply(replacement, path: path, rebalance: (record?.size ?? 0) < (old?.size ?? 0))
      return previous
    }

    private func stage(
      _ encoded: Page.Encoded, in batch: inout DiskPager.Batch
    ) throws(DiskBTreeError) -> Replacement {
      let left = try pager.stage(encoded.left, in: &batch)
      guard let split = encoded.split else { return Replacement(left: left, split: nil) }
      return Replacement(left: left, split: (split.separator, try pager.stage(split.right, in: &batch)))
    }

    private mutating func apply(
      _ leaf: Leaf, path: [Reader.Ancestor], rebalance: Bool
    ) async throws(DiskBTreeError) {
      var batch = try pager.beginBatch()
      var page = Page.leaf(leaf)
      for step in path.reversed() {
        var branch = step.branch
        let encoded = try page.encode()
        // Whole records can limit how evenly a sparse page and its sibling can be split.
        if rebalance, encoded.payloadSize <= Format.payloadCapacity / 2 {
          let (left, joined) = try await rebalanceSiblings(around: step, page: page)
          batch.retire(branch.children[left])
          batch.retire(branch.children[left + 1])
          let replacement = try stage(joined, in: &batch)
          branch.children[left] = replacement.left
          if let split = replacement.split {
            branch.separators[left] = split.separator
            branch.children[left + 1] = split.right
          } else {
            branch.separators.remove(at: left)
            branch.children.remove(at: left + 1)
          }
        } else {
          batch.retire(branch.children[step.childIndex])
          let replacement = try stage(encoded, in: &batch)
          branch.children[step.childIndex] = replacement.left
          if let split = replacement.split {
            branch.separators.insert(split.separator, at: step.childIndex)
            branch.children.insert(split.right, at: step.childIndex + 1)
          }
        }
        page = .branch(branch)
      }
      let rootPageID: Int64
      if case .branch(let branch) = page, branch.children.count == 1 {
        rootPageID = branch.children[0]
      } else {
        let replacement = try stage(page.encode(), in: &batch)
        if let split = replacement.split {
          let level = page.level + 1
          guard level <= Format.maxLevel else { throw .storeFull }
          let root = Page.Branch(level: level, separators: [split.separator], children: [replacement.left, split.right])
          let newRoot = try stage(Page.encode(root), in: &batch)
          assert(newRoot.split == nil, "A new root has room for its first separator")
          rootPageID = newRoot.left
        } else {
          rootPageID = replacement.left
        }
      }

      try await pager.apply(batch, root: rootPageID)
    }

    private func rebalanceSiblings(
      around step: Reader.Ancestor, page: Page
    ) async throws(DiskBTreeError) -> (Int, Page.Encoded) {
      let index = step.childIndex
      let left = max(index - 1, 0)
      let preferred = try await encodeSiblings(at: left, around: step, page: page)
      if preferred.split != nil, index > 0, index + 1 < step.branch.children.count {
        let alternate = try await encodeSiblings(at: index, around: step, page: page)
        if alternate.split == nil { return (index, alternate) }
      }
      return (left, preferred)
    }

    private func encodeSiblings(
      at left: Int, around step: Reader.Ancestor, page: Page
    ) async throws(DiskBTreeError) -> Page.Encoded {
      let pageIsLeft = step.childIndex == left
      var adjacent = step
      adjacent.childIndex = pageIsLeft ? left + 1 : left
      let reference = adjacent.child
      let sibling = try await reader.readPage(reference)
      let separator = step.branch.separators[left]
      let joined =
        if pageIsLeft { page.joined(with: sibling, separator: separator) } else {
          sibling.joined(with: page, separator: separator)
        }
      guard let joined else { throw .corruptStore(cause: .invalidPage(pageID: reference.id)) }
      return try joined.encode()
    }
  }
}
