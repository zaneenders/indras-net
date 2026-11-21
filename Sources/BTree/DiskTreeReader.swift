struct DiskTreeReader<Partition: DiskBTreeKey, SortKey: DiskBTreeKey, Value: DiskBTreeValue>: Sendable, ~Escapable {
  typealias Page = DiskPage<Partition, SortKey>
  typealias Leaf = Page.Leaf
  typealias Key = Page.Key
  typealias ScanPage = DiskBTree<Partition, SortKey, Value>.ScanPage

  struct Reference {
    let id: Int64
    let level: UInt8?
    let lowerBound: Key?
    let upperBound: Key?

    static func root(_ id: Int64) -> Self {
      Self(id: id, level: nil, lowerBound: nil, upperBound: nil)
    }
  }

  struct Ancestor {
    let reference: Reference
    let branch: Page.Branch
    var childIndex: Int

    var child: Reference {
      Reference(
        id: branch.children[childIndex], level: branch.level - 1,
        lowerBound: childIndex == 0 ? reference.lowerBound : branch.separators[childIndex - 1],
        upperBound: childIndex == branch.separators.count ? reference.upperBound : branch.separators[childIndex])
    }
  }

  private let view: DiskPager.View

  @_lifetime(copy view)
  init(view: DiskPager.View) {
    self.view = view
  }

  func validateRoot() async throws(DiskBTreeError) {
    if let rootPageID = view.rootPageID {
      _ = try await readPage(.root(rootPageID))
    }
  }

  func value(for partition: Partition, sortKey: SortKey) async throws(DiskBTreeError) -> Value? {
    let key = Key(partition: partition, sortKey: sortKey)
    let (leaf, path) = try await lookupLeaf(for: key)
    return try leaf.value(for: key).map { record throws(DiskBTreeError) in try decode(record, path: path) }
  }

  func decode(_ record: DiskRecord, path: [Ancestor]) throws(DiskBTreeError) -> Value {
    guard let id = path.last?.child.id ?? view.rootPageID else { throw .corruptStore(cause: .invalidManifest) }
    return try record.decoded(as: Value.self, at: id)
  }

  func scan(partition: Partition, after: SortKey?, limit: Int) async throws(DiskBTreeError) -> ScanPage {
    guard limit > 0 else { throw .invalidScanLimit(limit) }
    guard let rootPageID = view.rootPageID else { return ScanPage(entries: [], nextAfter: nil) }
    let startKey = after.map { Key(partition: partition, sortKey: $0) }
    let isAtOrAfterStart: (Key) -> Bool = { key in
      if let startKey { return key > startKey }
      return key.partition >= partition
    }
    var path: [Ancestor] = []
    var leaf = try await descend(from: .root(rootPageID), where: isAtOrAfterStart, onto: &path)
    var start = leaf.partitioningIndex(where: isAtOrAfterStart)
    var entries: [ScanPage.Entry] = []
    while true {
      for entry in leaf.entries[start...] {
        guard entry.key.partition == partition else { return ScanPage(entries: entries, nextAfter: nil) }
        if entries.count == limit { return ScanPage(entries: entries, nextAfter: entries.last?.sortKey) }
        entries.append(.init(sortKey: entry.key.sortKey, value: try decode(entry.value, path: path)))
      }
      guard let next = try await nextLeaf(in: partition, onto: &path) else {
        return ScanPage(entries: entries, nextAfter: nil)
      }
      leaf = next
      start = 0
    }
  }

  func lookupLeaf(for key: Key) async throws(DiskBTreeError) -> (Leaf, [Ancestor]) {
    guard let rootPageID = view.rootPageID else { return (Leaf(), []) }
    var path: [Ancestor] = []
    let leaf = try await descend(from: .root(rootPageID), where: { $0 > key }, onto: &path)
    return (leaf, path)
  }

  private func descend(
    from start: Reference, where isInSuffix: (Key) -> Bool, onto path: inout [Ancestor]
  ) async throws(DiskBTreeError) -> Leaf {
    var reference = start
    while true {
      switch try await readPage(reference) {
      case .leaf(let leaf):
        return leaf
      case .branch(let branch):
        let index = Page.partitioningIndex(in: branch.separators, where: isInSuffix)
        let step = Ancestor(reference: reference, branch: branch, childIndex: index)
        path.append(step)
        reference = step.child
      }
    }
  }

  private func nextLeaf(in partition: Partition, onto path: inout [Ancestor]) async throws(DiskBTreeError) -> Leaf? {
    while var step = path.popLast() {
      step.childIndex += 1
      if step.childIndex < step.branch.children.count {
        let child = step.child
        if let lower = child.lowerBound, lower.partition > partition { return nil }
        path.append(step)
        return try await descend(from: child, where: { _ in true }, onto: &path)
      }
    }
    return nil
  }

  func readPage(_ reference: Reference) async throws(DiskBTreeError) -> Page {
    let invalid = DiskBTreeError.corruptStore(cause: .invalidPage(pageID: reference.id))
    let bytes = try await view.read(reference.id)
    let page = try Page.decode(bytes, at: reference.id)
    if let level = reference.level, level != page.level { throw invalid }

    let first: Key?
    let last: Key?
    switch page {
    case .leaf(let leaf):
      if reference.level != nil, leaf.entries.isEmpty { throw invalid }
      first = leaf.entries.first?.key
      last = leaf.entries.last?.key
    case .branch(let branch):
      guard branch.children.allSatisfy({ $0 < view.pageCount }) else { throw invalid }
      first = branch.separators.first
      last = branch.separators.last
    }
    if let lower = reference.lowerBound, let first {
      guard first >= lower, page.level == 0 || first != lower else { throw invalid }
    }
    if let upper = reference.upperBound, let last, last >= upper { throw invalid }
    return page
  }
}
