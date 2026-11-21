extension DiskPage {
  struct Leaf {
    struct Entry {
      let key: Key
      let value: DiskRecord
    }

    private(set) var entries: [Entry]

    init() {
      entries = []
    }

    init?(validating entries: [Entry]) {
      guard Self.isStrictlyOrdered(entries) else { return nil }
      self.entries = entries
    }

    func value(for key: Key) -> DiskRecord? {
      let index = partitioningIndex { $0 >= key }
      guard matches(key, at: index) else { return nil }
      return entries[index].value
    }

    mutating func replace(_ value: DiskRecord?, for key: Key) -> DiskRecord? {
      defer { assert(Self.isStrictlyOrdered(entries), "Leaf keys must be strictly ordered") }
      let index = partitioningIndex { $0 >= key }
      guard let value else {
        return matches(key, at: index) ? entries.remove(at: index).value : nil
      }
      let entry = Entry(key: key, value: value)
      if matches(key, at: index) {
        let previous = entries[index].value
        entries[index] = entry
        return previous
      }
      entries.insert(entry, at: index)
      return nil
    }

    mutating func append(contentsOf right: Leaf) {
      entries.append(contentsOf: right.entries)
    }

    func partitioningIndex(where isInSuffix: (Key) -> Bool) -> Int {
      DiskPage.partitioningIndex(in: entries) { isInSuffix($0.key) }
    }

    private func matches(_ key: Key, at index: Int) -> Bool {
      index < entries.endIndex && entries[index].key == key
    }

    private static func isStrictlyOrdered(_ entries: [Entry]) -> Bool {
      zip(entries, entries.dropFirst()).allSatisfy { previous, current in
        previous.key < current.key
      }
    }
  }
}
