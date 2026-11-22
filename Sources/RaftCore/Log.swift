import Foundation

public typealias Log = [LogEntry]

extension Log {
  public static let sentinel: [LogEntry] = [LogEntry(term: 0, command: Data(), request: nil, isNoOp: false)]

  var lastLogIndex: LogIndex {
    LogIndex(count - 1)
  }

  var lastLogTerm: Term {
    self[Int(lastLogIndex)].term
  }

  mutating func appendReplicationEntries(prevLogIndex: LogIndex, entries: [LogEntry]) {
    for (offset, entry) in entries.enumerated() {
      let index = Int(prevLogIndex + 1) + offset
      if index == count || self[index].term != entry.term {
        replaceSubrange(index..., with: entries[offset...])
        return
      }
    }
  }
}
