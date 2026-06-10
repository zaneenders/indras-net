import Foundation

typealias Log = [LogEntry]

extension Log {
  static let sentinel: [LogEntry] = [LogEntry(term: 0, command: Data())]

  var lastLogIndex: LogIndex {
    LogIndex(count - 1)
  }

  func lastLogTerm() -> Term {
    self[Int(lastLogIndex)].term
  }

  func term(at index: LogIndex) -> Term? {
    guard index < count else { return nil }
    return self[Int(index)].term
  }

  func matches(prevLogIndex: LogIndex, prevLogTerm: Term) -> Bool {
    guard prevLogIndex < count else { return false }
    return self[Int(prevLogIndex)].term == prevLogTerm
  }

  mutating func appendReplicationEntries(prevLogIndex: LogIndex, entries: [LogEntry]) {
    var entryOffset = 0
    var index = Int(prevLogIndex + 1)

    while entryOffset < entries.count {
      if index < count {
        if self[index].term != entries[entryOffset].term {
          removeSubrange(index...)
          append(contentsOf: entries[entryOffset...])
          return
        }
      } else {
        append(contentsOf: entries[entryOffset...])
        return
      }
      index += 1
      entryOffset += 1
    }
  }
}

func isLogUpToDate(
  candidateLastIndex: LogIndex,
  candidateLastTerm: Term,
  receiverLastIndex: LogIndex,
  receiverLastTerm: Term
) -> Bool {
  if candidateLastTerm != receiverLastTerm {
    return candidateLastTerm > receiverLastTerm
  }
  return candidateLastIndex >= receiverLastIndex
}
