import Foundation
import NIOCore

extension ByteBuffer {
  internal mutating func writePeerId(_ peerId: PeerId) {
    writeInteger(UInt32(peerId.utf8.count))
    writeString(peerId)
  }

  internal mutating func readPeerId() -> PeerId? {
    guard let length = readInteger(as: UInt32.self) else { return nil }
    return readString(length: Int(length))
  }

  internal mutating func writeLogEntry(_ entry: LogEntry) {
    writeInteger(entry.term)
    writeInteger(UInt32(entry.command.count))
    writeBytes(entry.command)
  }

  internal mutating func readLogEntry() -> LogEntry? {
    guard
      let term = readInteger(as: Term.self),
      let length = readInteger(as: UInt32.self)
    else { return nil }
    guard let command = readBytes(length: Int(length)) else { return nil }
    return LogEntry(term: term, command: Data(command))
  }

  internal mutating func writeLogEntries(_ entries: [LogEntry]) {
    writeInteger(UInt32(entries.count))
    for entry in entries {
      writeLogEntry(entry)
    }
  }

  internal mutating func readLogEntries() -> [LogEntry]? {
    guard let count = readInteger(as: UInt32.self) else { return nil }
    var entries: [LogEntry] = []
    entries.reserveCapacity(Int(count))
    for _ in 0..<count {
      guard let entry = readLogEntry() else { return nil }
      entries.append(entry)
    }
    return entries
  }
}
