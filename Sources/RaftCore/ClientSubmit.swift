public import Foundation

public enum ClientSubmit: Sendable {
  public enum Status: UInt8, Codable, Equatable, Sendable {
    case ok = 1
    case notLeader = 2
    case aborted = 3
    case commandTooLarge = 4
  }

  public struct Reply: Codable, Equatable, Sendable {
    public let requestId: UInt64
    public let status: Status
    public let leaderId: PeerId?
    public let logIndex: LogIndex
    public let result: Data?

    public init(
      requestId: UInt64, status: Status, leaderId: PeerId?, logIndex: LogIndex, result: Data?
    ) {
      self.requestId = requestId
      self.status = status
      self.leaderId = leaderId
      self.logIndex = logIndex
      self.result = result
    }
  }

  public struct Args: Codable, Equatable, Sendable {
    public let requestId: UInt64
    public let command: Data
    public let traceHeaders: [String: String]

    public enum Action: Equatable, Sendable {
      case sendClientSubmitReply(to: PeerId, reply: Reply)
      case clientWriteAppended(logIndex: LogIndex, requestId: UInt64, client: PeerId)
      case sendAppendEntry(to: PeerId, args: AppendEntries.Args)
      case apply(entry: LogEntry, atIndex: LogIndex)
      case persist
    }

    public init(requestId: UInt64, command: Data, traceHeaders: [String: String]) {
      self.requestId = requestId
      self.command = command
      self.traceHeaders = traceHeaders
    }
  }
}
