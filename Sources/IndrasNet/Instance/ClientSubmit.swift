import Foundation
import NIOCore

enum ClientSubmit {
  enum Status: UInt8, Equatable, Sendable {
    case ok = 1
    case notLeader = 2
  }

  struct Reply: Equatable, Sendable {
    let requestId: UInt128
    let status: Status
    let leaderId: PeerId?
    let logIndex: LogIndex

    init(requestId: UInt128, status: Status, leaderId: PeerId? = nil, logIndex: LogIndex = 0) {
      self.requestId = requestId
      self.status = status
      self.leaderId = leaderId
      self.logIndex = logIndex
    }

    func toMessage() -> Message {
      var payload = ByteBuffer()
      payload.writeInteger(requestId)
      payload.writeInteger(status.rawValue)
      payload.writeInteger(logIndex)
      if let leaderId {
        payload.writeInteger(UInt8(1))
        payload.writePeerId(leaderId)
      } else {
        payload.writeInteger(UInt8(0))
      }
      return Message(type: .clientSubmitResponse, payload: payload)
    }

    init?(from message: Message) {
      guard message.type == .clientSubmitResponse else { return nil }
      var payload = message.payload
      guard
        let requestId = payload.readInteger(as: UInt128.self),
        let statusRaw = payload.readInteger(as: UInt8.self),
        let status = Status(rawValue: statusRaw),
        let logIndex = payload.readInteger(as: LogIndex.self),
        let hasLeader = payload.readInteger(as: UInt8.self)
      else { return nil }
      let leaderId = hasLeader != 0 ? payload.readPeerId() : nil
      if hasLeader != 0, leaderId == nil { return nil }
      self.requestId = requestId
      self.status = status
      self.leaderId = leaderId
      self.logIndex = logIndex
    }
  }

  struct Args: Equatable, Sendable {
    let requestId: UInt128
    let command: Data

    enum Action: Equatable {
      case sendClientSubmitReply(to: PeerId, reply: Reply)
      case sendAppendEntry(to: PeerId, args: AppendEntries.Args)
      case persist
    }

    init(requestId: UInt128, command: Data) {
      self.requestId = requestId
      self.command = command
    }

    func toMessage() -> Message {
      var payload = ByteBuffer()
      payload.writeInteger(requestId)
      payload.writeInteger(UInt32(command.count))
      payload.writeBytes(command)
      return Message(type: .clientSubmit, payload: payload)
    }

    init?(from message: Message) {
      guard message.type == .clientSubmit else { return nil }
      var payload = message.payload
      guard
        let requestId = payload.readInteger(as: UInt128.self),
        let length = payload.readInteger(as: UInt32.self),
        let command = payload.readBytes(length: Int(length))
      else { return nil }
      self.requestId = requestId
      self.command = Data(command)
    }
  }
}
