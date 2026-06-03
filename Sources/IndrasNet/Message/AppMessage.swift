import NIOCore

enum AppMessage: Equatable, Sendable {
  case ping
  case pong
}

extension AppMessage {
  init?(_ message: Message) {
    switch message.type {
    case .ping: self = .ping
    case .pong: self = .pong
    default: return nil
    }
  }

  var message: Message {
    switch self {
    case .ping: Message(type: .ping, payload: ByteBuffer())
    case .pong: Message(type: .pong, payload: ByteBuffer())
    }
  }
}

extension MessageType {
  static let ping = MessageType(rawValue: 0x0003)
  static let pong = MessageType(rawValue: 0x0004)
}
