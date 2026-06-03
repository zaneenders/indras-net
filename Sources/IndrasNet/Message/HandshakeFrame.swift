import NIOCore

extension MessageType {
  static let signal = MessageType(rawValue: 0x0000)  // magic + version
  static let greet = MessageType(rawValue: 0x0001)  // dialer's ID
  static let hello = MessageType(rawValue: 0x0002)  // accepter's ID
}

enum HandshakeFrame: Equatable, Sendable {
  case signal(magic: UInt8, version: UInt8)
  case greet(PeerID)
  case hello(PeerID)

  static let magic: UInt8 = 0x66
  static let version: UInt8 = 0x0
}

extension HandshakeFrame {
  init?(_ message: Message) {
    switch message.type {
    case .signal:
      var payload = message.payload
      guard
        let magic = payload.readInteger(as: UInt8.self),
        let version = payload.readInteger(as: UInt8.self)
      else { return nil }
      self = .signal(magic: magic, version: version)
    case .greet:
      guard let id = message.payload.readingPeerID() else { return nil }
      self = .greet(id)
    case .hello:
      guard let id = message.payload.readingPeerID() else { return nil }
      self = .hello(id)
    default:
      return nil
    }
  }

  var message: Message {
    switch self {
    case .signal(let magic, let version):
      var payload = ByteBuffer()
      payload.writeInteger(magic, as: UInt8.self)
      payload.writeInteger(version, as: UInt8.self)
      return Message(type: .signal, payload: payload)
    case .greet(let id):
      return Message(type: .greet, payload: ByteBuffer(string: id))
    case .hello(let id):
      return Message(type: .hello, payload: ByteBuffer(string: id))
    }
  }
}

extension ByteBuffer {
  fileprivate func readingPeerID() -> PeerID? {
    var copy = self
    guard let raw = copy.readString(length: copy.readableBytes) else { return nil }
    return PeerID(raw)
  }
}
