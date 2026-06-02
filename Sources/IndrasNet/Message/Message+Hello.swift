import Foundation
import NIOCore

extension Message {
  static let magic: UInt8 = 0x66
  static let version: UInt8 = 0x0

  static func signal() -> Message {
    var msg = ByteBuffer()
    msg.writeInteger(Message.magic, as: UInt8.self)  // Magic number
    msg.writeInteger(Message.version, as: UInt8.self)  // Version
    return Message(type: .signal, payload: msg)
  }

  func signalRead() -> (magic: UInt8, version: UInt8)? {
    guard type == .signal else { return nil }
    var copy = payload
    guard let magic = copy.readInteger(as: UInt8.self) else { return nil }
    guard let version = copy.readInteger(as: UInt8.self) else { return nil }
    return (magic, version)
  }

  static func greet(id: PeerID) -> Message {
    return Message(type: .greet, payload: ByteBuffer(string: id))
  }

  func greetPeerID() -> PeerID? {
    guard type == .greet else { return nil }
    var copy = payload
    guard let raw = copy.readString(length: copy.readableBytes) else { return nil }
    return PeerID(raw)
  }

  static func hello(id: PeerID) -> Message {
    return Message(type: .hello, payload: ByteBuffer(string: id))
  }

  func helloPeerID() -> PeerID? {
    guard type == .hello else { return nil }
    var copy = payload
    guard let raw = copy.readString(length: copy.readableBytes) else { return nil }
    return PeerID(raw)
  }

  static func ping() -> Message {
    Message(type: .ping, payload: ByteBuffer())
  }

  static func pong() -> Message {
    Message(type: .pong, payload: ByteBuffer())
  }
}
