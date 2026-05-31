import Foundation
import NIOCore

extension Message {
  public static func hello(peerID: PeerID) throws -> Message {
    let data = try JSONEncoder().encode(peerID)
    var buffer = ByteBuffer()
    buffer.writeBytes(data)
    return Message(type: .hello, payload: buffer)
  }

  public func helloPeerID() throws -> PeerID? {
    guard type == .hello else { return nil }
    var copy = payload
    guard let bytes = copy.readBytes(length: copy.readableBytes) else { return nil }
    return try JSONDecoder().decode(PeerID.self, from: Data(bytes))
  }
}
