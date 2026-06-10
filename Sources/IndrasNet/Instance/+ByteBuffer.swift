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
}
