public import NIOCore

/// A protocol-neutral binary message. Both endpoints can send frames at any time.
///
/// The transport preserves `target` and `opcode` without interpreting them. A higher
/// layer defines their meaning, payload encoding, and interface versions.
public struct Frame: Sendable, Equatable {
  public static let headerBytes = 12

  public var target: UInt32
  public var opcode: UInt32
  public var payload: ByteBuffer

  public init(target: UInt32, opcode: UInt32, payload: ByteBuffer) {
    self.target = target
    self.opcode = opcode
    self.payload = payload
  }
}

public enum FrameError: Error, Sendable, Equatable {
  case payloadLengthOverflow
  case truncatedFrame
}
