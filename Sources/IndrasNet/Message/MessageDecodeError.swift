public enum MessageDecodeError: Error, Sendable, Equatable {
  case truncatedHeader(remainingBytes: Int)
  case invalidMagic(got: UInt8)
  case unsupportedVersion(got: UInt8, expected: UInt8)
  case messageTooLarge(length: UInt32, max: UInt32)
  case incompleteMessageOnClose(remainingBytes: Int)
}
