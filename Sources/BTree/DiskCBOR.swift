import CBOR

enum DiskCBOR {
  static func decode<Value: Decodable>(_ type: Value.Type, from bytes: [UInt8]) throws -> Value {
    // The pinned decoder validates definite text and the combined bytes of chunked text.
    // RFC 8949 also requires valid UTF-8 in each chunk. Only walk items if a text marker is possible.
    if bytes.contains(0x7f) {
      var validator = TextChunkValidator(bytes: bytes.span)
      try validator.validate()
    }
    return try CBORDecoder(options: .default).decode(type, from: bytes)
  }

  private struct TextChunkValidator: ~Copyable, ~Escapable {
    private let bytes: Span<UInt8>
    private var offset: Int

    @_lifetime(copy bytes)
    init(bytes: Span<UInt8>) {
      self.bytes = bytes
      offset = 0
    }

    mutating func validate() throws(CBORError) {
      // Containers and tags are followed by more items. Skip scalar payloads and let CBORDecoder check structure.
      while offset < bytes.count {
        let start = offset
        let head = bytes[offset]
        offset += 1
        let additional = head & 0x1f
        if head == 0x7f {
          try validateChunks()
        } else if additional != 31 {
          let argument = try readArgument(additional, at: start)
          if head >> 5 == 2 || head >> 5 == 3 { _ = try take(argument, item: start) }
        }
      }
    }

    private mutating func validateChunks() throws(CBORError) {
      while offset < bytes.count {
        let start = offset
        let head = bytes[offset]
        offset += 1
        if head == 0xff { return }
        let additional = head & 0x1f
        guard head >> 5 == 3, additional != 31 else { throw .invalidIndefiniteLength(offset: start) }
        let length = try readArgument(additional, at: start)
        let range = try take(length, item: start)
        do {
          _ = try UTF8Span(validating: bytes.extracting(range))
        } catch {
          throw .invalidUTF8(offset: start)
        }
      }
      throw .unexpectedEnd(offset: offset)
    }

    private mutating func readArgument(_ additional: UInt8, at start: Int) throws(CBORError) -> UInt64 {
      if additional < 24 { return UInt64(additional) }
      guard additional <= 27 else { throw .reservedAdditionalInfo(additional, offset: start) }
      let range = try take(1 << (additional - 24), item: start)
      var argument: UInt64 = 0
      for index in range { argument = (argument << 8) | UInt64(bytes[index]) }
      return argument
    }

    private mutating func take(_ count: UInt64, item start: Int) throws(CBORError) -> Range<Int> {
      guard count <= UInt64(bytes.count - offset) else { throw .unexpectedEnd(offset: start) }
      let lower = offset
      offset += Int(count)
      return lower..<offset
    }
  }
}
