import NIOCore
import NIOEmbedded
import Testing

@testable import IndrasNet

@Suite struct MessageCodecTests {
  @Test func handshakeSignalRoundTripThroughCodec() throws {
    let original = HandshakeFrame.signal(magic: HandshakeFrame.magic, version: HandshakeFrame.version)
    let decoded = try decodeInbound(original.message.encodeToByteBuffer())
    let roundTripped = try #require(HandshakeFrame(decoded))
    #expect(roundTripped == original)
  }

  @Test func handshakeGreetRoundTripThroughCodec() throws {
    let original = HandshakeFrame.greet("peer-a")
    let decoded = try decodeInbound(original.message.encodeToByteBuffer())
    let roundTripped = try #require(HandshakeFrame(decoded))
    #expect(roundTripped == original)
  }

  @Test func handshakeHelloRoundTripThroughCodec() throws {
    let original = HandshakeFrame.hello("peer-b")
    let decoded = try decodeInbound(original.message.encodeToByteBuffer())
    let roundTripped = try #require(HandshakeFrame(decoded))
    #expect(roundTripped == original)
  }

  @Test func requestVoteArgsRoundTripThroughCodec() throws {
    let args = RequestVote.Args(term: 2, candidateId: "node-a", lostLogIndex: 3, lastLogTerm: 4)
    let original = args.toMessage()
    let decoded = try decodeInbound(original.encodeToByteBuffer())
    let roundTripped = try #require(RequestVote.Args(from: decoded))
    #expect(roundTripped == args)
  }

  @Test func requestVoteReplyRoundTripThroughCodec() throws {
    let reply = RequestVote.Reply(granted: true, term: 7)
    let original = reply.toMessage()
    let decoded = try decodeInbound(original.encodeToByteBuffer())
    let roundTripped = try #require(RequestVote.Reply(from: decoded))
    #expect(roundTripped == reply)
  }

  @Test func appendEntriesArgsRoundTripThroughCodec() throws {
    let args = AppendEntries.Args(term: 3, leaderId: "leader-1")
    let original = args.toMessage()
    let decoded = try decodeInbound(original.encodeToByteBuffer())
    let roundTripped = try #require(AppendEntries.Args(from: decoded))
    #expect(roundTripped == args)
  }

  @Test func appendEntriesReplyRoundTripThroughCodec() throws {
    let reply = AppendEntries.Reply(term: 7, success: true)
    let original = reply.toMessage()
    let decoded = try decodeInbound(original.encodeToByteBuffer())
    let roundTripped = try #require(AppendEntries.Reply(from: decoded))
    #expect(roundTripped == reply)
  }

  @Test func appendEntriesReplyWireFormatMatchesProtocolLayout() throws {
    let reply = AppendEntries.Reply(term: 1, success: false)
    var wire = reply.toMessage().encodeToByteBuffer()

    #expect(wire.readableBytes == Message.headerLength + 17)
    #expect(wire.readInteger(as: UInt16.self) == MessageType.appendEntriesResponse.rawValue)
    #expect(wire.readInteger(as: UInt32.self) == 17)
    #expect(wire.readInteger(as: UInt128.self) == 1)
    #expect(wire.readInteger(as: UInt8.self) == 0)
  }

  @Test func requestVoteReplyWireFormatMatchesProtocolLayout() throws {
    let reply = RequestVote.Reply(granted: true, term: 1)
    var wire = reply.toMessage().encodeToByteBuffer()

    #expect(wire.readableBytes == Message.headerLength + 17)
    #expect(wire.readInteger(as: UInt16.self) == MessageType.requestVoteResponse.rawValue)
    #expect(wire.readInteger(as: UInt32.self) == 17)
    #expect(wire.readInteger(as: UInt128.self) == 1)
    #expect(wire.readInteger(as: UInt8.self) == 1)
  }

  @Test func decoderWaitsForFullFrameBeforeEmitting() throws {
    let wire = HandshakeFrame.signal(magic: 0x66, version: 0).message.encodeToByteBuffer()
    let totalBytes = wire.readableBytes
    let partial = wire.getSlice(at: wire.readerIndex, length: totalBytes - 1)!

    let channel = try makeCodecChannel()
    try channel.writeInbound(partial)
    #expect(try channel.readInbound(as: Message.self) == nil)

    try channel.writeInbound(wire.getSlice(at: wire.readerIndex + totalBytes - 1, length: 1)!)
    #expect(try channel.readInbound(as: Message.self) != nil)
  }

  @Test func decodeAcceptsUnknownMessageTypeForExtensibility() throws {
    let wire = validHeaderWire(typeRaw: 0xFFFF, payloadLength: 0)
    let decoded = try decodeInbound(wire)

    #expect(decoded.type == MessageType(rawValue: 0xFFFF))
    #expect(decoded.payload.readableBytes == 0)
  }

  @Test func decodeLastRejectsPartialFrameOnClose() throws {
    var wire = ByteBuffer()
    wire.writeInteger(UInt16(0x0001))  // type only; payload length (4 bytes) missing

    let channel = try makeCodecChannel()
    try channel.writeInbound(wire)

    #expect(throws: MessageDecodeError.incompleteMessageOnClose(remainingBytes: 2)) {
      _ = try channel.finish(acceptAlreadyClosed: false)
    }
  }

  @Test func decodeRejectsPayloadLengthAboveMax() throws {
    let max: UInt32 = 64
    let wire = validHeaderWire(type: .requestVote, payloadLength: max + 1)

    #expect(throws: MessageDecodeError.messageTooLarge(length: max + 1, max: max)) {
      _ = try decodeInbound(wire, maxPayloadLength: max)
    }
  }

  @Test func decodeLastRejectsTrailingBytesOnClose() throws {
    var wire = RequestVote.Reply(granted: false, term: 0).toMessage().encodeToByteBuffer()
    wire.writeInteger(UInt8(0xFF))

    let channel = try makeCodecChannel()
    try channel.writeInbound(wire)
    _ = try channel.readInbound(as: Message.self)

    #expect(throws: MessageDecodeError.incompleteMessageOnClose(remainingBytes: 1)) {
      _ = try channel.finish(acceptAlreadyClosed: false)
    }
  }
}

extension MessageCodecTests {
  private func makeCodecChannel(maxPayloadLength: UInt32 = Message.defaultMaxPayloadLength) throws
    -> EmbeddedChannel
  {
    let channel = EmbeddedChannel()
    try channel.pipeline.syncOperations.addHandler(
      ByteToMessageHandler(MessageDecoder(maxPayloadLength: maxPayloadLength))
    )
    return channel
  }

  private func decodeInbound(
    _ wire: ByteBuffer,
    maxPayloadLength: UInt32 = Message.defaultMaxPayloadLength
  ) throws -> Message {
    let channel = try makeCodecChannel(maxPayloadLength: maxPayloadLength)
    try channel.writeInbound(wire)
    return try #require(try channel.readInbound(as: Message.self))
  }

  private func validHeaderWire(
    type: MessageType,
    payloadLength: UInt32
  ) -> ByteBuffer {
    validHeaderWire(typeRaw: type.rawValue, payloadLength: payloadLength)
  }

  private func validHeaderWire(
    typeRaw: UInt16,
    payloadLength: UInt32
  ) -> ByteBuffer {
    var buffer = ByteBuffer()
    buffer.writeInteger(typeRaw)
    buffer.writeInteger(payloadLength)
    return buffer
  }
}

extension Message {
  func encodeToByteBuffer(allocator: ByteBufferAllocator = ByteBufferAllocator()) -> ByteBuffer {
    var buffer = allocator.buffer(capacity: Message.headerLength + payload.readableBytes)
    let encoder = MessageEncoder()
    try? encoder.encode(data: self, out: &buffer)
    return buffer
  }
}
