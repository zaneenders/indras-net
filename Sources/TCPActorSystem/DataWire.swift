import Crypto
import Foundation
import NIOCore
import Networking
import Synchronization

enum ActorFraming: TCPFraming {
  static let maximumPayloadBytes: UInt32 = 4 * 1024 * 1024
  static let connectionLimits = [DataWire.target: UInt32(DataWire.blockBytes)]
}

enum DataWire {
  static let target: UInt32 = 0x4441_5441
  static let blockBytes = 256 * 1024
  typealias Frames = NIOAsyncChannelInboundStream<Frame>.AsyncIterator

  private struct Open: Codable {
    let version: Int
    let ticket: UUID
    let peer: ActorEndpoint
  }

  private enum Opcode: UInt32 {
    case open = 1
    case ready = 2
    case bytes = 3
    case finish = 4
    case committed = 5
    case failure = 6
  }

  static func connect(
    _ ticket: TransferTicket, connection: Connection, transfers: DataTransfers,
    open: @Sendable () async throws -> TransferBody, cancel: @escaping @Sendable () -> Void
  ) async throws -> TransferReceipt {
    try await connection.executeThenClose { inbound, _ in
      try Task.checkCancellation()
      let events = try await installTransferHandlers(connection)
      var frames = inbound.makeAsyncIterator()
      guard let first = try await frames.next(),
        case .hello(let version, let endpoint, _) = try WireMessage(frame: first),
        version == WireMessage.version, endpoint == ticket.endpoint
      else { throw TransferError("Unexpected peer on transfer connection") }
      try await send(
        .open, value: Open(version: 1, ticket: ticket.id, peer: transfers.endpoint), connection: connection)
      let ready = try await next(&frames, expecting: .ready)
      guard ready.readableBytes == 0 else { throw RPCError.invalidMessage }
      try Task.checkCancellation()
      do {
        return try await stream(
          open: open, cancel: cancel, ticket: ticket, frames: &frames, connection: connection, transfers: transfers,
          accepting: false)
      } catch {
        throw await reportFailure(error, events: events, connection: connection)
      }
    }
  }

  static func serve(
    _ first: Frame, frames: inout Frames, connection: Connection, transfers: DataTransfers,
    cancel: @escaping @Sendable () -> Void
  ) async throws {
    let events = try await installTransferHandlers(connection)
    do {
      guard first.opcode == Opcode.open.rawValue else { throw RPCError.invalidMessage }
      let request = try JSONDecoder().decode(Open.self, from: Data(first.payload.readableBytesView))
      guard request.version == 1 else { throw TransferError("Unsupported data protocol version") }
      let (ticket, open) = try await transfers.claim(request.ticket, peer: request.peer)
      defer { await transfers.release(ticket.id) }
      try Task.checkCancellation()
      _ = try await stream(
        open: open, cancel: cancel, ticket: ticket, frames: &frames, connection: connection, transfers: transfers,
        accepting: true)
    } catch {
      throw await reportFailure(error, events: events, connection: connection)
    }
  }

  private static func stream(
    open: @Sendable () async throws -> TransferBody, cancel: @escaping @Sendable () -> Void,
    ticket: TransferTicket, frames: inout Frames, connection: Connection, transfers: DataTransfers, accepting: Bool
  ) async throws -> TransferReceipt {
    let systemIDs = ManagedActorSystemWork.systemIDs.union([transfers.endpoint.systemID])
    return try await ManagedActorSystemWork.$systemIDs.withValue(systemIDs) {
      try await runTransfer(
        open: open, cancel: cancel, ticket: ticket, frames: &frames, connection: connection, transfers: transfers,
        accepting: accepting)
    }
  }

  private static func runTransfer(
    open: @Sendable () async throws -> TransferBody, cancel: @escaping @Sendable () -> Void,
    ticket: TransferTicket, frames: inout Frames, connection: Connection, transfers: DataTransfers, accepting: Bool
  ) async throws -> TransferReceipt {
    // Input may close while storage is suspended and the iterator is not being polled.
    try await connection.setInputCloseHandler(cancel)
    try Task.checkCancellation()
    let body = try await open()
    var committed = false
    defer {
      await withTaskCancellationShield {
        switch body {
        case .source(let source): await source.close()
        case .sink(let sink): if !committed { await sink.abort() }
        }
      }
    }
    try Task.checkCancellation()
    if accepting { try await write(.ready, bytes: ByteBuffer(), connection: connection) }

    var offset: Int64 = 0
    var hash = SHA256()
    switch body {
    case .source(let source):
      while offset < ticket.byteCount {
        try Task.checkCancellation()
        let limit = Int(min(Int64(blockBytes), ticket.byteCount - offset))
        let bytes = try await source.read(at: offset, maximumBytes: limit)
        guard (1...limit).contains(bytes.readableBytes) else { throw TransferError("Source size changed") }
        hash.update(data: Data(bytes.readableBytesView))
        try await write(.bytes, bytes: bytes, connection: connection)
        offset += Int64(bytes.readableBytes)
      }
      try Task.checkCancellation()
      guard try await source.read(at: offset, maximumBytes: 1).readableBytes == 0 else {
        throw TransferError("Source exceeds offered size")
      }
      let receipt = makeReceipt(ticket, hash: hash)
      // EOF can follow a valid acknowledgement in the same socket read. Drain that reply.
      try await connection.setInputCloseHandler(nil)
      try Task.checkCancellation()
      do {
        try await send(.finish, value: receipt, connection: connection)
        let reply = try await next(&frames, expecting: .committed)
        guard try JSONDecoder().decode(TransferReceipt.self, from: Data(reply.readableBytesView)) == receipt else {
          throw RPCError.invalidMessage
        }
      } catch let rejection as TransferError {
        throw rejection
      } catch {
        throw CommitUnconfirmed(expected: receipt)
      }
      return receipt
    case .sink(let sink):
      while offset < ticket.byteCount {
        try Task.checkCancellation()
        let bytes = try await next(&frames, expecting: .bytes)
        guard (1...blockBytes).contains(bytes.readableBytes),
          Int64(bytes.readableBytes) <= ticket.byteCount - offset
        else { throw TransferError("Data exceeds offered size") }
        hash.update(data: Data(bytes.readableBytesView))
        try await sink.write(bytes, at: offset)
        offset += Int64(bytes.readableBytes)
      }
      let finish = try await next(&frames, expecting: .finish)
      let receipt = makeReceipt(ticket, hash: hash)
      guard try JSONDecoder().decode(TransferReceipt.self, from: Data(finish.readableBytesView)) == receipt else {
        throw TransferError("Transfer integrity check failed")
      }
      try await transfers.commit(sink, receipt: receipt)
      committed = true
      // The receiver has committed even if the sender disconnects before observing the receipt.
      try? await send(.committed, value: receipt, connection: connection)
      return receipt
    }
  }

  private static func makeReceipt(_ ticket: TransferTicket, hash: SHA256) -> TransferReceipt {
    let hex = hash.finalize().map { String(format: "%02x", $0) }.joined()
    return TransferReceipt(id: ticket.id, byteCount: ticket.byteCount, sha256: hex)
  }

  private static func next(_ frames: inout Frames, expecting opcode: Opcode) async throws -> ByteBuffer {
    guard let frame = try await frames.next(isolation: #isolation) else { throw RPCError.disconnected }
    guard frame.target == target else { throw RPCError.invalidMessage }
    if frame.opcode == Opcode.failure.rawValue {
      throw failure(from: frame)
    }
    guard frame.opcode == opcode.rawValue else { throw RPCError.invalidMessage }
    return frame.payload
  }

  private static func failure(from frame: Frame) -> TransferError {
    TransferError(String(decoding: frame.payload.readableBytesView.prefix(4096), as: UTF8.self))
  }

  private static func reportFailure(
    _ error: any Error, events: TransferEvents, connection: Connection
  ) async -> any Error {
    if let rejection = events.rejection.withLock({ $0 }) { return rejection }
    if !Task.isCancelled {
      let bytes = ByteBuffer(bytes: String(describing: error).utf8.prefix(4096))
      try? await write(.failure, bytes: bytes, connection: connection)
    }
    return error
  }

  private static func send(_ opcode: Opcode, value: some Encodable, connection: Connection) async throws {
    try await write(opcode, bytes: ByteBuffer(bytes: JSONEncoder().encode(value)), connection: connection)
  }

  private static func write(_ opcode: Opcode, bytes: ByteBuffer, connection: Connection) async throws {
    guard bytes.readableBytes <= blockBytes else { throw FrameError.payloadLengthOverflow }
    try await connection.channel.writeAndFlush(
      Frame(target: target, opcode: opcode.rawValue, payload: bytes)
    ).getAbandoningOnCancel()
  }

  private static func installTransferHandlers(_ connection: Connection) async throws -> TransferEvents {
    let channel = connection.channel
    let events = TransferEvents()
    try await channel.eventLoop.submit {
      let pipeline = channel.pipeline.syncOperations
      try pipeline.addHandler(events, position: .last)
      try pipeline.addHandler(IdleStateHandler(allTimeout: .seconds(120)), position: .first)
    }.get()
    return events
  }

  private final class TransferEvents: ChannelInboundHandler, Sendable {
    typealias InboundIn = Frame
    let rejection = Mutex<TransferError?>(nil)

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
      let frame = unwrapInboundIn(data)
      if frame.target == target, frame.opcode == Opcode.failure.rawValue {
        // EOF can cancel suspended storage before the iterator reads this frame.
        rejection.withLock { if $0 == nil { $0 = failure(from: frame) } }
      }
      context.fireChannelRead(data)
    }

    func userInboundEventTriggered(context: ChannelHandlerContext, event: Any) {
      if event is IdleStateHandler.IdleStateEvent {
        context.close(promise: nil)
      } else {
        context.fireUserInboundEventTriggered(event)
      }
    }
  }
}
