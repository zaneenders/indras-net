public import Foundation
public import NIOCore
import Networking

public protocol DataSource: Sendable {
  func read(at offset: Int64, maximumBytes: Int) async throws -> ByteBuffer
  func close() async
}

public protocol DataSink: Sendable {
  func write(_ bytes: ByteBuffer, at offset: Int64) async throws
  func commit() async throws
  func abort() async
}

public enum TransferBody: Sendable {
  case source(any DataSource)
  case sink(any DataSink)
}

public struct TransferTicket: Codable, Sendable {
  public let id: UUID
  public let endpoint: ActorEndpoint
  public let byteCount: Int64
}

public struct TransferReceipt: Codable, Sendable, Equatable {
  public let id: UUID
  public let byteCount: Int64
  public let sha256: String
}

public struct TransferError: Error, CustomStringConvertible, Sendable {
  public let description: String

  public init(_ description: String) { self.description = description }
}

public struct CommitUnconfirmed: Error, CustomStringConvertible, Sendable {
  public let expected: TransferReceipt
  public var description: String { "Commit unconfirmed for transfer \(expected.id)" }
}

public actor DataTransfers {
  private struct Offer {
    let ticket: TransferTicket
    let peer: ActorEndpoint
    let expiration: Task<Void, Never>
    let expires: ContinuousClock.Instant
    let open: @Sendable () async throws -> TransferBody
  }

  private struct OutboundTransfer {
    let dial: Task<Connection, any Error>
    let task: Task<TransferReceipt, any Error>
  }

  nonisolated let endpoint: ActorEndpoint
  private let connectTimeout: TimeAmount
  private var offers: [UUID: Offer] = [:]
  private var outbound: [UUID: OutboundTransfer] = [:]
  private var serving: Set<UUID> = []
  private var receipts: [TransferReceipt] = []
  private var closingTask: Task<Void, Never>?
  private var stopped: Bool { closingTask != nil }

  init(endpoint: ActorEndpoint, connectTimeout: TimeAmount) {
    self.endpoint = endpoint
    self.connectTimeout = connectTimeout
  }

  /// A service authorizes a peer over RPC. Open storage only when that peer claims the ticket.
  public func offer(
    to peer: ActorEndpoint, byteCount: Int64, open: @escaping @Sendable () async throws -> TransferBody
  ) throws -> TransferTicket {
    guard !stopped else { throw RPCError.shutDown }
    guard byteCount >= 0, offers.count + serving.count < 128 else { throw TransferError("Transfer offer refused") }
    let ticket = TransferTicket(id: UUID(), endpoint: endpoint, byteCount: byteCount)
    let expires = ContinuousClock.now + .seconds(60)
    let expiration = Task { [weak self] in
      do { try await Task.sleep(until: expires, clock: .continuous) } catch { return }
      await self?.expire(ticket.id)
    }
    offers[ticket.id] = Offer(ticket: ticket, peer: peer, expiration: expiration, expires: expires, open: open)
    return ticket
  }

  private func expire(_ id: UUID) { offers.removeValue(forKey: id) }

  func claim(_ id: UUID, peer: ActorEndpoint) throws -> (TransferTicket, @Sendable () async throws -> TransferBody) {
    guard !stopped, let offer = offers[id], offer.expires > .now, offer.peer == peer else {
      throw TransferError("Unknown or expired transfer ticket")
    }
    offer.expiration.cancel()
    offers.removeValue(forKey: id)
    serving.insert(id)
    return (offer.ticket, offer.open)
  }

  func release(_ id: UUID) { serving.remove(id) }

  func commit(_ sink: any DataSink, receipt: TransferReceipt) async throws {
    try Task.checkCancellation()
    guard !stopped else { throw RPCError.shutDown }
    // The connection owner must remain cancellable while publication finishes.
    let publication = Task { try await sink.commit() }
    try await publication.value
    if receipts.count == 1024 { receipts.removeFirst() }
    receipts.append(receipt)
  }

  public func receipt(for id: UUID) -> TransferReceipt? { receipts.last { $0.id == id } }

  public func transfer(
    _ ticket: TransferTicket, open: @escaping @Sendable () async throws -> TransferBody
  ) async throws -> TransferReceipt {
    try Task.checkCancellation()
    guard !stopped else { throw RPCError.shutDown }
    guard ticket.byteCount >= 0, outbound[ticket.id] == nil else {
      throw TransferError("Cannot start transfer")
    }
    let dial = Task { [connectTimeout] in
      try await FramedTCP<ActorFraming>.connect(
        host: ticket.endpoint.host, port: ticket.endpoint.port, connectTimeout: connectTimeout)
    }
    let task = Task {
      let connection = try await dial.value
      guard !stopped, let owner = outbound[ticket.id]?.task else {
        try? await connection.channel.close(mode: .all, file: #fileID, line: #line)
        throw RPCError.shutDown
      }
      return try await withTaskCancellationHandler {
        try await DataWire.connect(ticket, connection: connection, transfers: self, open: open, cancel: owner.cancel)
      } onCancel: {
        connection.channel.close(promise: nil)
      }
    }
    outbound[ticket.id] = OutboundTransfer(dial: dial, task: task)
    defer { outbound.removeValue(forKey: ticket.id) }
    return try await withTaskCancellationHandler {
      try await task.value
    } onCancel: {
      dial.cancel()
      task.cancel()
    }
  }

  func close() async {
    if let closingTask {
      await closingTask.value
      return
    }
    let active = Array(outbound.values)
    for offer in offers.values { offer.expiration.cancel() }
    offers.removeAll()
    for transfer in active {
      transfer.dial.cancel()
      transfer.task.cancel()
    }
    let task = Task {
      await withTaskGroup(of: Void.self) { tasks in
        for transfer in active {
          let dial = transfer.dial
          tasks.addTask {
            guard let connection = try? await dial.value else { return }
            connection.channel.close(mode: .all, promise: nil)
            try? await connection.channel.closeFuture.get()
          }
        }
      }
    }
    closingTask = task
    await task.value
  }

  func shutdown() async {
    await close()
    for transfer in outbound.values { _ = await transfer.task.result }
  }
}
