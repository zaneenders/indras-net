import Foundation
import NIOCore
import Networking
import Setu
import Synchronization

struct Peer: Sendable {
  let host: String
  let port: Int
  let connections: Int
  let chunkSize: Int
}

// Each request owns a socket, so bulk writes never queue ahead of control calls.
final class PeerConnection: Sendable {
  private typealias Reply = (TransferResponse, ByteBuffer?)
  private struct State: Sendable {
    var closed = false
    var requests: [UUID: Task<Reply, any Error>] = [:]
  }
  let peer: Peer
  private let connections = ServerConnections()
  private let state = Mutex(State())

  init(peer: Peer) { self.peer = peer }

  func close() async {
    let requests = state.withLock { state in
      state.closed = true
      return Array(state.requests.values)
    }
    for request in requests { request.cancel() }
    await connections.close()
    for request in requests { _ = try? await request.value }
  }

  func request(_ request: TransferRequest, body: ByteBuffer?, timeout: Duration) async throws -> (
    TransferResponse, ByteBuffer?
  ) {
    try Task.checkCancellation()
    let id = UUID()
    let task = try state.withLock { state in
      guard !state.closed else { throw PeerTransportError.closed }
      let task = Task {
        if request.retryable {
          return try await retryTransient { try await self.exchange(request, body: body, timeout: timeout) }
        }
        return try await self.exchange(request, body: body, timeout: timeout)
      }
      state.requests[id] = task
      return task
    }
    defer { _ = state.withLock { $0.requests.removeValue(forKey: id) } }
    return try await withTaskCancellationHandler {
      try await task.value
    } onCancel: {
      task.cancel()
    }
  }

  private func exchange(
    _ request: TransferRequest, body: ByteBuffer?, timeout: Duration
  ) async throws -> (TransferResponse, ByteBuffer?) {
    guard !connections.isClosed else { throw PeerTransportError.closed }
    let activity = RequestActivity()
    return try await withInactivityTimeout(timeout: timeout, activity: activity) { [self] in
      let connection = try await TCP.connect(host: peer.host, port: peer.port, connectTimeout: .seconds(10))
      connections.add(connection.channel)
      try await connection.channel.pipeline.addHandler(
        RequestActivityHandler(activity: activity), position: .first
      ).get()
      return try await connection.executeThenClose { inbound, outbound in
        try await outbound.write(SetuWire.encode(request, target: SetuWire.service, opcode: SetuWire.request))
        if let body { try await outbound.write(SetuWire.bytes(body)) }
        var iterator = inbound.makeAsyncIterator()
        var frame = try await iterator.next()
        while let heartbeat = frame, SetuWire.isHeartbeat(heartbeat) { frame = try await iterator.next() }
        guard let frame else { throw PeerTransportError.disconnected }
        let response = try SetuWire.decode(
          frame, target: SetuWire.service, opcode: SetuWire.response, as: TransferResponse.self)
        if case .failure(let message) = response { throw TransferError(message) }
        if case .chunk(let encoding, let length) = response {
          guard case .downloadChunk(_, _, _, let expectedLength) = request, length == expectedLength else {
            throw TransferError("Unexpected download chunk length")
          }
          var frame = try await iterator.next()
          while let heartbeat = frame, SetuWire.isHeartbeat(heartbeat) { frame = try await iterator.next() }
          guard let frame else { throw PeerTransportError.disconnected }
          let bytes = try SetuWire.payload(frame)
          let decoded = try await decodeChunk(bytes, encoding: encoding, expectedLength: length)
          return (response, decoded)
        }
        return (response, nil)
      }
    }
  }
}
