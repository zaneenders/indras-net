import NIOCore
import Networking
import Setu

struct TransferService: Sendable {
  let store: UploadStore
  let catalog: FileCatalog
  let onRestart: (@Sendable () -> Void)?

  func handle(_ connection: Connection, idleTimeout: Duration) async throws {
    let activity = RequestActivity()
    let progress = TransferProgressTracker(onUpdate: { activity.record() })
    try await connection.channel.pipeline.addHandler(
      RequestActivityHandler(activity: activity), position: .first
    ).get()
    let restart = try await withInactivityTimeout(timeout: idleTimeout, activity: activity) {
      try await connection.executeThenClose { inbound, outbound in
        try await withThrowingDiscardingTaskGroup { tasks in
          tasks.addTask {
            do {
              while true {
                try await Task.sleep(for: min(idleTimeout / 3, .seconds(10)))
                try await outbound.write(
                  Frame(target: SetuWire.service, opcode: SetuWire.heartbeat, payload: ByteBuffer()))
              }
            } catch {}
          }
          defer { tasks.cancelAll() }
          var iterator = inbound.makeAsyncIterator()
          guard let frame = try await iterator.next() else { return false }
          do {
            let request = try SetuWire.decode(
              frame, target: SetuWire.service, opcode: SetuWire.request, as: TransferRequest.self)
            if case .restart = request {
              guard onRestart != nil else { throw TransferError("Restart is not supported by this server") }
              try await outbound.write(
                SetuWire.encode(
                  TransferResponse.restart(RestartAcknowledgement(restartRequested: true, exitCode: 1)),
                  target: SetuWire.service, opcode: SetuWire.response))
              outbound.finish()
              guard try await iterator.next() == nil else { throw TransferError("Unexpected request continuation") }
              return true
            }
            let response: TransferResponse
            var body: ByteBuffer?
            switch request {
            case .health: response = .health(catalog.node)
            case .list: response = .files(try await catalog.list())
            case .manifest(let fileID, let chunkSize):
              response = .manifest(try await catalog.manifest(fileID: fileID, chunkSize: chunkSize, progress: progress))
            case .begin(let manifest, let destination, let create, let owner):
              response = .status(
                try await store.begin(
                  manifest, destination: destination, createDestination: create, owner: owner, progress: progress))
            case .uploadChunk(let id, let index, let encoding):
              let reservation = try await store.reserve(id, index: index)
              do {
                guard let frame = try await iterator.next() else { throw PeerTransportError.disconnected }
                let payload = try SetuWire.payload(frame)
                if let (manifest, path) = reservation {
                  let bytes = try await decodeChunk(payload, encoding: encoding, expectedLength: manifest.length(index))
                  guard sha256(bytes.readableBytesView) == manifest.chunks[index] else {
                    throw TransferError("Chunk integrity check failed")
                  }
                  try await writeRange(
                    path: path, offset: Int64(index) * Int64(manifest.chunkSize), data: bytes.readableBytesView)
                  try await store.complete(id, index: index)
                }
                response = .acknowledged
              } catch {
                if reservation != nil { await store.release(id, index: index) }
                throw error
              }
            case .downloadChunk(let name, let index, let chunkSize, let length):
              let bytes = try await catalog.chunk(name: name, index: index, chunkSize: chunkSize)
              guard bytes.readableBytes == length else { throw TransferError("Source chunk length changed") }
              body = try await encodeChunk(bytes)
              response = .chunk(encoding: .gzip, length: bytes.readableBytes)
            case .commit(let id):
              try await store.commit(id, progress: progress)
              response = .acknowledged
            case .discard(let id, let owner):
              try await store.discard(id, owner: owner)
              response = .acknowledged
            case .restart: throw TransferError("Unexpected restart request")
            }
            try await outbound.write(SetuWire.encode(response, target: SetuWire.service, opcode: SetuWire.response))
            if let body { try await outbound.write(SetuWire.bytes(body)) }
          } catch {
            try await outbound.write(
              SetuWire.encode(
                TransferResponse.failure(describeError(error)), target: SetuWire.service, opcode: SetuWire.response))
          }
          // Keep the scope open until the client consumes the response and closes.
          // Closing immediately after write admission can discard queued bulk bytes.
          outbound.finish()
          guard try await iterator.next() == nil else { throw TransferError("Unexpected request continuation") }
          return false
        }
      }
    }
    if restart { onRestart?() }
  }
}
