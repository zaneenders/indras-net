import Foundation
import NIOCore
import NIOFoundationEssentialsCompat
public import Setu
import _NIOFileSystem

package struct RestartAcknowledgement: Codable, Sendable {
  let restartRequested: Bool
  let exitCode: Int
}

public struct PeerInspection: Codable, Sendable {
  public let node: String
}

public struct TransferResult: Codable, Sendable {
  public let bytesTransferred: Int64
  public let elapsedSeconds: Double
  public let resumedChunks: Int
  public let receipt: TransferReceipt?
  public var mebibytesPerSecond: Double { Double(bytesTransferred) / elapsedSeconds / 1_048_576 }
  public var megabitsPerSecond: Double { Double(bytesTransferred) * 8 / elapsedSeconds / 1_000_000 }

  package init(
    bytesTransferred: Int64, resumedChunks: Int, started: ContinuousClock.Instant, receipt: TransferReceipt?
  ) {
    let elapsed = started.duration(to: .now).components
    self.bytesTransferred = bytesTransferred
    self.elapsedSeconds = max(0.000001, Double(elapsed.seconds) + Double(elapsed.attoseconds) / 1e18)
    self.resumedChunks = resumedChunks
    self.receipt = receipt
  }
}

public actor TransferClient {
  private let connection: PeerConnection
  private let temporary: TemporaryFiles
  private let recoveries: DownloadRecoveries
  private let recoveryOwner = UUID()
  private var downloads: Set<String> = []
  private var closed = false

  private init(
    connection: PeerConnection, temporary: TemporaryFiles, recoveries: DownloadRecoveries
  ) {
    self.connection = connection
    self.temporary = temporary
    self.recoveries = recoveries
  }

  public static func requestRestart(host: String, port: Int) async throws {
    guard !host.isEmpty, (1...65535).contains(port) else { throw TransferError("Invalid restart host or port") }
    let connection = PeerConnection(peer: Peer(host: host, port: port, connections: 1, chunkSize: defaultChunkSize))
    do {
      let (response, _) = try await connection.request(.restart, body: nil, timeout: .seconds(10))
      guard case .restart(let reply) = response, reply.restartRequested, reply.exitCode == 1 else {
        throw TransferError("Invalid restart acknowledgement")
      }
      await connection.close()
    } catch {
      await connection.close()
      throw error
    }
  }

  public static func inspect(host: String, port: Int) async throws -> PeerInspection {
    guard !host.isEmpty, (1...65535).contains(port) else { throw TransferError("Invalid peer host or port") }
    let connection = PeerConnection(peer: Peer(host: host, port: port, connections: 1, chunkSize: defaultChunkSize))
    do {
      let (response, _) = try await connection.request(.health, body: nil, timeout: .seconds(5))
      guard case .health(let node) = response else { throw TransferError("Invalid peer acknowledgement") }
      await connection.close()
      return PeerInspection(node: node.uuidString)
    } catch {
      await connection.close()
      throw error
    }
  }

  public static func connect(
    host: String, port: Int,
    connections: Int, chunkSize: Int, temporaryDirectory: String,
    recoveryBudgetBytes: Int64
  ) async throws(any Error) -> TransferClient {
    guard recoveryBudgetBytes >= 0 else { throw TransferError("Download recovery budget must not be negative") }
    let temporary = try await TemporaryFiles(directory: temporaryDirectory, protecting: [])
    do {
      return try await connect(
        host: host, port: port, connections: connections, chunkSize: chunkSize,
        temporary: temporary, recoveries: DownloadRecoveries(budgetBytes: recoveryBudgetBytes))
    } catch {
      await temporary.close()
      throw error
    }
  }

  static func connect(
    host: String, port: Int, connections: Int, chunkSize: Int,
    temporary: TemporaryFiles, recoveries: DownloadRecoveries
  ) async throws(any Error) -> TransferClient {
    guard !host.isEmpty, (1...65535).contains(port), (1...8).contains(connections),
      (1_048_576...67_108_864).contains(chunkSize)
    else {
      throw TransferError("Invalid host, port, connections (1–8), or chunk size (1–64 MiB)")
    }
    let peer = Peer(
      host: host, port: port, connections: connections,
      chunkSize: chunkSize)
    let connection = PeerConnection(peer: peer)
    do {
      let (response, _) = try await connection.request(.health, body: nil, timeout: .seconds(120))
      guard case .health = response else { throw TransferError("Invalid peer acknowledgement") }
      return TransferClient(connection: connection, temporary: temporary, recoveries: recoveries)
    } catch {
      await connection.close()
      throw error
    }
  }

  public func list() async throws(any Error) -> [FileEntry] {
    let (response, _) = try await connection.request(.list, body: nil, timeout: .seconds(120))
    guard case .files(let files) = response else { throw TransferError("Invalid file list response") }
    return files
  }

  public func request(fileID: String) async throws -> Manifest {
    guard validDigest(fileID) else { throw TransferError("Invalid file ID") }
    let (response, _) = try await connection.request(
      .manifest(fileID: fileID, chunkSize: connection.peer.chunkSize), body: nil, timeout: .seconds(120))
    guard case .manifest(let manifest) = response else { throw TransferError("Invalid manifest response") }
    try manifest.validate()
    guard sha256(Data(manifest.name.utf8)) == fileID, manifest.chunkSize == connection.peer.chunkSize else {
      throw TransferError("Download manifest does not match the requested file or chunk size")
    }
    return manifest
  }

  public func upload(
    file: String, destination: String?, createDestination: Bool,
    progress: TransferProgressTracker?
  ) async throws(any Error) -> TransferResult {
    guard !createDestination || destination != nil else {
      throw TransferError("Creating a destination requires a receiver directory path")
    }
    if let destination {
      guard validDestinationPath(destination) else {
        throw TransferError("Destination must be an absolute directory on the receiver")
      }
    }
    let path = URL(fileURLWithPath: file).path
    let started = ContinuousClock.now
    let manifest = try await Manifest.scan(
      path: path, name: nil, chunkSize: connection.peer.chunkSize, followSymbolicLinks: true, progress: progress)
    let client = connection
    let id = try manifest.uploadID(destination: destination)
    let owner = UUID()
    @Sendable func fetchStatus() async throws -> UploadStatus {
      let (response, _) = try await client.request(
        .begin(manifest: manifest, destination: destination, createDestination: createDestination, owner: owner),
        body: nil, timeout: .seconds(120))
      guard case .status(let status) = response else { throw TransferError("Invalid upload status") }
      _ = try status.validate(manifest: manifest, expectedID: id)
      return status
    }
    var readyToCommit = false
    do {
      let status = try await fetchStatus()
      let completed = try status.validate(manifest: manifest, expectedID: id)
      let pending = manifest.chunks.indices.filter { !completed.contains($0) }
      let reusedBytes = try completed.reduce(Int64(0)) { try $0 + Int64(manifest.length($1)) }
      await progress?.begin(.transferring, totalBytes: manifest.size, completedBytes: reusedBytes)
      if !status.committed {
        do {
          try await parallel(items: pending, jobs: client.peer.connections) { index in
            let length = try manifest.length(index)
            let data = try await readRange(
              path: path, offset: Int64(index) * Int64(manifest.chunkSize), count: length, followSymbolicLinks: true)
            let (reply, _) = try await client.request(
              .uploadChunk(id: status.id, index: index, encoding: .gzip),
              body: try await encodeChunk(data), timeout: .seconds(120))
            guard case .acknowledged = reply else { throw TransferError("Invalid chunk acknowledgement") }
            await progress?.advance(by: length)
          }
        } catch {
          try Task.checkCancellation()
          // Another uploader may have published and removed the shared transfer meanwhile.
          guard let confirmed = try? await fetchStatus(), confirmed.committed else { throw error }
        }
        await progress?.begin(.verifying, totalBytes: nil, completedBytes: 0)
        readyToCommit = true
        try await retryTransient {
          // Begin is idempotent and verifies existing destinations. Reconcile before every commit
          // attempt, including when both a commit acknowledgement and a later status reply were lost.
          let confirmed = try await fetchStatus()
          if !confirmed.committed {
            do {
              let (reply, _) = try await client.request(.commit(id: status.id), body: nil, timeout: .seconds(120))
              guard case .acknowledged = reply else { throw TransferError("Invalid publication acknowledgement") }
            } catch {
              try Task.checkCancellation()
              guard let confirmed = try? await fetchStatus(), confirmed.committed else { throw error }
            }
          }
        }
      }
      await progress?.begin(.completed, totalBytes: manifest.size, completedBytes: manifest.size)
      let bytes = try pending.reduce(Int64(0)) { try $0 + Int64(manifest.length($1)) }
      return TransferResult(
        bytesTransferred: bytes, resumedChunks: completed.count, started: started,
        receipt: TransferReceipt(manifest: manifest, destination: status.destination, peer: client.peer.host))
    } catch {
      // Keep a fully received upload for a retry of commit. The receiver's idle timeout
      // bounds retention; incomplete or explicitly cancelled uploads are still discarded.
      if !readyToCommit || Task.isCancelled {
        await Task.detached {
          _ = try? await client.request(.discard(id: id, owner: owner), body: nil, timeout: .seconds(5))
        }.value
      }
      throw error
    }
  }

  public func download(
    _ manifest: Manifest, to destination: String, progress: TransferProgressTracker?
  ) async throws(any Error) -> TransferResult {
    try manifest.validate()
    let path = URL(fileURLWithPath: destination).path
    guard downloads.insert(path).inserted else {
      throw TransferError("Download is already in progress")
    }
    defer { downloads.remove(path) }
    let started = ContinuousClock.now
    let client = connection
    guard try await FileSystem.shared.info(forFileAt: FilePath(path), infoAboutSymbolicLink: true) == nil else {
      throw TransferError("Download destination already exists")
    }
    let temporary = temporary
    try await preflightDestination(path, temporary: temporary)
    let partial = try await temporary.allocate(size: manifest.size)
    do {
      await progress?.begin(.transferring, totalBytes: manifest.size, completedBytes: 0)
      try await parallel(items: Array(manifest.chunks.indices), jobs: client.peer.connections) { index in
        let length = try manifest.length(index)
        let (reply, payload) = try await client.request(
          .downloadChunk(name: manifest.name, index: index, chunkSize: manifest.chunkSize, length: length),
          body: nil, timeout: .seconds(120))
        guard case .chunk = reply, let data = payload else { throw TransferError("Invalid download response") }
        guard sha256(data.readableBytesView) == manifest.chunks[index] else {
          throw TransferError("Download chunk integrity check failed")
        }
        try await writeRange(
          path: partial, offset: Int64(index) * Int64(manifest.chunkSize), data: data.readableBytesView)
        await progress?.advance(by: length)
      }
      try await publishDownload(partial: partial, destination: path, manifest: manifest, progress: progress)
    } catch {
      if !(error is DownloadPublicationError) { try? await temporary.discard(partial) }
      throw error
    }
    try await temporary.discard(partial)
    await progress?.begin(.completed, totalBytes: manifest.size, completedBytes: manifest.size)
    return TransferResult(
      bytesTransferred: manifest.size, resumedChunks: 0, started: started,
      receipt: TransferReceipt(manifest: manifest, destination: path, peer: client.peer.host))
  }

  public func finishDownload(
    recoveryID: String, to destination: String, progress: TransferProgressTracker?
  ) async throws(any Error) -> TransferResult {
    let payload = try await recoveries.take(recoveryID, owner: recoveryOwner)
    let path = URL(fileURLWithPath: destination).path
    let started = ContinuousClock.now
    do {
      try await publishDownload(
        partial: payload.partial, destination: path, manifest: payload.manifest, progress: progress)
      try await temporary.discard(payload.partial)
    } catch {
      if !(error is DownloadPublicationError) { try? await temporary.discard(payload.partial) }
      throw error
    }
    await progress?.begin(.completed, totalBytes: payload.manifest.size, completedBytes: payload.manifest.size)
    return TransferResult(
      bytesTransferred: 0, resumedChunks: payload.manifest.chunks.count, started: started,
      receipt: TransferReceipt(manifest: payload.manifest, destination: path, peer: connection.peer.host))
  }

  private func publishDownload(
    partial: String, destination: String, manifest: Manifest, progress: TransferProgressTracker?
  ) async throws(any Error) {
    await progress?.begin(.verifying, totalBytes: manifest.size, completedBytes: 0)
    try await verifyFile(path: partial, manifest: manifest, progress: progress)
    try Task.checkCancellation()
    do {
      try await publish(
        partial: partial, destination: destination, manifest: manifest, progress: progress, alreadyVerified: true)
    } catch {
      guard !Task.isCancelled,
        try await FileSystem.shared.info(forFileAt: FilePath(partial), infoAboutSymbolicLink: true) != nil
      else { throw error }
      let recovery: DownloadRecovery
      do {
        recovery = try await recoveries.retain(
          owner: recoveryOwner, manifest: manifest, partial: partial, temporary: temporary)
      } catch let retentionError {
        throw TransferError(
          "Verified download could not be placed at \(destination): \(describeError(error)). \(describeError(retentionError))"
        )
      }
      throw DownloadPublicationError(
        recovery: recovery, destination: destination,
        message:
          "Verified download could not be placed at \(destination): \(describeError(error)). Data retained for 15 minutes."
      )
    }
  }

  package func expireRecoveries(at time: Double) async throws(any Error) {
    try await recoveries.expire(at: time)
  }

  package func hasRecovery(_ id: String) async -> Bool {
    await recoveries.contains(id, owner: recoveryOwner)
  }

  package func discardRecovery(_ id: String) async throws(any Error) {
    try await recoveries.discard(id, owner: recoveryOwner)
  }

  public func close() async {
    guard !closed else {
      return
    }
    closed = true
    await recoveries.discardAll(owner: recoveryOwner)
    await connection.close()
    await temporary.close()
  }

  deinit {
    if !closed {
      let connection = connection
      let temporary = temporary
      let recoveries = recoveries
      let owner = recoveryOwner
      Task {
        await recoveries.discardAll(owner: owner)
        await connection.close()
        await temporary.close()
      }
    }
  }
}
