package import Foundation
import Logging
import _NIOFileSystem

package actor UploadStore {
  private struct Transfer: Sendable {
    package let manifest: Manifest
    package let destination: String
    package let state: Task<TransferState, any Error>
    package var owners: Set<UUID>
    package init(manifest: Manifest, destination: String, state: Task<TransferState, any Error>, owner: UUID) {
      self.manifest = manifest
      self.destination = destination
      self.state = state
      self.owners = [owner]
    }
  }

  package nonisolated let root: String
  package let quota: Int64
  package let temporary: TemporaryFiles
  private let maxActiveUploads: Int
  private let idleTimeout: Duration
  private var cleanup: Task<Void, Never>?
  private var transfers: [String: Transfer] = [:]
  private var nameLocks: [String: [CheckedContinuation<Void, Never>]] = [:]
  private var closed = false

  package init(
    root: String, quota: Int64, temporary: TemporaryFiles,
    maxActiveUploads: Int, idleTimeout: Duration
  ) async throws(any Error) {
    guard maxActiveUploads > 0, idleTimeout > .zero else {
      throw TransferError("Upload limit and idle timeout must be positive")
    }
    var rootPath = FilePath(root)
    if rootPath.isRelative {
      rootPath = try await FileSystem.shared.currentWorkingDirectory.appending(root)
    }
    self.root = rootPath.string
    self.quota = quota
    try await FileSystem.shared.createDirectory(
      at: rootPath, withIntermediateDirectories: true, permissions: .ownerReadWriteExecute)
    self.temporary = temporary
    self.maxActiveUploads = maxActiveUploads
    self.idleTimeout = idleTimeout
  }

  package func startCleanup() {
    guard cleanup == nil, !closed else { return }
    let interval = min(idleTimeout, .seconds(30))
    cleanup = Task { [weak self] in
      while !Task.isCancelled {
        do { try await Task.sleep(for: interval) } catch { return }
        guard let store = self else { return }
        do { try await store.expireIdleUploads(at: .now) } catch {
          Logger(label: "setu.storage").warning("Upload cleanup failed: \(describeError(error))")
        }
      }
    }
  }

  package func expireIdleUploads(at now: ContinuousClock.Instant) async throws(any Error) {
    let cutoff = now - idleTimeout
    var failure: (any Error)?
    for (id, transfer) in transfers {
      // Publication can hold a name lock while verifying a large file. Skip it so that
      // one busy file cannot delay cleanup of every other abandoned upload.
      guard !closed, nameLocks[transfer.destination] == nil else { continue }
      nameLocks[transfer.destination] = []
      do {
        if let current = transfers[id], try await current.state.value.expire(before: cutoff) {
          transfers.removeValue(forKey: id)
        }
      } catch {
        failure = error
      }
      unlock(transfer.destination)
    }
    if let failure { throw failure }
  }

  package func begin(
    _ manifest: Manifest, destination directoryPath: String?, createDestination: Bool,
    owner: UUID, progress: TransferProgressTracker?
  ) async throws(any Error) -> UploadStatus {
    try manifest.validate()
    guard !createDestination || directoryPath != nil else {
      throw TransferError("Creating a destination requires a receiver directory path")
    }
    if let directoryPath, !validDestinationPath(directoryPath) {
      throw TransferError("Destination must be an absolute directory on the receiver")
    }
    let directory = URL(fileURLWithPath: directoryPath ?? root).standardizedFileURL.resolvingSymlinksInPath().path
    let privateDirectory = URL(fileURLWithPath: temporary.directory).resolvingSymlinksInPath().path
    guard directory != privateDirectory, !directory.hasPrefix(privateDirectory + "/") else {
      throw TransferError("Destination must be outside Setu's private temporary directory")
    }
    let destination = FilePath(directory).appending(manifest.name).string
    await lock(destination)
    defer { unlock(destination) }
    guard !closed else { throw TransferError("Receiver is stopping") }
    if createDestination {
      try await FileSystem.shared.createDirectory(
        at: FilePath(directory), withIntermediateDirectories: true, permissions: .ownerReadWriteExecute)
      guard !closed else { throw TransferError("Receiver is stopping") }
    }
    let id = try manifest.uploadID(destination: directoryPath)
    guard !transfers.contains(where: { $0.key != id && $0.value.destination == destination }) else {
      throw TransferError("Another transfer owns this name")
    }
    if let transfer = transfers[id] {
      var status = try await transfer.state.value.status(id: id)
      transfers[id]?.owners.insert(owner)
      status.destination = destination
      return status
    }

    if try await FileSystem.shared.info(forFileAt: FilePath(destination), infoAboutSymbolicLink: true) != nil {
      // Deduplication uses the actual file, without retaining a completed-transfer receipt.
      try await verifyFile(path: destination, manifest: manifest, progress: progress)
      return UploadStatus(id: id, completed: Array(manifest.chunks.indices), committed: true, destination: destination)
    }
    guard !closed else { throw TransferError("Receiver is stopping") }
    try await preflightDestination(destination, temporary: temporary)
    guard !closed else { throw TransferError("Receiver is stopping") }
    // Preflight suspends the actor. Recheck capacity immediately before reserving it.
    guard transfers.count < maxActiveUploads else { throw TransferError("Receiver active upload limit reached") }
    let reserved = transfers.values.reduce(manifest.size) { $0 + $1.manifest.size }
    guard reserved <= quota else { throw TransferError("Receiver active upload quota reached") }
    let temporary = temporary
    let task = Task {
      let partial = try await temporary.allocate(size: manifest.size)
      return TransferState(manifest: manifest, partial: partial, temporary: temporary)
    }
    transfers[id] = Transfer(manifest: manifest, destination: destination, state: task, owner: owner)
    do {
      let state = try await task.value
      guard !closed else { throw TransferError("Receiver is stopping") }
      var status = await state.status(id: id)
      status.destination = destination
      return status
    } catch {
      transfers.removeValue(forKey: id)
      if let state = try? await task.value { try? await state.discard() }
      throw error
    }
  }

  private func state(_ id: String) async throws(any Error) -> TransferState {
    guard validDigest(id), let transfer = transfers[id] else { throw TransferError("Unknown transfer") }
    return try await transfer.state.value
  }

  package func reserve(_ id: String, index: Int) async throws(any Error) -> (Manifest, String)? {
    try await state(id).reserve(index)
  }

  package func release(_ id: String, index: Int) async {
    if let state = try? await state(id) { await state.release(index) }
  }

  package func complete(_ id: String, index: Int) async throws(any Error) {
    try await state(id).complete(index)
  }

  package func commit(_ id: String, progress: TransferProgressTracker?) async throws(any Error) {
    guard let transfer = transfers[id] else { throw TransferError("Unknown transfer") }
    await lock(transfer.destination)
    defer { unlock(transfer.destination) }
    let state = try await state(id)
    try await state.finish(destination: transfer.destination, progress: progress)
    transfers.removeValue(forKey: id)
  }

  package func discard(_ id: String, owner: UUID) async throws(any Error) {
    guard let transfer = transfers[id] else { return }
    await lock(transfer.destination)
    defer { unlock(transfer.destination) }
    guard let current = transfers[id], current.owners.contains(owner) else { return }
    if current.owners.count > 1 {
      transfers[id]?.owners.remove(owner)
      return
    }
    if let transfer = transfers[id], let state = try? await transfer.state.value {
      try await state.discard()
      transfers.removeValue(forKey: id)
    }
  }

  package func close() async {
    closed = true
    cleanup?.cancel()
    cleanup = nil
    let active = transfers.values.map(\.state)
    transfers.removeAll()
    for task in active {
      if let state = try? await task.value { try? await state.discard() }
    }
  }

  deinit { cleanup?.cancel() }

  private func lock(_ name: String) async {
    if nameLocks[name] != nil {
      await withCheckedContinuation { nameLocks[name, default: []].append($0) }
    } else {
      nameLocks[name] = []
    }
  }

  private func unlock(_ name: String) {
    if nameLocks[name]?.isEmpty == false {
      nameLocks[name]?.removeFirst().resume()
    } else {
      nameLocks.removeValue(forKey: name)
    }
  }
}
