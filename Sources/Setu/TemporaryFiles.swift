import Foundation
import NIOCore
import _NIOFileSystem

#if canImport(Darwin)
import Darwin
#elseif canImport(Musl)
import Musl
#else
import Glibc
#endif

package actor TemporaryFiles {
  package nonisolated let directory: String
  private var lock: WriteFileHandle?
  private var parent: TemporaryFiles?
  private var files: [String: Task<Void, any Error>] = [:]
  private var closed = false

  private init(parent: TemporaryFiles) {
    directory = parent.directory
    self.parent = parent
  }

  // Clients share the directory lease while tracking and cleaning only their own payloads.

  package func scope() throws(TransferError) -> TemporaryFiles {
    guard !closed else { throw TransferError("Temporary directory is closed") }
    return TemporaryFiles(parent: self)
  }

  package init(directory: String, protecting directories: [String]) async throws(any Error) {
    guard directory.hasPrefix("/") else { throw TransferError("Temporary directory must be an absolute path") }
    let path = URL(fileURLWithPath: directory).standardizedFileURL
    let resolved = path.resolvingSymlinksInPath().path
    guard !directories.contains(where: { URL(fileURLWithPath: $0).resolvingSymlinksInPath().path == resolved }) else {
      throw TransferError("Temporary directory must be separate from shared files")
    }
    self.directory = path.path
    try await FileSystem.shared.createDirectory(
      at: FilePath(path.path), withIntermediateDirectories: true, permissions: .ownerReadWriteExecute)
    guard let info = try await FileSystem.shared.info(forFileAt: FilePath(path.path), infoAboutSymbolicLink: true),
      info.type == .directory, info.permissions.rawValue & 0o077 == 0, info.userID.rawValue == geteuid()
    else {
      throw TransferError("Temporary directory must be owned by this user and private (0700)")
    }
    var options = OpenOptions.Write.modifyFile(createIfNecessary: true, permissions: .ownerReadWrite)
    options.followSymbolicLinks = false
    options.closeOnExec = true
    let handle = try await FileSystem.shared.openFile(forWritingAt: FilePath(path.path + "/.lock"), options: options)
    do {
      try await handle.withUnsafeDescriptor { descriptor in
        guard flock(descriptor.rawValue, LOCK_EX | LOCK_NB) == 0 else {
          throw TransferError("Another node owns this temporary directory")
        }
      }
      try await Self.clearPayloads(in: path.path)
      lock = handle
    } catch {
      _ = await Task.detached { try? await handle.close() }.value
      throw error
    }
  }

  package func allocate(size: Int64) async throws(any Error) -> String {
    guard !closed else { throw TransferError("Temporary directory is closed") }
    let path = directory + "/.transfer-\(UUID().uuidString).partial"
    let task = Task { try await allocateFile(path: path, size: size) }
    files[path] = task
    do {
      try await task.value
      guard !closed else { throw TransferError("Temporary directory is closed") }
      return path
    } catch {
      files.removeValue(forKey: path)
      try? await discardFile(path)
      throw error
    }
  }

  package func discard(_ path: String) async throws(any Error) {
    guard let allocation = files[path] else { return }
    _ = try? await allocation.value
    try await discardFile(path)
    files.removeValue(forKey: path)
  }

  package func close() async {
    guard !closed else { return }
    closed = true
    let handle = lock
    lock = nil
    let allocations = files
    files.removeAll()
    let parent = parent
    self.parent = nil
    await Task.detached {
      await Self.cleanup(allocations, lock: handle, parent: parent)
    }.value
  }

  private static func cleanup(
    _ files: [String: Task<Void, any Error>], lock: WriteFileHandle?, parent: TemporaryFiles?
  ) async {
    for (path, allocation) in files {
      _ = try? await allocation.value
      try? await discardFile(path)
    }
    if let lock { try? await lock.close() }
    withExtendedLifetime(parent) {}
  }

  private static func clearPayloads(in directory: String) async throws(any Error) {
    try await FileSystem.shared.withDirectoryHandle(atPath: FilePath(directory)) { handle in
      for try await entry in handle.listContents() {
        let name = entry.name.string
        guard name.hasPrefix(".transfer-"), name.hasSuffix(".partial"),
          UUID(uuidString: String(name.dropFirst(10).dropLast(8))) != nil,
          let info = try await FileSystem.shared.info(forFileAt: entry.path, infoAboutSymbolicLink: true),
          info.type == .regular || info.type == .symlink
        else { continue }
        _ = try await FileSystem.shared.removeItem(at: entry.path, recursively: false)
      }
    }
  }

  deinit {
    if !closed {
      let files = files
      let lock = lock
      let parent = parent
      Task.detached {
        await Self.cleanup(files, lock: lock, parent: parent)
      }
    }
  }
}
