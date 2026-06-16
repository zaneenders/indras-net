import Foundation
import _NIOFileSystem
import _NIOFileSystemFoundationCompat
import Synchronization
import SystemPackage

/// Persists durable Raft state (`currentTerm`, `votedFor`, log entries).
package protocol RaftStore: Sendable {
  func load() async throws -> PersistentRaftState?
  func save(_ state: PersistentRaftState) async throws
}

/// In-memory store for tests and nodes that do not need crash recovery.
package final class InMemoryRaftStore: RaftStore, Sendable {
  private let storage = Mutex<PersistentRaftState?>(nil)

  package init() {}

  package func load() async throws -> PersistentRaftState? {
    storage.withLock { $0 }
  }

  package func save(_ state: PersistentRaftState) async throws {
    storage.withLock { $0 = state }
  }
}

/// File-backed store using NIOFileSystem with write-to-temp, fsync, and atomic replace.
package struct FileRaftStore: RaftStore {
  private let directory: FilePath
  private let fileSystem: FileSystem
  private let maximumFileSize: ByteCount

  package init(
    directory: URL,
    fileSystem: FileSystem = .shared,
    maximumFileSize: ByteCount = .mebibytes(64)
  ) {
    self.directory = FilePath(directory.path(percentEncoded: false))
    self.fileSystem = fileSystem
    self.maximumFileSize = maximumFileSize
  }

  private var statePath: FilePath {
    childPath("raft-state.json")
  }

  private var tempPath: FilePath {
    childPath("raft-state.json.tmp")
  }

  package func load() async throws -> PersistentRaftState? {
    guard try await fileSystem.info(forFileAt: statePath) != nil else {
      return nil
    }
    let data = try await Data(
      contentsOf: statePath,
      maximumSizeAllowed: maximumFileSize,
      fileSystem: fileSystem
    )
    return try JSONDecoder().decode(PersistentRaftState.self, from: data)
  }

  package func save(_ state: PersistentRaftState) async throws {
    try await fileSystem.createDirectory(
      at: directory,
      withIntermediateDirectories: true,
      permissions: nil
    )

    let data = try JSONEncoder().encode(state)

    if try await fileSystem.info(forFileAt: tempPath) != nil {
      try await fileSystem.removeItem(at: tempPath)
    }

    try await fileSystem.withFileHandle(
      forWritingAt: tempPath,
      options: .newFile(replaceExisting: true)
    ) { handle in
      try await handle.write(contentsOf: data, toAbsoluteOffset: 0)
      try await handle.synchronize()
    }

    try await fileSystem.replaceItem(at: statePath, withItemAt: tempPath)
  }

  private func childPath(_ name: String) -> FilePath {
    var path = directory
    path.append(name)
    return path
  }
}
