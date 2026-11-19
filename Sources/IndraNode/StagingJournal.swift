import Foundation
import NIOCore
package import SystemPackage
import _NIOFileSystem

#if canImport(Darwin)
import Darwin
#elseif canImport(Musl)
import Musl
#else
import Glibc
#endif

/// Each named stage has a durable record before it is created. Its open record holds the writer's lease.
package struct StagingJournal: Sendable {
  private let parent: FilePath
  package init(parent: FilePath) { self.parent = parent }

  private struct Location: Codable {
    let parent: FilePath
    let device: DeviceID
    let inode: Inode

    init(path: FilePath, directory: DirectoryFileHandle) async throws {
      let stat = try await directory.withUnsafeDescriptor { try $0.stat(retryOnInterrupt: true) }
      parent = path
      device = stat.deviceID
      inode = stat.inode
    }

    func matches(_ directory: DirectoryFileHandle) async throws -> Bool {
      try await directory.withUnsafeDescriptor {
        let stat = try $0.stat(retryOnInterrupt: true)
        return device == stat.deviceID && inode == stat.inode
      }
    }
  }

  struct Registration: Sendable {
    let name: FilePath
    let directory: DirectoryFileHandle
    let record: ReadWriteFileHandle
    var temporary: FilePath { FilePath(".indra-" + name.string) }

    func finish(removeRecord: Bool) async {
      await withTaskCancellationShield {
        if removeRecord { try? await directory.unlink(name) }
        try? await record.close()
        try? await directory.close()
      }
    }
  }

  private func openDirectory(create: Bool) async throws -> DirectoryFileHandle {
    let options = OpenOptions.Directory(followSymbolicLinks: false, closeOnExec: true)
    return try await FileSystem.shared.withDirectoryHandle(
      atPath: parent, options: .init(followSymbolicLinks: true, closeOnExec: true)
    ) { base in
      if create {
        try await base.withUnsafeDescriptor { fd in
          guard unsafe mkdirat(fd.rawValue, ".indra-staging", 0o700) == 0 || errno == EEXIST else {
            throw Errno(rawValue: errno)
          }
        }
        try await base.synchronize()
      }
      let directory = try await base.openDirectory(atPath: ".indra-staging", options: options)
      do {
        let info = try await directory.info()
        guard info.userID.rawValue == geteuid(), info.permissions.rawValue & 0o077 == 0 else {
          throw IndraError("Expected a private staging journal")
        }
        return directory
      } catch {
        await withTaskCancellationShield { try? await directory.close() }
        throw error
      }
    }
  }

  func register(parent path: FilePath, directory parent: DirectoryFileHandle) async throws -> Registration {
    let location = try await Location(path: path, directory: parent)
    let bytes = try JSONEncoder().encode(location)
    let directory = try await openDirectory(create: true)
    do {
      while true {
        try Task.checkCancellation()
        let name = FilePath(UUID().uuidString)
        let record = try await directory.openFile(
          forReadingAndWritingAt: name,
          options: .init(
            existingFile: .none, newFile: .init(permissions: .ownerReadWrite, transactionalCreation: false),
            followSymbolicLinks: false, closeOnExec: true))
        do {
          // Recovery may have opened this just-created record before we obtained its lock.
          guard try await lock(record),
            try await record.withUnsafeDescriptor({ try $0.stat(retryOnInterrupt: true).linkCount == 1 })
          else {
            await withTaskCancellationShield { try? await record.close() }
            continue
          }
          guard try await record.write(contentsOf: bytes, toAbsoluteOffset: 0) == bytes.count else {
            throw IndraError("Incomplete staging journal record")
          }
          try await record.synchronize()
          try await directory.synchronize()
          return Registration(name: name, directory: directory, record: record)
        } catch {
          await withTaskCancellationShield { try? await record.close() }
          throw error
        }
      }
    } catch {
      await withTaskCancellationShield { try? await directory.close() }
      throw error
    }
  }

  private func lock(_ record: ReadWriteFileHandle) async throws -> Bool {
    try await record.withUnsafeDescriptor { fd in
      if flock(fd.rawValue, LOCK_EX | LOCK_NB) == 0 { return true }
      if errno == EWOULDBLOCK { return false }
      throw Errno(rawValue: errno)
    }
  }

  package func recover() async throws {
    let directory: DirectoryFileHandle
    do { directory = try await openDirectory(create: false) } catch let error as FileSystemError
      where error.code == .notFound
    { return }
    defer { await withTaskCancellationShield { try? await directory.close() } }
    for try await entry in directory.listContents(recursive: false) {
      guard entry.type == .regular, let name = String(validating: entry.name),
        UUID(uuidString: name)?.uuidString == name
      else { continue }
      let record: ReadWriteFileHandle
      do {
        record = try await directory.openFile(
          forReadingAndWritingAt: FilePath(name),
          options: .init(
            existingFile: .open, newFile: nil, followSymbolicLinks: false, closeOnExec: true))
      } catch let error as FileSystemError where error.code == .notFound { continue }
      defer { await withTaskCancellationShield { try? await record.close() } }
      guard try await lock(record) else { continue }
      let bytes = try await record.readToEnd(fromAbsoluteOffset: 0, maximumSizeAllowed: .bytes(65536))
      if let location = try? JSONDecoder().decode(Location.self, from: Data(bytes.readableBytesView)) {
        do {
          let reclaimed = try await FileSystem.shared.withDirectoryHandle(
            atPath: location.parent, options: .init(followSymbolicLinks: true, closeOnExec: true)
          ) { parent in
            guard try await location.matches(parent) else { return false }
            try await parent.unlink(FilePath(".indra-" + name))
            try await parent.synchronize()
            return true
          }
          if !reclaimed { continue }
        } catch {
          try Task.checkCancellation()
          // Keep the record for a later restart if this output directory is offline or read-only.
          continue
        }
      }
      // An incomplete record is possible only before creation of the named stage.
      try await directory.unlink(FilePath(name))
    }
  }
}

extension DirectoryFileHandle {
  func unlink(_ name: FilePath) async throws {
    try await withUnsafeDescriptor { fd in
      let result = unsafe name.withPlatformString { unsafe unlinkat(fd.rawValue, $0, 0) }
      guard result == 0 || errno == ENOENT else { throw Errno(rawValue: errno) }
    }
  }
}
