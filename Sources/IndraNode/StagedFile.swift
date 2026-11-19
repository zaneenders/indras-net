import Foundation
package import NIOCore
package import SystemPackage
import TCPActorSystem
import _NIOFileSystem

#if canImport(Darwin)
import Darwin
#elseif canImport(Musl)
import Musl
#else
import Glibc
#endif

/// One writer owns the staging file until commit succeeds or abort finishes.
package struct StagedFile: Sendable {
  package enum Publication: Sendable { case createOnly, replace }

  private let directory: DirectoryFileHandle
  private let file: WriteFileHandle
  private let registration: StagingJournal.Registration?
  private let destination: FilePath
  private let publication: Publication
  var synchronizeDirectory: @Sendable (DirectoryFileHandle) async throws -> Void = { try await $0.synchronize() }

  package init(path: FilePath, publication: Publication, journal: StagingJournal) async throws {
    guard let name = path.lastComponent, name.kind == .regular else { throw IndraError("Expected a file path") }
    let parent = try await absolutePath(path).removingLastComponent()
    let directory = try await FileSystem.shared.openDirectory(
      atPath: parent, options: .init(followSymbolicLinks: true, closeOnExec: true))
    self.directory = directory
    self.destination = FilePath(root: nil, components: name)
    self.publication = publication
    do {
      #if os(Linux)
      if publication == .createOnly, let file = try await Self.anonymousFile(in: directory) {
        self.file = file
        self.registration = nil
        return
      }
      #endif
      let registration = try await journal.register(parent: parent, directory: directory)
      do {
        self.file = try await directory.openFile(
          forWritingAt: registration.temporary,
          options: .init(
            existingFile: .none, newFile: .init(permissions: .ownerReadWrite, transactionalCreation: false),
            followSymbolicLinks: false, closeOnExec: true))
      } catch {
        await registration.finish(removeRecord: true)
        throw error
      }
      self.registration = registration
    } catch {
      await withTaskCancellationShield { try? await directory.close() }
      throw error
    }
    do { try await file.replacePermissions(.ownerReadWrite) } catch {
      await abort()
      throw error
    }
  }

  #if os(Linux)
  private static func anonymousFile(in directory: DirectoryFileHandle) async throws -> WriteFileHandle? {
    let fd: FileDescriptor? = try await directory.withUnsafeDescriptor { parent in
      #if canImport(Musl)
      let temporary = O_TMPFILE
      #else
      let temporary = __O_TMPFILE
      #endif
      let value = unsafe openat(parent.rawValue, ".", temporary | O_RDWR | O_CLOEXEC, 0o600)
      if value < 0 {
        if errno == EOPNOTSUPP || errno == EINVAL || errno == EISDIR { return nil }
        throw Errno(rawValue: errno)
      }
      let descriptor = FileDescriptor(rawValue: value)
      guard fchmod(value, 0o600) == 0 else {
        let error = Errno(rawValue: errno)
        try? descriptor.close()
        throw error
      }
      return descriptor
    }
    guard let fd else { return nil }
    defer { try? fd.close() }
    // Reopen the unnamed inode under NIO ownership. Publication uses the same procfs interface.
    do {
      return try await FileSystem.shared.openFile(
        forWritingAt: FilePath("/proc/self/fd/\(fd.rawValue)"),
        options: .init(existingFile: .open, newFile: nil, followSymbolicLinks: true, closeOnExec: true))
    } catch let error as FileSystemError where error.code == .notFound || error.code == .permissionDenied {
      return nil
    }
  }
  #endif

  package func write(_ bytes: ByteBuffer, at offset: Int64) async throws {
    let count = try await file.write(contentsOf: bytes.readableBytesView, toAbsoluteOffset: offset)
    guard count == bytes.readableBytes else { throw IndraError("Incomplete file write") }
  }

  @concurrent package func commit() async throws {
    try await file.synchronize()
    if let registration {
      try await file.close()
      try await directory.withUnsafeDescriptor { descriptor in
        let fd = descriptor.rawValue
        let result = unsafe registration.temporary.withPlatformString { source in
          unsafe destination.withPlatformString { target in
            switch publication {
            case .createOnly: unsafe linkat(fd, source, fd, target, 0)
            case .replace: unsafe renameat(fd, source, fd, target)
            }
          }
        }
        guard result == 0 else { throw Errno(rawValue: errno) }
      }
    } else {
      #if os(Linux)
      let fd = try await file.withUnsafeDescriptor { descriptor in
        let value = fcntl(descriptor.rawValue, F_DUPFD_CLOEXEC, 0)
        guard value >= 0 else { throw Errno(rawValue: errno) }
        return FileDescriptor(rawValue: value)
      }
      defer { try? fd.close() }
      try await directory.withUnsafeDescriptor { parent in
        let source = "/proc/self/fd/\(fd.rawValue)"
        let result = unsafe destination.withPlatformString {
          unsafe linkat(AT_FDCWD, source, parent.rawValue, $0, AT_SYMLINK_FOLLOW)
        }
        guard result == 0 else { throw Errno(rawValue: errno) }
      }
      #else
      preconditionFailure("Named staging requires a journal registration")
      #endif
    }

    // From here on the destination is visible, including when synchronization fails.
    try await withTaskCancellationShield {
      defer {
        try? await file.close()
        try? await directory.close()
      }
      do {
        if let registration, publication == .createOnly { try await directory.unlink(registration.temporary) }
        try await synchronizeDirectory(directory)
        await registration?.finish(removeRecord: true)
      } catch {
        await registration?.finish(removeRecord: false)
        throw UnconfirmedPublication(detail: String(describing: error))
      }
    }
  }

  package func abort() async {
    await withTaskCancellationShield {
      try? await file.close()
      if let registration {
        do {
          try await directory.unlink(registration.temporary)
          try await directory.synchronize()
          await registration.finish(removeRecord: true)
        } catch {
          await registration.finish(removeRecord: false)
        }
      }
      try? await directory.close()
    }
  }
}
