import Foundation
package import NIOCore
package import SystemPackage
import _NIOFileSystem

#if canImport(Darwin)
import Darwin
#elseif canImport(Musl)
import Musl
#else
import Glibc
#endif

package func absolutePath(_ path: FilePath) async throws -> FilePath {
  if path.isAbsolute { return path }
  return try await FileSystem.shared.currentWorkingDirectory.pushing(path)
}

package func createPrivateDirectory(at path: FilePath) async throws {
  let fileSystem = FileSystem.shared
  let path = try await absolutePath(path)
  if path.components.contains(where: { $0.kind == .parentDirectory }) {
    // NIO normalizes paths before mkdir, which can change where `..` points after a symlink.
    guard try await fileSystem.info(forFileAt: path, infoAboutSymbolicLink: false)?.type == .directory else {
      throw IndraError("Specify a path without '..' to create a directory: \(path)")
    }
    return
  }
  try await fileSystem.createDirectory(
    at: path, withIntermediateDirectories: true, permissions: .ownerReadWriteExecute)
}

func readNodeJSON<Value: Decodable>(_ type: Value.Type, from path: String) async throws -> Value {
  let path = try await absolutePath(FilePath(path))
  guard let name = path.lastComponent, name.kind == .regular else { throw IndraError("Expected a file path: \(path)") }
  let data = try await FileSystem.shared.withDirectoryHandle(
    atPath: path.removingLastComponent(), options: .init(followSymbolicLinks: true, closeOnExec: true)
  ) { directory in
    try await directory.withFileHandle(
      forReadingAt: FilePath(root: nil, components: name), options: .init(followSymbolicLinks: true, closeOnExec: true)
    ) { file in
      guard try await file.info().type == .regular else { throw IndraError("Expected a regular file: \(path)") }
      let buffer = try await file.readToEnd(fromAbsoluteOffset: 0, maximumSizeAllowed: .unlimited)
      return Data(buffer.readableBytesView)
    }
  }
  return try JSONDecoder().decode(type, from: data)
}

func writeNodeJSON(_ value: some Encodable, to path: String, overwrite: Bool) async throws {
  let path = try await absolutePath(FilePath(path))
  guard path.lastComponent?.kind == .regular else { throw IndraError("Expected a file path: \(path)") }
  let encoder = JSONEncoder()
  encoder.outputFormatting = [.prettyPrinted, .sortedKeys, .withoutEscapingSlashes]
  var data = try encoder.encode(value)
  data.append(10)

  try await createPrivateDirectory(at: path.removingLastComponent())
  let file = try await StagedFile(path: path, publication: overwrite ? .replace : .createOnly)
  do {
    try await file.write(ByteBuffer(bytes: data), at: 0)
    try await file.commit()
  } catch {
    await file.abort()
    throw error
  }
}

/// One writer owns the staging file until commit succeeds or abort finishes.
package struct StagedFile: Sendable {
  package enum Publication: Sendable { case createOnly, replace }

  private let directory: DirectoryFileHandle
  private let file: WriteFileHandle
  private let temporary: FilePath
  private let destination: FilePath
  private let publication: Publication

  package init(path: FilePath, publication: Publication) async throws {
    guard let name = path.lastComponent, name.kind == .regular else { throw IndraError("Expected a file path") }
    let absolute = try await absolutePath(path)
    let directory = try await FileSystem.shared.openDirectory(
      atPath: absolute.removingLastComponent(), options: .init(followSymbolicLinks: true, closeOnExec: true))
    let temporary = FilePath(".indra-" + UUID().uuidString)
    do {
      self.file = try await directory.openFile(
        forWritingAt: temporary,
        options: .init(
          existingFile: .none, newFile: .init(permissions: .ownerReadWrite, transactionalCreation: false),
          followSymbolicLinks: false, closeOnExec: true))
    } catch {
      await withTaskCancellationShield { try? await directory.close() }
      throw error
    }
    self.directory = directory
    self.temporary = temporary
    self.destination = FilePath(root: nil, components: name)
    self.publication = publication
    do {
      try await file.replacePermissions(.ownerReadWrite)
    } catch {
      await abort()
      throw error
    }
  }

  package func write(_ bytes: ByteBuffer, at offset: Int64) async throws {
    let count = try await file.write(contentsOf: bytes.readableBytesView, toAbsoluteOffset: offset)
    guard count == bytes.readableBytes else { throw IndraError("Incomplete file write") }
  }

  package func commit() async throws {
    try await file.synchronize()
    try await file.close()
    try await directory.withUnsafeDescriptor { descriptor in
      let fd = descriptor.rawValue
      let result = unsafe temporary.withPlatformString { source in
        unsafe destination.withPlatformString { target in
          switch publication {
          case .createOnly: unsafe linkat(fd, source, fd, target, 0)
          case .replace: unsafe renameat(fd, source, fd, target)
          }
        }
      }
      guard result == 0 else { throw Errno(rawValue: errno) }
    }
    await cleanUp(removeTemporary: publication == .createOnly)
  }

  package func abort() async {
    await withTaskCancellationShield { try? await file.close() }
    await cleanUp(removeTemporary: true)
  }

  private func cleanUp(removeTemporary: Bool) async {
    await withTaskCancellationShield {
      if removeTemporary {
        _ = try? await directory.withUnsafeDescriptor { descriptor in
          unsafe temporary.withPlatformString { unsafe unlinkat(descriptor.rawValue, $0, 0) }
        }
      }
      try? await directory.close()
    }
  }
}
