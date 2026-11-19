import Foundation
import NIOCore
package import SystemPackage
import _NIOFileSystem

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
  let journal = StagingJournal(parent: path.removingLastComponent())
  if !overwrite { try await journal.recover() }
  let file = try await StagedFile(path: path, publication: overwrite ? .replace : .createOnly, journal: journal)
  do {
    try await file.write(ByteBuffer(bytes: data), at: 0)
    try await file.commit()
  } catch {
    await file.abort()
    throw error
  }
}
