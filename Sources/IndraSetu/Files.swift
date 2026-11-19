import Foundation
import IndraNode
import NIOCore
import TCPActorSystem
import _NIOFileSystem

package struct SharedFile: Codable, Sendable {
  package let name: String
  package let size: Int64
}

struct SharedDirectory: Sendable {
  let root: FilePath

  init(path: String) async throws {
    let root = FilePath(path)
    guard root.isAbsolute else { throw IndraError("Specify an absolute shared directory") }
    try await createPrivateDirectory(at: root)
    self.root = root
  }

  func path(for name: String) throws -> FilePath {
    guard Self.isValidName(name) else { throw IndraError("Invalid shared file name") }
    return root.appending(name)
  }

  private static func isValidName(_ name: String) -> Bool {
    !name.isEmpty && name.utf8.first != 0x2E && name.utf8.count <= 255
      && !name.utf8.contains(47) && !name.utf8.contains(92)
      && !name.unicodeScalars.contains(where: CharacterSet.controlCharacters.contains)
  }

  func list(send: @Sendable ([SharedFile]) async throws -> Void) async throws {
    try await FileSystem.shared.withDirectoryHandle(
      atPath: root, options: .init(followSymbolicLinks: true, closeOnExec: true)
    ) { directory in
      var page: [SharedFile] = []
      for try await entries in directory.listContents(recursive: false).batched() {
        try Task.checkCancellation()
        for entry in entries {
          guard let name = String(validating: entry.name), Self.isValidName(name),
            let info = try await FileSystem.shared.info(forFileAt: entry.path, infoAboutSymbolicLink: true),
            info.type == .regular
          else { continue }
          page.append(SharedFile(name: name, size: info.size))
          if page.count == 1024 {
            try await send(page)
            page.removeAll(keepingCapacity: true)
          }
        }
      }
      if !page.isEmpty { try await send(page) }
    }
  }
}

struct FileSource: DataSource {
  let handle: ReadFileHandle
  let info: FileInfo

  init(path: FilePath, followLinks: Bool) async throws {
    guard let name = path.lastComponent else { throw IndraError("Expected a regular file") }
    let parent = path.removingLastComponent()
    let handle = try await FileSystem.shared.withDirectoryHandle(
      atPath: parent.isEmpty ? FilePath(".") : parent, options: .init(followSymbolicLinks: true, closeOnExec: true)
    ) { directory in
      // SwiftNIO opens files relative to a directory handle with O_NONBLOCK.
      try await directory.openFile(
        forReadingAt: FilePath().appending(name), options: .init(followSymbolicLinks: followLinks, closeOnExec: true))
    }
    do {
      let info = try await handle.info()
      guard info.type == .regular else { throw IndraError("Expected a regular file") }
      self.info = info
      self.handle = handle
    } catch {
      await Task.detached { try? await handle.close() }.value
      throw error
    }
  }

  func read(at offset: Int64, maximumBytes: Int) async throws -> ByteBuffer {
    let bytes = try await handle.readChunk(fromAbsoluteOffset: offset, length: .bytes(Int64(maximumBytes)))
    if bytes.readableBytes == 0 {
      let final = try await handle.info()
      guard final.size == info.size, final.lastDataModificationTime == info.lastDataModificationTime else {
        throw IndraError("Source file changed during the transfer")
      }
    }
    return bytes
  }

  func close() async { await Task.detached { try? await handle.close() }.value }
}

struct FileSink: DataSink {
  private let file: StagedFile

  static func validate(size: Int64) throws {
    guard (0...1_099_511_627_776).contains(size) else { throw IndraError("File exceeds the 1 TiB limit") }
  }

  init(path: FilePath) async throws {
    guard try await FileSystem.shared.info(forFileAt: path, infoAboutSymbolicLink: true) == nil else {
      throw IndraError("Destination already exists: \(path)")
    }
    file = try await StagedFile(path: path, publication: .createOnly)
  }

  func write(_ bytes: ByteBuffer, at offset: Int64) async throws {
    try await file.write(bytes, at: offset)
  }

  func commit() async throws { try await file.commit() }

  func abort() async { await file.abort() }
}
