package import Foundation
package import NIOCore
import _NIOFileSystem

public struct FileEntry: Codable, Sendable {
  public let id: String
  public let name: String
  public let size: Int64
  package init(id: String, name: String, size: Int64) {
    self.id = id
    self.name = name
    self.size = size
  }
}

package struct FileCatalog: Sendable {
  package let root: String
  package let node: UUID

  @concurrent
  package func list() async throws(any Error) -> [FileEntry] {
    let root = self.root
    let entries = try await FileSystem.shared.withDirectoryHandle(atPath: FilePath(root)) { directory in
      var entries: [FileEntry] = []
      for try await entry in directory.listContents() {
        let name = entry.name.string
        guard validName(name),
          let info = try await FileSystem.shared.info(forFileAt: entry.path, infoAboutSymbolicLink: true),
          info.type == .regular
        else {
          continue
        }
        guard entries.count < 1024 else {
          throw TransferError("Shared directory exceeds 1024 files")
        }
        entries.append(FileEntry(id: sha256(Data(name.utf8)), name: name, size: info.size))
      }
      return entries.sorted { $0.name < $1.name }
    }
    return entries
  }

  private func entry(fileID: String) async throws(any Error) -> FileEntry {
    guard validDigest(fileID) else { throw TransferError("Invalid file ID") }
    guard let entry = try await list().first(where: { $0.id == fileID }) else {
      throw TransferError("File is unavailable")
    }
    return entry
  }

  @concurrent
  package func manifest(fileID: String, chunkSize: Int, progress: TransferProgressTracker?) async throws(any Error)
    -> Manifest
  {
    let entry = try await entry(fileID: fileID)
    return try await Manifest.scan(
      path: root + "/" + entry.name, name: nil, chunkSize: chunkSize, followSymbolicLinks: false, progress: progress)
  }

  @concurrent
  package func chunk(name: String, index: Int, chunkSize: Int) async throws(any Error) -> ByteBuffer {
    guard validName(name), (1_048_576...67_108_864).contains(chunkSize) else {
      throw TransferError("Invalid file name or chunk size")
    }
    let path = root + "/" + name
    guard let info = try await FileSystem.shared.info(forFileAt: FilePath(path), infoAboutSymbolicLink: true),
      info.type == .regular, (0...1_099_511_627_776).contains(info.size),
      index >= 0, Int64(index) < (info.size + Int64(chunkSize) - 1) / Int64(chunkSize)
    else { throw TransferError("Invalid chunk size or index") }
    let offset = Int64(index) * Int64(chunkSize)
    let length = Int(min(Int64(chunkSize), info.size - offset))
    return try await readRange(path: path, offset: offset, count: length, followSymbolicLinks: false)
  }
  package init(root: String, node: UUID) {
    self.root = root
    self.node = node
  }
}
