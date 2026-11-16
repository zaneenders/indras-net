import Crypto
public import Foundation
import NIOCore
import NIOFoundationEssentialsCompat
import _NIOFileSystem

public struct TransferError: Error, LocalizedError, CustomStringConvertible, Sendable {
  public let description: String
  public var errorDescription: String? { description }
  public init(_ description: String) {
    self.description = description
  }
}

package func describeError(_ error: any Error) -> String {
  // These Swift errors carry useful causes in their descriptions, but Foundation's
  // default NSError bridge otherwise replaces them with a generic domain/code message.
  if let error = error as? FileSystemError { return error.description }
  if let error = error as? IOError { return error.description }
  return error.localizedDescription
}

package func hex<D: Sequence>(_ bytes: D) -> String where D.Element == UInt8 {
  bytes.map {
    let value = String($0, radix: 16)
    return $0 < 16 ? "0" + value : value
  }.joined()
}

package func sha256(_ data: some DataProtocol) -> String {
  hex(SHA256.hash(data: data))
}

package func jsonData<T: Encodable>(_ value: T) throws(any Error) -> Data {
  let encoder = JSONEncoder()
  encoder.outputFormatting = [.sortedKeys]
  return try encoder.encode(value)
}

package func validName(_ name: String) -> Bool {
  !name.isEmpty && name.utf8.count <= 240 && name != "." && name != ".."
    && !name.hasPrefix(".") && !name.contains("/") && !name.contains("\\")
    && !name.unicodeScalars.contains { CharacterSet.controlCharacters.contains($0) }
}

package func validDigest(_ digest: String) -> Bool {
  digest.utf8.count == 64 && digest.utf8.allSatisfy { (48...57).contains($0) || (97...102).contains($0) }
}

public struct Manifest: Codable, Equatable, Sendable {
  public let name: String
  public let size: Int64
  public let chunkSize: Int
  public let digest: String
  public let chunks: [String]

  package var id: String {
    get throws(any Error) {
      return sha256(try jsonData(self))
    }
  }

  public func validate() throws(TransferError) {
    guard validName(name), size >= 0, size <= 1_099_511_627_776,
      (1_048_576...67_108_864).contains(chunkSize), validDigest(digest),
      chunks.count <= 131_072,
      Int64(chunks.count) == (size + Int64(chunkSize) - 1) / Int64(chunkSize),
      chunks.allSatisfy(validDigest)
    else {
      throw TransferError("Invalid file manifest")
    }
  }

  public func length(_ index: Int) throws(TransferError) -> Int {
    guard chunks.indices.contains(index) else {
      throw TransferError("Invalid chunk index")
    }
    return Int(min(Int64(chunkSize), size - Int64(index) * Int64(chunkSize)))
  }

  @concurrent
  public static func scan(
    path: String, name: String?, chunkSize: Int,
    followSymbolicLinks: Bool, progress: TransferProgressTracker?
  ) async throws(any Error) -> Manifest {
    guard (1_048_576...67_108_864).contains(chunkSize) else {
      throw TransferError("Invalid chunk size")
    }
    return try await FileSystem.shared.withFileHandle(
      forReadingAt: FilePath(path),
      options: .init(followSymbolicLinks: followSymbolicLinks)
    ) { file in
      let info = try await file.info()
      guard info.type == .regular, (0...1_099_511_627_776).contains(info.size) else {
        throw TransferError("Source must be a regular file of at most 1 TiB")
      }
      await progress?.begin(.hashing, totalBytes: info.size, completedBytes: 0)
      var whole = SHA256()
      var chunkHash = SHA256()
      var chunkBytes = 0
      var total: Int64 = 0
      var hashes: [String] = []
      for try await buffer in file.readChunks(chunkLength: .kibibytes(256)) {
        let data = buffer.readableBytesView
        total += Int64(data.count)
        await progress?.advance(by: data.count)
        guard total <= info.size else {
          throw TransferError("Source file changed while hashing")
        }
        whole.update(data: data)
        var offset = data.startIndex
        while offset < data.endIndex {
          let count = min(chunkSize - chunkBytes, data.endIndex - offset)
          chunkHash.update(data: data[offset..<offset + count])
          offset += count
          chunkBytes += count
          if chunkBytes == chunkSize {
            hashes.append(hex(chunkHash.finalize()))
            chunkHash = SHA256()
            chunkBytes = 0
          }
        }
      }
      if chunkBytes > 0 {
        hashes.append(hex(chunkHash.finalize()))
      }
      let after = try await file.info()
      guard total == info.size, after.size == info.size,
        after.lastDataModificationTime == info.lastDataModificationTime
      else {
        throw TransferError("Source file changed while hashing")
      }
      let manifest = Manifest(
        name: name ?? FilePath(path).lastComponent?.string ?? "", size: info.size,
        chunkSize: chunkSize, digest: hex(whole.finalize()), chunks: hashes)
      try manifest.validate()
      return manifest
    }
  }
  public init(name: String, size: Int64, chunkSize: Int, digest: String, chunks: [String]) {
    self.name = name
    self.size = size
    self.chunkSize = chunkSize
    self.digest = digest
    self.chunks = chunks
  }
}

package struct UploadStatus: Codable, Sendable {
  package let id: String
  package let completed: [Int]
  package let committed: Bool
  package var destination: String? = nil

  package func validate(manifest: Manifest, expectedID: String) throws(TransferError) -> Set<Int> {
    let completed = Set(completed)
    guard id == expectedID, completed.count == self.completed.count,
      completed.allSatisfy(manifest.chunks.indices.contains),
      !committed || completed.count == manifest.chunks.count
    else { throw TransferError("Server returned invalid upload status") }
    return completed
  }
  package init(id: String, completed: [Int], committed: Bool, destination: String?) {
    self.id = id
    self.completed = completed
    self.committed = committed
    self.destination = destination
  }
}

package func parallel<T: Sendable>(
  items: [T], jobs: Int,
  operation: @escaping @Sendable (T) async throws(any Error) -> Void
) async throws(any Error) {
  try await withThrowingTaskGroup(of: Void.self) { group in
    var iterator = items.makeIterator()
    for _ in 0..<jobs {
      if let item = iterator.next() {
        group.addTask { try await operation(item) }
      }
    }
    while try await group.next() != nil {
      if let item = iterator.next() {
        group.addTask { try await operation(item) }
      }
    }
  }
}
