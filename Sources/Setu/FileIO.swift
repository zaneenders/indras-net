import Crypto
import Foundation
package import NIOCore
import NIOPosix
import _NIOFileSystem

#if canImport(Darwin)
import Darwin
#elseif canImport(Musl)
import Musl
#else
import Glibc
#endif

package func readRange(path: String, offset: Int64, count: Int, followSymbolicLinks: Bool) async throws(any Error)
  -> ByteBuffer
{
  return try await FileSystem.shared.withFileHandle(
    forReadingAt: FilePath(path),
    options: .init(followSymbolicLinks: followSymbolicLinks)
  ) { file in
    var buffer = try await file.readChunk(fromAbsoluteOffset: offset, length: .bytes(Int64(count)))
    while buffer.readableBytes < count {
      let next = try await file.readChunk(
        fromAbsoluteOffset: offset + Int64(buffer.readableBytes),
        length: .bytes(Int64(count - buffer.readableBytes)))
      guard next.readableBytes > 0 else {
        throw TransferError("Unexpected end of file")
      }
      buffer.writeImmutableBuffer(next)
    }
    return buffer
  }
}

package func writeRange(path: String, offset: Int64, data: some Sequence<UInt8> & Sendable) async throws(any Error) {
  try await data.write(
    toFileAt: FilePath(path), absoluteOffset: offset,
    options: .modifyFile(createIfNecessary: false))
}

package func allocateFile(path: String, size: Int64) async throws(any Error) {
  var options = OpenOptions.Write.newFile(replaceExisting: false, permissions: .ownerReadWrite)
  // The path is already temporary; keep even interrupted allocations discoverable by startup cleanup.
  options.newFile?.transactionalCreation = false
  try await FileSystem.shared.withFileHandle(
    forWritingAt: FilePath(path), options: options
  ) { file in
    try await file.resize(to: .bytes(size))
  }
}

package func discardFile(_ path: String) async throws(any Error) {
  _ = try await Task.detached {
    try await FileSystem.shared.removeItem(at: FilePath(path), recursively: false)
  }.value
}

public func verifyFile(path: String, manifest: Manifest, progress: TransferProgressTracker?) async throws(any Error) {
  // Chunks are verified on receipt. Publication needs only the whole-file digest,
  // avoiding a second round of per-chunk hashing while still checking bytes on disk.
  try await FileSystem.shared.withFileHandle(
    forReadingAt: FilePath(path), options: .init(followSymbolicLinks: false)
  ) { file in
    try await verifyContents(file, manifest: manifest, progress: progress)
  }
}

private func verifyContents(
  _ file: some ReadableFileHandleProtocol & Sendable, manifest: Manifest, progress: TransferProgressTracker?
)
  async throws(any Error)
{
  let info = try await file.info()
  guard info.type == .regular, info.size == manifest.size else { throw TransferError("File integrity check failed") }
  var hash = SHA256()
  var total: Int64 = 0
  for try await buffer in file.readChunks(chunkLength: .mebibytes(1)) {
    total += Int64(buffer.readableBytes)
    guard total <= manifest.size else { throw TransferError("File changed during verification") }
    hash.update(data: buffer.readableBytesView)
    await progress?.advance(by: buffer.readableBytes)
  }
  let after = try await file.info()
  guard total == manifest.size, after.size == info.size,
    after.lastDataModificationTime == info.lastDataModificationTime, hex(hash.finalize()) == manifest.digest
  else { throw TransferError("File integrity check failed") }
}

public func publish(
  partial: String, destination: String, manifest: Manifest, progress: TransferProgressTracker?,
  alreadyVerified: Bool
) async throws(any Error) {
  if !alreadyVerified {
    await progress?.begin(.verifying, totalBytes: manifest.size, completedBytes: 0)
    try await verifyFile(path: partial, manifest: manifest, progress: progress)
  }
  await progress?.begin(.publishing, totalBytes: nil, completedBytes: 0)
  try await FileSystem.shared.withFileHandle(
    forWritingAt: FilePath(partial),
    options: .modifyFile(createIfNecessary: false)
  ) { file in
    try await file.synchronize()
  }
  do {
    try await moveFile(partial, to: destination)
  } catch let error as IOError where error.errnoCode == EXDEV {
    try await copyForPublication(partial, to: destination, manifest: manifest, progress: progress)
    try await discardFile(partial)
  }
  // Acknowledge publication only after the destination directory is durable.
  try await synchronizeDirectory(containing: destination)
}

// Exercise publication as the node's user before receiving a large payload. A writable
// directory alone is insufficient: macOS may allow creating files but refuse hard links.

package func preflightDestination(_ destination: String, temporary: TemporaryFiles) async throws(any Error) {
  guard try await FileSystem.shared.info(forFileAt: FilePath(destination), infoAboutSymbolicLink: true) == nil else {
    throw TransferError("Destination already exists: \(destination)")
  }
  let parent = FilePath(destination).removingLastComponent()
  let probe = parent.appending(".setu-probe-\(UUID().uuidString)").string
  let partial = try await temporary.allocate(size: 0)
  let manifest = Manifest(name: "probe", size: 0, chunkSize: defaultChunkSize, digest: sha256(Data()), chunks: [])
  do {
    try await publish(partial: partial, destination: probe, manifest: manifest, progress: nil, alreadyVerified: false)
    try await discardFile(probe)
    try await temporary.discard(partial)
  } catch {
    try? await discardFile(probe)
    try? await temporary.discard(partial)
    throw TransferError("Cannot publish to \(destination): \(describeError(error))")
  }
}

private func copyForPublication(
  _ source: String, to destination: String, manifest: Manifest, progress: TransferProgressTracker?
) async throws(any Error) {
  // Transactional creation keeps the destination invisible until the verified copy is closed.
  let output = try await FileSystem.shared.openFile(
    forReadingAndWritingAt: FilePath(destination),
    options: .newFile(replaceExisting: false, permissions: .ownerReadWrite))
  do {
    try await FileSystem.shared.withFileHandle(
      forReadingAt: FilePath(source), options: .init(followSymbolicLinks: false)
    ) { input in
      var offset: Int64 = 0
      for try await buffer in input.readChunks(chunkLength: .mebibytes(1)) {
        let written = try await output.write(contentsOf: buffer.readableBytesView, toAbsoluteOffset: offset)
        guard written == Int64(buffer.readableBytes), offset + written <= manifest.size else {
          throw TransferError("Incomplete file copy")
        }
        offset += written
        await progress?.advance(by: buffer.readableBytes)
      }
    }
    try await verifyContents(output, manifest: manifest, progress: progress)
    try await output.synchronize()
    try Task.checkCancellation()
    try await output.close()
  } catch {
    _ = await Task.detached { try? await output.close(makeChangesVisible: false) }.value
    throw error
  }
}

package func synchronizeDirectory(containing path: String) async throws(any Error) {
  var parent = FilePath(path).removingLastComponent().string
  if parent.isEmpty {
    parent = "."
  }
  try await FileSystem.shared.withDirectoryHandle(atPath: FilePath(parent)) { directory in
    try await directory.synchronize()
  }
}

// Never use a check-then-rename sequence, which can overwrite a competing writer.
private func moveFile(_ source: String, to destination: String) async throws(any Error) {
  try await NIOThreadPool.singleton.runIfActive {
    // Both borrowed path buffers remain valid for the synchronous system call.
    let failure = unsafe FilePath(source).withPlatformString { source in
      unsafe FilePath(destination).withPlatformString { destination in
        #if canImport(Darwin)
        // Hard links into protected folders can fail even when ordinary file creation
        // succeeds. An exclusive rename is atomic and preserves the no-overwrite rule.
        return unsafe renamex_np(source, destination, UInt32(RENAME_EXCL)) == 0 ? 0 : errno
        #else
        guard unsafe link(source, destination) == 0 else {
          return errno
        }
        return unsafe unlink(source) == 0 ? 0 : errno
        #endif
      }
    }
    guard failure == 0 else {
      throw IOError(errnoCode: failure, reason: "Publishing \(destination)")
    }
  }
}
