import Crypto
import NIOCore
import _NIOFileSystem

struct DiskTreeSchema: Equatable, Sendable {
  fileprivate let partitionOrderingDigest: ByteBuffer
  fileprivate let sortKeyOrderingDigest: ByteBuffer
  fileprivate let valueSchemaDigest: ByteBuffer

  func validateCompatibility(with expected: Self) throws(DiskBTreeError) {
    guard partitionOrderingDigest == expected.partitionOrderingDigest,
      sortKeyOrderingDigest == expected.sortKeyOrderingDigest
    else { throw .incompatibleKeyOrdering }
    guard valueSchemaDigest == expected.valueSchemaDigest else { throw .incompatibleValueSchema }
  }
}

extension DiskTreeSchema {
  init(partitionOrderingID: String, sortKeyOrderingID: String, valueSchemaID: String) {
    self.init(
      partitionOrderingDigest: Self.digest(of: partitionOrderingID),
      sortKeyOrderingDigest: Self.digest(of: sortKeyOrderingID),
      valueSchemaDigest: Self.digest(of: valueSchemaID))
  }

  private static func digest(of id: String) -> ByteBuffer {
    let bytes = ByteBuffer(string: id)
    var hash = SHA256()
    hash.update(bytes: bytes.readableBytesSpan)
    return ByteBuffer(bytes: hash.finalize())
  }
}

struct DiskTreeManifest: Sendable {
  static let name = "manifest"
  static let pendingName = "manifest.next"
  static let recoveryName = "manifest.recovery"

  private static let magic: UInt32 = 0x4254_4d30  // BTM0
  private static let checksumSize = 8
  private static let size = 20 + 3 * SHA256.byteCount + checksumSize

  let rootPageID: Int64?
  let pageCount: Int64
  let schema: DiskTreeSchema

  static func read(in directory: DirectoryFileHandle) async throws(DiskBTreeError) -> Self? {
    do {
      return try await directory.withFileHandle(
        forReadingAt: FilePath(name), options: .init(followSymbolicLinks: false, closeOnExec: true)
      ) { file in
        let info = try await file.info()
        guard info.type == .regular, info.size == Int64(size) else {
          throw DiskBTreeError.corruptStore(cause: .invalidManifest)
        }
        var bytes = try await file.readChunk(fromAbsoluteOffset: 0, length: .bytes(Int64(size)))
        guard let fields = bytes.getSlice(at: 0, length: size - checksumSize),
          bytes.readInteger(endianness: .big, as: UInt32.self) == magic,
          let root = bytes.readInteger(endianness: .big, as: Int64.self),
          let pageCount = bytes.readInteger(endianness: .big, as: Int64.self),
          let partitionOrderingDigest = bytes.readSlice(length: SHA256.byteCount),
          let sortKeyOrderingDigest = bytes.readSlice(length: SHA256.byteCount),
          let valueSchemaDigest = bytes.readSlice(length: SHA256.byteCount),
          bytes.readInteger(endianness: .big, as: UInt64.self) == checksum(of: fields),
          pageCount <= DiskPageFormat.maxPageCount,
          (root == -1 && pageCount == 0) || (root >= 0 && root < pageCount)
        else { throw DiskBTreeError.corruptStore(cause: .invalidManifest) }
        return Self(
          rootPageID: root == -1 ? nil : root, pageCount: pageCount,
          schema: DiskTreeSchema(
            partitionOrderingDigest: partitionOrderingDigest,
            sortKeyOrderingDigest: sortKeyOrderingDigest,
            valueSchemaDigest: valueSchemaDigest))
      }
    } catch let error as FileSystemError where error.code == .notFound {
      return nil
    } catch {
      throw .fromIO(error)
    }
  }

  func write(to file: WriteFileHandle) async throws {
    var bytes = ByteBuffer()
    bytes.writeInteger(Self.magic, endianness: .big, as: UInt32.self)
    bytes.writeInteger(rootPageID ?? -1, endianness: .big, as: Int64.self)
    bytes.writeInteger(pageCount, endianness: .big, as: Int64.self)
    bytes.writeImmutableBuffer(schema.partitionOrderingDigest)
    bytes.writeImmutableBuffer(schema.sortKeyOrderingDigest)
    bytes.writeImmutableBuffer(schema.valueSchemaDigest)
    let checksum = Self.checksum(of: bytes)
    bytes.writeInteger(checksum, endianness: .big, as: UInt64.self)
    guard try await file.write(contentsOf: bytes.readableBytesView, toAbsoluteOffset: 0) == Int64(Self.size) else {
      throw DiskBTreeError.io(cause: .incompleteWrite)
    }
  }

  private static func checksum(of bytes: ByteBuffer) -> UInt64 {
    bytes.readableBytesView.reduce(0xcbf2_9ce4_8422_2325) { ($0 ^ UInt64($1)) &* 0x0100_0000_01b3 }
  }
}
