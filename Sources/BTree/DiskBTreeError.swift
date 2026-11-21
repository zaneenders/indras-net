import struct SystemPackage.Errno
import struct _NIOFileSystem.FileSystemError

package enum DiskBTreeError: Error, Sendable, Equatable {
  package enum Corruption: Sendable, Equatable {
    case missingManifest
    case missingPages
    case missingPublicationLock
    case invalidManifest
    case invalidPagesFile
    case incompletePage
    case invalidPage(pageID: Int64)
    case invalidPublicationLock
  }

  package enum IOFailure: Sendable, Equatable {
    case posix(errno: Int32)
    case fileSystem(message: String)
    case incompleteWrite
    case cancelled
    case unknown

    init(_ error: any Error) {
      if case DiskBTreeError.io(let cause) = error {
        self = cause
      } else if let error = error as? FileSystemError {
        if let cause = error.cause as? FileSystemError.SystemCallError {
          self = .posix(errno: cause.errno.rawValue)
        } else {
          self = .fileSystem(message: error.description)
        }
      } else if let error = error as? Errno {
        self = .posix(errno: error.rawValue)
      } else if error is CancellationError {
        self = .cancelled
      } else {
        self = .unknown
      }
    }
  }

  case storeNotFound
  case storeAlreadyExists
  case storeInUse
  case incompatibleKeyOrdering
  case incompatibleValueSchema
  case corruptStore(cause: Corruption)
  case keyTooLarge(encodedBytes: Int, maximumBytes: Int)
  case recordTooLarge(encodedBytes: Int, maximumBytes: Int)
  case invalidScanLimit(Int)
  case storeFull
  case keyEncodingFailed
  case valueEncodingFailed
  case valueDecodingFailed(pageID: Int64)
  case io(cause: IOFailure)
  case cancelled
  case recoveryRequired(cause: IOFailure)

  static func fromIO(_ error: any Error) -> Self {
    if let failure = error as? Self { return failure }
    if error is CancellationError { return .cancelled }
    return .io(cause: IOFailure(error))
  }
}
