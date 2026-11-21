import Logging
import NIOCore
import _NIOFileSystem

#if canImport(Darwin)
import Darwin
#elseif canImport(Musl)
import Musl
#else
import Glibc
#endif

struct DiskPager: Sendable {
  struct View: Sendable, ~Escapable {
    private let pages: any ReadableFileHandleProtocol & Sendable
    let rootPageID: Int64?
    let pageCount: Int64

    @_lifetime(borrow pages)
    fileprivate init<Pages: ReadableFileHandleProtocol & Sendable>(
      pages: borrowing Pages, rootPageID: Int64?, pageCount: Int64
    ) {
      self.pages = copy pages
      self.rootPageID = rootPageID
      self.pageCount = pageCount
    }

    func read(_ id: Int64) async throws(DiskBTreeError) -> ByteBuffer {
      guard id >= 0, id < pageCount else { throw .corruptStore(cause: .invalidPage(pageID: id)) }
      let bytes = try await DiskPager.performingIO {
        try await pages.readChunk(
          fromAbsoluteOffset: DiskPageFormat.offset(for: id), length: .bytes(Int64(DiskPageFormat.pageSize)))
      }
      guard bytes.readableBytes == DiskPageFormat.pageSize else { throw .corruptStore(cause: .incompletePage) }
      return bytes
    }
  }

  struct Batch: ~Copyable {
    fileprivate var nextPageID: Int64
    fileprivate var reusedPageCount: Int
    fileprivate var writes: [(id: Int64, bytes: ByteBuffer)]
    fileprivate var retiredPageIDs: [Int64]

    mutating func retire(_ id: Int64) {
      retiredPageIDs.append(id)
    }
  }

  private static let readOptions = OpenOptions.Read(followSymbolicLinks: false, closeOnExec: true)
  private static let directoryOptions = OpenOptions.Directory(followSymbolicLinks: false, closeOnExec: true)
  private static let createOptions = OpenOptions.Write(
    existingFile: .open, newFile: .init(permissions: .ownerReadWrite, transactionalCreation: false),
    followSymbolicLinks: false, closeOnExec: true)
  private static let writeOptions = OpenOptions.Write(
    existingFile: .open, newFile: nil, followSymbolicLinks: false, closeOnExec: true)
  private static let pagesName = FilePath("pages")
  static let publicationLockName = FilePath("publication.lock")
  private static let logger = Logger(label: "BTree")
  private let directory: DirectoryFileHandle
  private let pages: ReadWriteFileHandle
  private let publicationLock: ReadWriteFileHandle
  private let published: DiskTreeManifest
  private var rootPageID: Int64?
  private var pageCount: Int64
  private var freePageIDs: [Int64]
  private(set) var pageWriteCount: Int
  var beforeCommitStep: (@Sendable (DiskCommitStep) async throws -> Void)?

  var view: View {
    @_lifetime(borrow self)
    borrowing get { View(pages: pages, rootPageID: rootPageID, pageCount: pageCount) }
  }

  static func create(at path: FilePath, schema: DiskTreeSchema) async throws(DiskBTreeError) {
    try await performingIO {
      try await withStoreDirectory(at: path) { directory in
        let pages: ReadWriteFileHandle
        do {
          pages = try await directory.openFile(forReadingAndWritingAt: pagesName, options: writeOptions)
        } catch let error as FileSystemError where error.code == .notFound {
          guard try await DiskTreeManifest.read(in: directory) == nil else { throw DiskBTreeError.storeAlreadyExists }
          pages = try await directory.openFile(forReadingAndWritingAt: pagesName, options: createOptions)
        }
        return try await finishFileScope(
          execute: {
            let info = try await pages.info()
            guard info.type == .regular else { throw DiskBTreeError.corruptStore(cause: .invalidPagesFile) }
            try await lockWriter(on: pages)
            guard try await DiskTreeManifest.read(in: directory) == nil else { throw DiskBTreeError.storeAlreadyExists }
            // An interrupted creation can leave an empty pages file behind.
            guard try await pages.info().size == 0 else {
              throw DiskBTreeError.corruptStore(cause: .missingManifest)
            }
            let publicationLock = try await directory.openFile(
              forReadingAndWritingAt: publicationLockName, options: createOptions)
            return try await finishFileScope(
              execute: {
                try await validatePublicationLock(publicationLock)
                let manifest = DiskTreeManifest(rootPageID: nil, pageCount: 0, schema: schema)
                let pager = Self(
                  directory: directory, pages: pages, publicationLock: publicationLock, published: manifest,
                  rootPageID: nil, pageCount: 0, freePageIDs: [], pageWriteCount: 0, beforeCommitStep: nil)
                try await pages.synchronize()
                try await publicationLock.synchronize()
                try await pager.publish(manifest, restoring: nil)
                return ((), true)
              },
              close: { try await publicationLock.close() })
          },
          close: { try await pages.close() })
      }
    }
  }

  static func withWriter<Result: Sendable>(
    at path: FilePath, schema: DiskTreeSchema,
    execute: @Sendable (Self) async throws -> (value: Result, published: Bool)
  ) async throws -> Result {
    try await withStoreDirectory(at: path) { directory in
      let pages = try await openStoreFile(in: directory, schema: schema, missing: .missingPages) {
        try await directory.openFile(forReadingAndWritingAt: pagesName, options: writeOptions)
      }
      return try await finishFileScope(
        execute: {
          let info = try await performingIO { try await pages.info() }
          guard info.type == .regular else { throw DiskBTreeError.corruptStore(cause: .invalidPagesFile) }
          try await lockWriter(on: pages)
          let publicationLock = try await openStoreFile(
            in: directory, schema: schema, missing: .missingPublicationLock
          ) {
            try await directory.openFile(forReadingAndWritingAt: publicationLockName, options: writeOptions)
          }
          return try await finishFileScope(
            execute: {
              try await validatePublicationLock(publicationLock)
              let (manifest, _) = try await withExclusivePublicationLock(
                on: publicationLock
              ) { () throws(DiskBTreeError) in
                let manifest = try await loadManifest(in: directory, pages: pages, schema: schema)
                return (manifest, false)
              }
              let pager = Self(
                directory: directory, pages: pages, publicationLock: publicationLock, published: manifest,
                rootPageID: manifest.rootPageID, pageCount: manifest.pageCount, freePageIDs: [], pageWriteCount: 0,
                beforeCommitStep: nil)
              return try await execute(pager)
            },
            close: { try await publicationLock.close() })
        },
        close: { try await pages.close() })
    }
  }

  static func withReader<Result: Sendable>(
    at path: FilePath, schema: DiskTreeSchema,
    execute: @Sendable (borrowing View) async throws -> Result
  ) async throws -> Result {
    try await withStoreDirectory(at: path) { directory in
      guard !Task.isCancelled else { throw DiskBTreeError.cancelled }
      let pages = try await openStoreFile(in: directory, schema: schema, missing: .missingPages) {
        try await directory.openFile(forReadingAt: pagesName, options: readOptions)
      }
      return try await finishFileScope(
        execute: {
          let publicationLock = try await openStoreFile(
            in: directory, schema: schema, missing: .missingPublicationLock
          ) {
            try await directory.openFile(forReadingAt: publicationLockName, options: readOptions)
          }
          // Closing this handle releases the shared lock before the snapshot uses any pages.
          let (manifest, _) = try await finishFileScope(
            execute: {
              try await validatePublicationLock(publicationLock)
              try await waitForPublicationLock(on: publicationLock, mode: LOCK_SH)
              let manifest = try await loadManifest(in: directory, pages: pages, schema: schema)
              return (manifest, false)
            },
            close: { try await publicationLock.close() })
          let view = View(pages: pages, rootPageID: manifest.rootPageID, pageCount: manifest.pageCount)
          return (try await execute(view), false)
        },
        close: { try await pages.close() })
    }
  }

  private static func withStoreDirectory<Result: Sendable>(
    at path: FilePath,
    execute: (DirectoryFileHandle) async throws -> (value: Result, published: Bool)
  ) async throws -> Result {
    let directory: DirectoryFileHandle
    do {
      directory = try await FileSystem.shared.openDirectory(atPath: path, options: directoryOptions)
    } catch let error as FileSystemError where error.code == .notFound {
      throw DiskBTreeError.storeNotFound
    } catch {
      throw DiskBTreeError.fromIO(error)
    }
    let result = try await finishFileScope(
      execute: { try await execute(directory) },
      close: { try await directory.close() })
    return result.value
  }

  private static func openStoreFile<File: FileHandleProtocol & Sendable>(
    in directory: DirectoryFileHandle, schema: DiskTreeSchema, missing: DiskBTreeError.Corruption,
    open: () async throws -> File
  ) async throws(DiskBTreeError) -> File {
    do {
      return try await open()
    } catch let error as FileSystemError where error.code == .notFound {
      _ = try await committedManifest(in: directory, schema: schema)
      // Creation may have completed after the first open.
      do {
        return try await open()
      } catch let error as FileSystemError where error.code == .notFound {
        throw .corruptStore(cause: missing)
      } catch {
        throw .fromIO(error)
      }
    } catch {
      throw .fromIO(error)
    }
  }

  static func finishFileScope<Result: Sendable>(
    execute: () async throws -> (value: Result, published: Bool),
    close: () async throws -> Void
  ) async throws -> (value: Result, published: Bool) {
    let result: (value: Result, published: Bool)
    do {
      result = try await execute()
    } catch {
      do {
        try await withTaskCancellationShield(operation: close)
      } catch let cleanupError {
        logCleanupError(cleanupError)
      }
      throw error
    }
    do {
      try await withTaskCancellationShield(operation: close)
    } catch {
      guard result.published else { throw DiskBTreeError.fromIO(error) }
      logCleanupError(error)
    }
    return result
  }

  static func performingIO<Result>(_ execute: () async throws -> Result) async throws(DiskBTreeError) -> Result {
    do {
      return try await execute()
    } catch {
      throw .fromIO(error)
    }
  }

  private static func logCleanupError(_ error: any Error) {
    let cause =
      if let error = error as? FileSystemError { error.description } else if let error = error as? Errno {
        error.description
      } else { "\(error._domain) (\(error._code))" }
    logger.warning("BTree file cleanup failed", metadata: ["cause": .string(cause)])
  }

  private static func loadManifest(
    in directory: DirectoryFileHandle, pages: some FileHandleProtocol & Sendable, schema: DiskTreeSchema
  ) async throws(DiskBTreeError) -> DiskTreeManifest {
    let recovering = try await performingIO {
      try await directory.withUnsafeDescriptor { descriptor in
        var info = stat()
        if unsafe fstatat(descriptor.rawValue, DiskTreeManifest.recoveryName, &info, AT_SYMLINK_NOFOLLOW) == 0 {
          return true
        }
        let error = Errno(rawValue: errno)
        guard error == .noSuchFileOrDirectory else { throw error }
        return false
      }
    }
    if recovering {
      do {
        // A stopped writer may have left either manifest in place. Both point at synced data.
        try await synchronizeDirectory(in: directory, using: pages)
      } catch {
        throw .recoveryRequired(cause: .init(error))
      }
    }
    let manifest = try await committedManifest(in: directory, schema: schema)
    let info = try await performingIO { try await pages.info() }
    guard info.type == .regular else { throw .corruptStore(cause: .invalidPagesFile) }
    try DiskPageFormat.validateFileSize(info.size, pageCount: manifest.pageCount)
    if recovering {
      // The publication lock excludes new markers. Unlink also tolerates another reader clearing it first.
      await clearRecoveryMarker(in: directory)
    }
    return manifest
  }

  private static func committedManifest(
    in directory: DirectoryFileHandle, schema: DiskTreeSchema
  ) async throws(DiskBTreeError) -> DiskTreeManifest {
    var stored = try await DiskTreeManifest.read(in: directory)
    if stored == nil, try await hasPageData(in: directory) {
      // The first writer may have published the empty manifest and started appending.
      stored = try await DiskTreeManifest.read(in: directory)
      guard stored != nil else { throw .corruptStore(cause: .missingManifest) }
    }
    guard let manifest = stored else { throw .storeNotFound }
    try manifest.schema.validateCompatibility(with: schema)
    return manifest
  }

  private static func hasPageData(in directory: DirectoryFileHandle) async throws(DiskBTreeError) -> Bool {
    do {
      return try await directory.withFileHandle(forReadingAt: pagesName, options: readOptions) { pages in
        let info = try await pages.info()
        guard info.type == .regular else { throw DiskBTreeError.corruptStore(cause: .invalidPagesFile) }
        return info.size > 0
      }
    } catch let error as FileSystemError where error.code == .notFound {
      return false
    } catch {
      throw .fromIO(error)
    }
  }

  private static func lockWriter(on pages: ReadWriteFileHandle) async throws(DiskBTreeError) {
    try await performingIO {
      try await pages.withUnsafeDescriptor { fd in
        while flock(fd.rawValue, LOCK_EX | LOCK_NB) != 0 {
          let error = Errno(rawValue: errno)
          if error == .interrupted { continue }
          if error == .wouldBlock { throw DiskBTreeError.storeInUse }
          throw error
        }
      }
    }
  }

  private static func waitForPublicationLock(
    on file: some FileHandleProtocol & Sendable, mode: Int32
  ) async throws(DiskBTreeError) {
    try await performingIO {
      while true {
        try Task.checkCancellation()
        let acquired = try await file.withUnsafeDescriptor { fd in
          while flock(fd.rawValue, mode | LOCK_NB) != 0 {
            let error = Errno(rawValue: errno)
            if error == .interrupted { continue }
            if error == .wouldBlock { return false }
            throw error
          }
          return true
        }
        if acquired { return }
        try await Task.sleep(for: .milliseconds(1))
      }
    }
  }

  private static func validatePublicationLock(
    _ file: some FileHandleProtocol & Sendable
  ) async throws(DiskBTreeError) {
    let info = try await performingIO { try await file.info() }
    guard info.type == .regular else {
      throw .corruptStore(cause: .invalidPublicationLock)
    }
  }

  private static func withExclusivePublicationLock<Result: Sendable>(
    on file: ReadWriteFileHandle,
    execute: () async throws(DiskBTreeError) -> (value: Result, published: Bool)
  ) async throws(DiskBTreeError) -> (value: Result, published: Bool) {
    try await waitForPublicationLock(on: file, mode: LOCK_EX)
    return try await performingIO {
      try await finishFileScope(
        execute: execute,
        close: {
          try await file.withUnsafeDescriptor { fd in
            while flock(fd.rawValue, LOCK_UN) != 0 {
              let error = Errno(rawValue: errno)
              if error != .interrupted { throw error }
            }
          }
        })
    }
  }

  func beginBatch() throws(DiskBTreeError) -> Batch {
    guard !Task.isCancelled else { throw .cancelled }
    return Batch(nextPageID: pageCount, reusedPageCount: 0, writes: [], retiredPageIDs: rootPageID.map { [$0] } ?? [])
  }

  func stage(_ bytes: ByteBuffer, in batch: inout Batch) throws(DiskBTreeError) -> Int64 {
    let id: Int64
    if batch.reusedPageCount < freePageIDs.count {
      // Reserve free pages by index so a failed batch leaves the free list intact.
      id = freePageIDs[freePageIDs.count - batch.reusedPageCount - 1]
      batch.reusedPageCount += 1
    } else {
      id = batch.nextPageID
      guard DiskPageFormat.validPageIDs.contains(id) else { throw .storeFull }
      batch.nextPageID += 1
    }
    batch.writes.append((id: id, bytes: bytes))
    return id
  }

  mutating func apply(_ batch: consuming Batch, root: Int64) async throws(DiskBTreeError) {
    for (index, write) in batch.writes.enumerated() {
      let offset = DiskPageFormat.offset(for: write.id)
      let count = try await Self.performingIO {
        try await beforeCommitStep?(.writePage(index: index))
        return try await pages.write(contentsOf: write.bytes.readableBytesView, toAbsoluteOffset: offset)
      }
      guard count == Int64(DiskPageFormat.pageSize) else {
        throw .io(cause: .incompleteWrite)
      }
    }
    rootPageID = root
    pageCount = batch.nextPageID
    pageWriteCount += batch.writes.count
    freePageIDs.removeLast(batch.reusedPageCount)
    // Published pages can still be visible to readers holding older snapshots.
    let firstPrivatePageID = published.pageCount
    freePageIDs.append(contentsOf: batch.retiredPageIDs.lazy.filter { $0 >= firstPrivatePageID })
  }

  func finish() async throws(DiskBTreeError) -> Bool {
    guard !Task.isCancelled else { throw .cancelled }
    guard pageCount != published.pageCount else { return false }
    try await Self.performingIO {
      try await beforeCommitStep?(.syncPages)
      try await pages.synchronize()
    }
    try await publish(
      DiskTreeManifest(rootPageID: rootPageID, pageCount: pageCount, schema: published.schema),
      restoring: published)
    return true
  }

  private static func synchronizeDirectory(
    in directory: DirectoryFileHandle, using pages: some FileHandleProtocol & Sendable
  ) async throws {
    try await directory.synchronize()
    #if canImport(Darwin)
    // Flush the drive cache after writing file data and directory metadata on this volume.
    try await pages.withUnsafeDescriptor { fd in
      while fcntl(fd.rawValue, F_FULLFSYNC) == -1 {
        let error = Errno(rawValue: errno)
        if error != .interrupted { throw error }
      }
    }
    #endif
  }

  private func stageManifest(
    _ manifest: DiskTreeManifest, beforeStep: (@Sendable (DiskCommitStep) async throws -> Void)?
  ) async throws {
    try await directory.withFileHandle(
      forWritingAt: FilePath(DiskTreeManifest.pendingName),
      options: .init(
        existingFile: .truncate, newFile: .init(permissions: .ownerReadWrite, transactionalCreation: false),
        followSymbolicLinks: false, closeOnExec: true)
    ) { file in
      try await beforeStep?(.writeManifest)
      try await manifest.write(to: file)
      try await beforeStep?(.syncManifest)
      try await file.synchronize()
    }
  }

  private func installStagedManifest() async throws {
    try await directory.withUnsafeDescriptor { descriptor in
      let fd = descriptor.rawValue
      guard unsafe renameat(fd, DiskTreeManifest.pendingName, fd, DiskTreeManifest.name) == 0 else {
        throw Errno(rawValue: errno)
      }
    }
  }

  private static func unlink(_ name: String, in directory: DirectoryFileHandle) async throws {
    try await directory.withUnsafeDescriptor { fd in
      guard unsafe unlinkat(fd.rawValue, name, 0) != 0 else { return }
      let error = Errno(rawValue: errno)
      if error != .noSuchFileOrDirectory { throw error }
    }
  }

  private static func clearRecoveryMarker(in directory: DirectoryFileHandle) async {
    do {
      try await withTaskCancellationShield {
        try await unlink(DiskTreeManifest.recoveryName, in: directory)
      }
    } catch {
      logCleanupError(error)
    }
  }

  private func publish(_ manifest: DiskTreeManifest, restoring previous: DiskTreeManifest?) async throws(DiskBTreeError)
  {
    try await Self.performingIO {
      try await stageManifest(manifest, beforeStep: beforeCommitStep)
    }
    _ = try await Self.withExclusivePublicationLock(on: publicationLock) { () throws(DiskBTreeError) -> (Void, Bool) in
      do throws(DiskBTreeError) {
        try await Self.performingIO {
          // Leave a durable marker before changing the active manifest.
          try await directory.withFileHandle(
            forWritingAt: FilePath(DiskTreeManifest.recoveryName), options: Self.createOptions
          ) { file in
            try await file.synchronize()
          }
          try await beforeCommitStep?(.syncDirectoryBeforeRename)
          try await Self.synchronizeDirectory(in: directory, using: pages)
          try await beforeCommitStep?(.renameManifest)
        }
        guard !Task.isCancelled else { throw .cancelled }
        try await withTaskCancellationShield { () async throws(DiskBTreeError) in
          try await replaceManifest(restoring: previous)
        }
      } catch .recoveryRequired(let cause) {
        throw .recoveryRequired(cause: cause)
      } catch {
        await Self.clearRecoveryMarker(in: directory)
        throw error
      }
      // A crash can restore the marker. Recovery will confirm the active manifest again.
      await Self.clearRecoveryMarker(in: directory)
      return ((), true)
    }
  }

  private func replaceManifest(restoring previous: DiskTreeManifest?) async throws(DiskBTreeError) {
    try await Self.performingIO { try await installStagedManifest() }
    do {
      try await beforeCommitStep?(.syncDirectoryAfterRename)
      try await Self.synchronizeDirectory(in: directory, using: pages)
    } catch let syncError {
      do {
        if let previous {
          try await stageManifest(previous, beforeStep: nil)
          try await installStagedManifest()
        } else {
          try await Self.unlink(DiskTreeManifest.name, in: directory)
        }
        try await beforeCommitStep?(.syncDirectoryAfterRollback)
        try await Self.synchronizeDirectory(in: directory, using: pages)
      } catch {
        throw .recoveryRequired(cause: .init(error))
      }
      throw .fromIO(syncError)
    }
  }
}
