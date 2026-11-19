import Distributed
import Foundation
package import IndraNode
package import TCPActorSystem
import _NIOFileSystem

struct SharedFilePage: Codable, Sendable {
  struct Cursor: Codable, Sendable {
    let listing: UUID
    let offset: Int
  }

  let files: [SharedFile]
  let next: Cursor?
}

package distributed actor SetuService {
  package typealias ActorSystem = TCPActorSystem
  package static let key = ServiceKey<SetuService>("indra.setu.v1")

  private let directory: SharedDirectory
  private let cluster: String
  private static let pageSize = 1024
  private var listings: [UUID: (files: [SharedFile], expiry: Task<Void, Never>)] = [:]

  private init(actorSystem: TCPActorSystem, directory: SharedDirectory, cluster: String) {
    self.actorSystem = actorSystem
    self.directory = directory
    self.cluster = cluster
  }

  package static func installer(directory path: String) -> ServiceInstaller {
    { context in
      let config = try await absolutePath(FilePath(context.configurationPath))
      let directory = try await SharedDirectory(
        path: path, journal: StagingJournal(parent: config.removingLastComponent()))
      let service = SetuService(actorSystem: context.actorSystem, directory: directory, cluster: context.cluster)
      let jobs = FileTransfers(
        directory: directory, system: context.actorSystem, receptionist: context.receptionist, cluster: context.cluster)
      try await context.receptionist.register(service, with: key)
      try await context.receptionist.registerCommand(key.name, run: jobs.handle)
      try await context.onShutdown { await jobs.stop() }
    }
  }

  // TODO: Add peer authentication and authorization later. For now, Setu assumes a trusted network.
  private func check(_ cluster: String) throws {
    guard cluster == self.cluster else { throw IndraError("Peer uses a different cluster") }
  }

  distributed func list(after cursor: SharedFilePage.Cursor?, cluster: String) async throws -> SharedFilePage {
    try check(cluster)
    let files: [SharedFile]
    let start: SharedFilePage.Cursor
    if let cursor {
      guard let listing = listings[cursor.listing], listing.files.indices.contains(cursor.offset)
      else { throw IndraError("Invalid or expired file listing cursor") }
      files = listing.files
      start = cursor
    } else {
      files = try await directory.list()
      guard files.count > Self.pageSize else { return SharedFilePage(files: files, next: nil) }
      guard listings.count < 128 else { throw IndraError("Too many open file listings") }
      let id = UUID()
      let expiry = Task { [weak self] in
        do { try await Task.sleep(for: NodeControl.commandTimeout) } catch { return }
        await self?.whenLocal { $0.listings[id] = nil }
      }
      listings[id] = (files, expiry)
      start = SharedFilePage.Cursor(listing: id, offset: 0)
    }
    let page = files[start.offset...].prefix(Self.pageSize)
    let next =
      page.endIndex < files.endIndex ? SharedFilePage.Cursor(listing: start.listing, offset: page.endIndex) : nil
    return SharedFilePage(files: Array(page), next: next)
  }

  distributed func closeListing(_ id: UUID, cluster: String) throws {
    try check(cluster)
    listings.removeValue(forKey: id)?.expiry.cancel()
  }

  distributed func receive(name: String, size: Int64, from peer: ActorEndpoint, cluster: String) async throws
    -> TransferTicket
  {
    try check(cluster)
    try FileSink.validate(size: size)
    let path = try directory.path(for: name)
    let journal = directory.journal
    return try await actorSystem.transfers.offer(to: peer, byteCount: size) {
      .sink(try await FileSink(path: path, journal: journal))
    }
  }

  distributed func fetch(name: String, for peer: ActorEndpoint, cluster: String) async throws -> TransferTicket {
    try check(cluster)
    let path = try directory.path(for: name)
    guard let info = try await FileSystem.shared.info(forFileAt: path, infoAboutSymbolicLink: true),
      info.type == .regular
    else { throw IndraError("Unknown shared file") }
    return try await actorSystem.transfers.offer(to: peer, byteCount: info.size) {
      .source(try await FileSource(path: path, followLinks: false))
    }
  }

  distributed func receipt(_ id: UUID, cluster: String) async throws -> TransferReceipt? {
    try check(cluster)
    return await actorSystem.transfers.receipt(for: id)
  }
}
