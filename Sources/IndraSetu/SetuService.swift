import Distributed
import Foundation
package import IndraNode
package import TCPActorSystem
import _NIOFileSystem

distributed actor SharedFileListing {
  typealias ActorSystem = TCPActorSystem

  private var files: [SharedFile] = []

  distributed func append(_ page: [SharedFile]) {
    files.append(contentsOf: page)
  }

  func sortedFiles() -> [SharedFile] {
    files.sort { $0.name.utf8.lexicographicallyPrecedes($1.name.utf8) }
    return files
  }
}

package distributed actor SetuService {
  package typealias ActorSystem = TCPActorSystem
  package static let key = ServiceKey<SetuService>("indra.setu.v1")

  private let directory: SharedDirectory
  private let cluster: String

  private init(actorSystem: TCPActorSystem, directory: SharedDirectory, cluster: String) {
    self.actorSystem = actorSystem
    self.directory = directory
    self.cluster = cluster
  }

  package static func installer(directory path: String) -> ServiceInstaller {
    { context in
      let directory = try await SharedDirectory(path: path)
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

  distributed func list(into listing: SharedFileListing, cluster: String) async throws {
    try check(cluster)
    try await directory.list { try await listing.append($0) }
  }

  distributed func receive(name: String, size: Int64, from peer: ActorEndpoint, cluster: String) async throws
    -> TransferTicket
  {
    try check(cluster)
    try FileSink.validate(size: size)
    let path = try directory.path(for: name)
    return try await actorSystem.transfers.offer(to: peer, byteCount: size) {
      .sink(try await FileSink(path: path))
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
