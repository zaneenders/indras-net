import Distributed
package import Foundation
import IndraNode
package import TCPActorSystem
import _NIOFileSystem

package enum FileCommand: Codable, Sendable {
  case list(peer: String?)
  case send(path: String, peer: String)
  case get(name: String, peer: String, output: String?)
  case receipt(id: UUID, peer: String?)
  case job(UUID)
  case cancel(UUID)
  case jobs
}

package struct FileJob: Codable, Sendable {
  package enum State: String, Codable, Sendable { case running, completed, failed, cancelled, commitUnconfirmed }
  package let id: UUID
  package let file: String
  package let peer: String
  package var state: State
  package var transferID: UUID?
  package var receipt: TransferReceipt?
  package var error: String?
}

package enum FileReply: Codable, Sendable {
  case files([SharedFile])
  case job(FileJob)
  case jobs([FileJob])
  case receipt(TransferReceipt?)
}

actor FileTransfers {
  let directory: SharedDirectory
  let system: TCPActorSystem
  let receptionist: Receptionist
  let cluster: String
  private var jobs: [FileJob] = []
  private var tasks: [UUID: Task<Void, Never>] = [:]
  private var stopped = false

  init(directory: SharedDirectory, system: TCPActorSystem, receptionist: Receptionist, cluster: String) {
    self.directory = directory
    self.system = system
    self.receptionist = receptionist
    self.cluster = cluster
  }

  private func service(_ peer: String) async throws -> SetuService {
    let id = UUID(uuidString: peer)
    let matches = await receptionist.services().filter {
      $0.registration.key == SetuService.key.name && $0.registration.actorType == String(reflecting: SetuService.self)
        && ($0.node.id == id || $0.node.name == peer)
    }
    guard let match = matches.first else { throw IndraError("No Setu service on \(peer)") }
    guard matches.count == 1 else { throw IndraError("Ambiguous peer. Specify the node UUID.") }
    return try SetuService.resolve(id: match.registration.address, using: system)
  }

  func handle(_ command: FileCommand) async throws -> FileReply {
    guard !stopped else { throw RPCError.shutDown }
    switch command {
    case .list(let peer):
      guard let peer else { return .files(try await directory.list()) }
      let remote = try await service(peer)
      var page = try await remote.list(after: nil, cluster: cluster)
      guard let firstCursor = page.next else { return .files(page.files) }
      defer {
        await withTaskCancellationShield { try? await remote.closeListing(firstCursor.listing, cluster: cluster) }
      }
      var files = page.files
      while let cursor = page.next {
        try Task.checkCancellation()
        page = try await remote.list(after: cursor, cluster: cluster)
        files.append(contentsOf: page.files)
      }
      return .files(files)
    case .receipt(let id, let peer):
      if let peer { return .receipt(try await service(peer).receipt(id, cluster: cluster)) }
      return .receipt(await system.transfers.receipt(for: id))
    case .send(let path, let peer):
      guard let name = FilePath(path).lastComponent?.string else { throw IndraError("Specify a source file") }
      return try submit(file: name, peer: peer) { [self] jobID in
        let remote = try await service(peer)
        guard let info = try await FileSystem.shared.info(forFileAt: FilePath(path), infoAboutSymbolicLink: false),
          info.type == .regular
        else {
          throw IndraError("Expected a regular source file")
        }
        let ticket = try await remote.receive(name: name, size: info.size, from: system.endpoint, cluster: cluster)
        guard ticket.endpoint == remote.id.endpoint, ticket.byteCount == info.size else {
          throw RPCError.invalidMessage
        }
        await record(ticket: ticket, for: jobID)
        do {
          return try await system.transfers.transfer(ticket) {
            .source(try await FileSource(path: FilePath(path), followLinks: true))
          }
        } catch let uncertain as CommitUnconfirmed {
          if let receipt = try? await remote.receipt(ticket.id, cluster: cluster), receipt == uncertain.expected {
            return receipt
          }
          throw uncertain
        }
      }
    case .get(let name, let peer, let output):
      let path = try output.map(FilePath.init(_:)) ?? directory.path(for: name)
      let journal = directory.journal
      return try submit(file: name, peer: peer) { [self] jobID in
        let remote = try await service(peer)
        let ticket = try await remote.fetch(name: name, for: system.endpoint, cluster: cluster)
        guard ticket.endpoint == remote.id.endpoint else { throw RPCError.invalidMessage }
        await record(ticket: ticket, for: jobID)
        try FileSink.validate(size: ticket.byteCount)
        return try await system.transfers.transfer(ticket) { .sink(try await FileSink(path: path, journal: journal)) }
      }
    case .jobs: return .jobs(jobs)
    case .job(let id), .cancel(let id):
      guard let job = jobs.first(where: { $0.id == id }) else { throw IndraError("Unknown file job") }
      if case .cancel = command { tasks[id]?.cancel() }
      return .job(job)
    }
  }

  private func record(ticket: TransferTicket, for jobID: UUID) {
    guard let index = jobs.firstIndex(where: { $0.id == jobID }) else { return }
    jobs[index].transferID = ticket.id
  }

  private func submit(
    file: String, peer: String, run: @escaping @Sendable (UUID) async throws -> TransferReceipt
  ) throws -> FileReply {
    guard tasks.count < 8 else { throw IndraError("Eight transfers are already running") }
    if jobs.count >= 128, let index = jobs.firstIndex(where: { $0.state != .running }) { jobs.remove(at: index) }
    let job = FileJob(id: UUID(), file: file, peer: peer, state: .running, transferID: nil, receipt: nil, error: nil)
    jobs.append(job)
    tasks[job.id] = Task {
      let result: Result<TransferReceipt, any Error>
      do {
        try Task.checkCancellation()
        result = .success(try await run(job.id))
      } catch { result = .failure(error) }
      guard let index = jobs.firstIndex(where: { $0.id == job.id }) else { return }
      switch result {
      case .success(let receipt):
        jobs[index].state = .completed
        jobs[index].receipt = receipt
      case .failure(let error as CommitUnconfirmed):
        jobs[index].state = .commitUnconfirmed
        jobs[index].error = error.description
      case .failure(let error):
        jobs[index].state = Task.isCancelled ? .cancelled : .failed
        jobs[index].error = String(describing: error)
      }
      tasks[job.id] = nil
    }
    return .job(job)
  }

  func stop() async {
    stopped = true
    let active = Array(tasks.values)
    for task in active { task.cancel() }
    for task in active { await task.value }
  }
}
