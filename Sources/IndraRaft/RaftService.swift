import Distributed
import Foundation
import IndraNode
import NIOCore
import RaftCore
import TCPActorSystem
import _NIOFileSystem

struct RaftForward: Codable, Sendable {
  static let commandName = "indra.raft.forward.v1"
  let peer: String?
  let group: String?
  let request: RaftRequest
}

distributed actor RaftService {
  typealias ActorSystem = TCPActorSystem
  static let key = ServiceKey<RaftService>(RaftStatus.serviceName)

  private let host: RaftHost
  private let transport: RaftTransport

  private init(actorSystem: TCPActorSystem, host: RaftHost, transport: RaftTransport) {
    self.actorSystem = actorSystem
    self.host = host
    self.transport = transport
  }

  static func installer(directory: String?, mode: RaftStartMode) -> ServiceInstaller {
    { context in
      let transport = RaftTransport(
        receptionist: context.receptionist, system: context.actorSystem, cluster: context.cluster)
      try await context.receptionist.registerQuery(for: key, timeout: .milliseconds(800)) { service, cluster in
        try await service.status(cluster: cluster)
      }
      try await context.receptionist.registerCommand(RaftForward.commandName, run: transport.forward)
      try await context.receptionist.registerCommand(RaftInspection.commandName, run: transport.inspect)
      let node = context.nodeID.uuidString
      guard let directory else {
        try await context.receptionist.registerCommand(key.name) { (request: RaftRequest) -> RaftReply in
          if case .status = request { return .client(node) }
          return try await transport.forward(RaftForward(peer: nil, group: nil, request: request))
        }
        return
      }
      let path = FilePath(directory)
      if case .join(let group) = mode, group == node {
        throw IndraError("Specify another node as the Raft bootstrap node")
      }
      do {
        try await activate(directory: path, mode: mode, context: context, transport: transport)
      } catch RaftServiceError.discoveringGroup {
        let task = Task {
          do {
            let group = try await transport.discoverGroup()
            try Task.checkCancellation()
            try await activate(directory: path, mode: .join(group), context: context, transport: transport)
          } catch is CancellationError {
          } catch {
            print("Raft join failed. \(error)")
          }
        }
        do {
          try await context.onShutdown {
            task.cancel()
            await task.value
          }
        } catch {
          task.cancel()
          await task.value
          throw error
        }
      }
    }
  }

  private static func activate(
    directory: FilePath, mode: RaftStartMode, context: ServiceContext, transport: RaftTransport
  ) async throws {
    let node = context.nodeID.uuidString
    let host = try await RaftHost.open(in: directory, node: node, cluster: context.cluster, mode: mode)
    let service = RaftService(actorSystem: context.actorSystem, host: host, transport: transport)
    do {
      try await context.onShutdown { await host.stop() }
      try await context.receptionist.register(service, with: key)
      try await context.receptionist.registerCommand(key.name) { (request: RaftRequest) -> RaftReply in
        if case .status = request { return try await host.execute(request) }
        return try await transport.forward(RaftForward(peer: node, group: nil, request: request))
      }
      await host.start(transport: transport)
    } catch {
      await host.stop()
      throw error
    }
  }

  distributed func execute(_ payload: ByteBuffer, cluster: String) async throws -> RaftReply {
    try validateCluster(cluster)
    let request = try JSONDecoder().decode(RaftRequest.self, from: Data(payload.readableBytesView))
    do {
      return try await host.execute(request)
    } catch RaftServiceError.stopped {
      return .notLeader(nil)
    }
  }

  distributed func status(cluster: String) async throws -> RaftStatus {
    try validateCluster(cluster)
    guard case .status(let status) = try await host.execute(.status) else { throw RaftServiceError.invalidState }
    return status
  }

  distributed func inspect(
    _ query: RaftInspection.Query, group: String, cluster: String
  ) async throws -> RaftInspectionReply {
    try validateGroup(group, cluster: cluster)
    switch query {
    case .log(let before):
      return .log(try await host.inspectLog(before: before))
    case .snapshot(let offset, let index):
      guard offset >= 0 else { throw IndraError("Specify a nonnegative snapshot offset") }
      let snapshot = try await host.inspectionSnapshot()
      if let index, snapshot?.lastIncludedIndex != index { throw RaftServiceError.snapshotChanged }
      guard let snapshot else { return .snapshot(nil) }
      return .snapshot(try await RaftSnapshotPage.read(snapshot, offset: offset))
    }
  }

  distributed func vote(_ request: RequestVote.Args, group: String, cluster: String) async throws -> RequestVote.Reply {
    try validateGroup(group, cluster: cluster)
    return try await host.vote(request)
  }

  distributed func append(
    _ request: AppendEntries.Args, group: String, cluster: String
  ) async throws -> AppendEntries.Reply {
    try validateGroup(group, cluster: cluster)
    return try await host.append(request)
  }

  distributed func install(
    _ request: InstallSnapshot.Args, group: String, cluster: String
  ) async throws -> InstallSnapshot.Reply {
    try validateGroup(group, cluster: cluster)
    return try await host.install(request)
  }

  distributed func admit(peer: PeerId, group: String, cluster: String) async throws -> Bool {
    try validateGroup(group, cluster: cluster)
    guard let id = UUID(uuidString: peer), id.uuidString == peer else {
      throw IndraError("Specify a Raft node UUID")
    }
    let candidate = try await transport.status(of: peer)
    guard candidate.node == peer, candidate.group == group else {
      throw IndraError("Raft joiner advertised a different group")
    }
    return try await host.admit(peer)
  }

  private func validateCluster(_ cluster: String) throws {
    guard cluster == transport.cluster else { throw IndraError("Peer uses a different cluster") }
  }

  private func validateGroup(_ group: String, cluster: String) throws {
    try validateCluster(cluster)
    guard group == host.group else { throw IndraError("Peer uses a different Raft group") }
  }
}
