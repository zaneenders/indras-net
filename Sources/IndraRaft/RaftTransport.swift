import Foundation
import IndraNode
import NIOCore
import RaftCore
import TCPActorSystem

struct RaftTransport: Sendable {
  static let snapshotChunkBytes = 16 * 1_024
  private weak var receptionist: Receptionist?
  private let system: TCPActorSystem
  let cluster: String

  private struct Target: Sendable {
    let node: NodeIdentity
    let service: RaftService
  }

  private struct Report: Sendable {
    let target: Target
    let status: RaftStatus
  }

  init(receptionist: Receptionist, system: TCPActorSystem, cluster: String) {
    self.receptionist = receptionist
    self.system = system
    self.cluster = cluster
  }

  func status(of peer: String) async throws -> RaftStatus {
    let target = try await target(peer)
    return try await Self.timed(.milliseconds(600)) { try await target.service.status(cluster: cluster) }
  }

  func inspect(_ request: RaftInspection) async throws -> RaftInspectionReply {
    let target = try await target(request.peer)
    return try await Self.timed(.seconds(5)) {
      try await target.service.inspect(request.query, group: request.group, cluster: cluster)
    }
  }

  func forward(_ command: RaftForward) async throws -> RaftReply {
    if case .status = command.request, let peer = command.peer { return .status(try await status(of: peer)) }
    let payload = ByteBuffer(bytes: try JSONEncoder().encode(command.request))
    let deadline = ContinuousClock.now.advanced(by: .seconds(6))
    var group = command.group
    var hint: PeerId?
    if let peer = command.peer {
      let preferred = try await target(peer)
      hint = preferred.node.id.uuidString
      let preferredGroup = try await status(of: peer).group
      if let group, group != preferredGroup { throw IndraError("Peer uses a different Raft group") }
      group = preferredGroup
    }

    repeat {
      let available = await reports()
      if group == nil {
        let groups = Set(available.map(\.status.group))
        guard groups.count <= 1 else { throw IndraError("Multiple Raft groups. Specify --peer.") }
        group = groups.first
      }
      let candidates = available.filter { $0.status.group == group }.sorted { left, right in
        if (left.status.role == "leader") != (right.status.role == "leader") {
          return left.status.role == "leader"
        }
        if left.status.term != right.status.term { return left.status.term > right.status.term }
        return left.status.node == hint && right.status.node != hint
      }
      if let candidate = candidates.first {
        do {
          let reply = try await Self.timed(.seconds(3)) {
            try await candidate.target.service.execute(payload, cluster: cluster)
          }
          if case .notLeader(let leader) = reply { hint = leader } else { return reply }
        } catch is RPCError {
          // A stale actor address or an unresponsive leader is retried via discovery.
        }
      }
      try await Task.sleep(for: .milliseconds(75))
    } while ContinuousClock.now < deadline
    throw RaftServiceError.noLeader
  }

  func join(peer: PeerId, group: String) async {
    let candidates = await reports().filter { $0.status.group == group && $0.status.role == "leader" }
    guard let leader = candidates.max(by: { $0.status.term < $1.status.term }) else { return }
    _ = try? await Self.timed(.seconds(2)) {
      try await leader.target.service.admit(peer: peer, group: group, cluster: cluster)
    }
  }

  func discoverGroup() async throws -> PeerId {
    while !Task.isCancelled {
      let groups = Set(await reports().map(\.status.group))
      if groups.count > 1 { throw IndraError("Multiple Raft groups discovered. Specify a UUID with --raft-join.") }
      if let group = groups.first { return group }
      try await Task.sleep(for: .milliseconds(400))
    }
    throw CancellationError()
  }

  func vote(peer: PeerId, group: String, request: RequestVote.Args) async throws -> RequestVote.Reply {
    let target = try await target(peer)
    return try await Self.timed(.milliseconds(650)) {
      try await target.service.vote(request, group: group, cluster: cluster)
    }
  }

  func append(peer: PeerId, group: String, request: AppendEntries.Args) async throws -> AppendEntries.Reply {
    let target = try await target(peer)
    return try await Self.timed(.milliseconds(650)) {
      try await target.service.append(request, group: group, cluster: cluster)
    }
  }

  func install(
    peer: PeerId, group: String, image: Data, request: InstallSnapshot.Args
  ) async throws -> InstallSnapshot.Reply {
    let target = try await target(peer)
    var offset = 0
    while offset < image.count {
      let chunk = image.subdata(in: offset..<min(offset + Self.snapshotChunkBytes, image.count))
      let args = InstallSnapshot.Args(
        term: request.term, rpcId: request.rpcId, leaderId: request.leaderId,
        lastIncludedIndex: request.lastIncludedIndex, lastIncludedTerm: request.lastIncludedTerm,
        offset: UInt64(offset), totalLength: request.totalLength, checksum: request.checksum, data: chunk)
      let reply = try await Self.timed(.seconds(10)) {
        try await target.service.install(args, group: group, cluster: cluster)
      }
      guard reply.rpcId == args.rpcId, reply.lastIncludedIndex == args.lastIncludedIndex else {
        throw RPCError.invalidMessage
      }
      if reply.term > args.term || reply.installed { return reply }
      guard reply.term == args.term, reply.nextOffset > UInt64(offset), reply.nextOffset < UInt64(image.count) else {
        throw RPCError.invalidMessage
      }
      offset = Int(reply.nextOffset)
    }
    throw RPCError.invalidMessage
  }

  private func targets() async -> [Target] {
    guard let receptionist else { return [] }
    return await receptionist.services().compactMap { candidate in
      guard candidate.registration.key == RaftService.key.name,
        candidate.registration.actorType == String(reflecting: RaftService.self),
        let service = try? RaftService.resolve(id: candidate.registration.address, using: system)
      else { return nil }
      return Target(node: candidate.node, service: service)
    }
  }

  private func target(_ peer: String) async throws -> Target {
    let id = UUID(uuidString: peer)
    let matches = await targets().filter { target in
      if let id { return target.node.id == id }
      return target.node.name == peer
    }
    guard let match = matches.first else { throw RPCError.unknownActor }
    guard matches.count == 1 else { throw IndraError("Ambiguous Raft peer. Specify the node UUID.") }
    return match
  }

  private func reports() async -> [Report] {
    let targets = await targets()
    return await withTaskGroup(of: Report?.self) { tasks in
      for target in targets {
        tasks.addTask {
          guard let status = try? await Self.timed(.milliseconds(600), run: {
            try await target.service.status(cluster: cluster)
          }), status.node == target.node.id.uuidString else { return nil }
          return Report(target: target, status: status)
        }
      }
      var reports: [Report] = []
      for await result in tasks {
        if let result { reports.append(result) }
      }
      return reports
    }
  }

  private static func timed<Value: Sendable>(
    _ timeout: Duration, run: @escaping @Sendable () async throws -> Value
  ) async throws -> Value {
    try await withThrowingTaskGroup(of: Value.self) { tasks in
      tasks.addTask { try await run() }
      tasks.addTask {
        try await Task.sleep(for: timeout)
        throw RPCError.timedOut
      }
      defer { tasks.cancelAll() }
      guard let result = try await tasks.next() else { throw RPCError.timedOut }
      return result
    }
  }
}
