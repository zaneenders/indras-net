public import Distributed
import Foundation
public import TCPActorSystem

public struct ServiceKey<Act>: Hashable, Sendable
where Act: DistributedActor, Act.ActorSystem == TCPActorSystem {
  public let name: String

  public init(_ name: String) {
    self.name = name
  }
}

public struct ServiceRegistration: Codable, Hashable, Sendable {
  public let key: String
  public let actorType: String
  public let address: ActorAddress

  func validate(owner: NodeIdentity) throws {
    guard !key.isEmpty, key.utf8.count <= 128,
      !actorType.isEmpty, actorType.utf8.count <= 1024,
      address.endpoint == owner.endpoint
    else { throw IndraError("Invalid service registration") }
  }
}

public struct DiscoveredService: Codable, Hashable, Sendable {
  public let node: NodeIdentity
  public let registration: ServiceRegistration
}

public actor Receptionist {
  private let system: TCPActorSystem
  private let cluster: String
  private var node: NodeIdentity
  private var stopped = false
  private var revision: UInt64 = 0
  private var local: [ServiceRegistration: any DistributedActor] = [:]
  private var members: [ActorEndpoint: NodeIdentity] = [:]
  private var generations: [ActorEndpoint: UUID] = [:]
  private var snapshots: [ActorEndpoint: ServiceSnapshot] = [:]
  private var remotes: [ActorEndpoint: ReceptionistService] = [:]
  private var watchers: [UUID: ServiceWatcher] = [:]

  init(system: TCPActorSystem, cluster: String, node: NodeIdentity) {
    self.system = system
    self.cluster = cluster
    self.node = node
  }

  func start() throws {
    let service = ReceptionistService(actorSystem: system, receptionist: self)
    try system.publish(service, as: ReceptionistService.serviceName)
    try register(service, with: ServiceKey<ReceptionistService>(ReceptionistService.serviceName))
  }

  public func register<Act>(_ actor: Act, with key: ServiceKey<Act>) throws {
    guard !stopped else { throw IndraError("Receptionist is stopping") }
    let registration = ServiceRegistration(key: key.name, actorType: String(reflecting: Act.self), address: actor.id)
    try registration.validate(owner: node)
    guard try Act.resolve(id: actor.id, using: system) === actor else {
      throw IndraError("Only local actors can register with this receptionist")
    }
    guard local[registration] == nil else { return }
    guard local.count < 1024 else { throw IndraError("Too many local service registrations") }
    local[registration] = actor
    revision += 1
    notifyWatchers()
  }

  public func unregister<Act>(_ actor: Act, from key: ServiceKey<Act>) {
    let registration = ServiceRegistration(key: key.name, actorType: String(reflecting: Act.self), address: actor.id)
    guard local.removeValue(forKey: registration) != nil else { return }
    revision += 1
    notifyWatchers()
  }

  public func lookup<Act>(_ key: ServiceKey<Act>) throws -> [Act] {
    guard !stopped else { throw IndraError("Receptionist is stopping") }
    return try registrations(for: key).map { try Act.resolve(id: $0.address, using: system) }
  }

  public func watch<Act>(_ key: ServiceKey<Act>) throws -> AsyncStream<[Act]> {
    guard !stopped else { throw IndraError("Receptionist is stopping") }
    let id = UUID()
    let (stream, continuation) = AsyncStream<[Act]>.makeStream(bufferingPolicy: .bufferingNewest(1))
    continuation.onTermination = { [weak self] _ in
      Task { await self?.removeWatcher(id) }
    }
    let system = self.system
    watchers[id] = ServiceWatcher(
      key: key.name, actorType: String(reflecting: Act.self), previous: [],
      yield: { registrations in
        continuation.yield(registrations.compactMap { try? Act.resolve(id: $0.address, using: system) })
      }, finish: { continuation.finish() })
    let registrations = registrations(for: key)
    watchers[id]?.previous = registrations
    watchers[id]?.yield(registrations)
    return stream
  }

  public func services() -> [DiscoveredService] {
    let owned = local.keys.map { DiscoveredService(node: node, registration: $0) }
    let discovered = snapshots.values.flatMap { snapshot in
      snapshot.registrations.map {
        DiscoveredService(node: members[snapshot.node.endpoint] ?? snapshot.node, registration: $0)
      }
    }
    return (owned + discovered).sorted {
      let left = $0.registration
      let right = $1.registration
      if left.key != right.key { return left.key < right.key }
      if left.actorType != right.actorType { return left.actorType < right.actorType }
      if $0.node.id != $1.node.id { return $0.node.id.uuidString < $1.node.id.uuidString }
      return left.address.id.uuidString < right.address.id.uuidString
    }
  }

  func update(node: NodeIdentity, members: [Member]) -> Bool {
    guard !stopped else { return false }
    let previous = Set(self.members.keys)
    self.node = node
    self.members = Dictionary(
      members.filter { $0.state == .alive || $0.state == .suspect }.map { ($0.node.endpoint, $0.node) },
      uniquingKeysWith: { first, _ in first })
    generations = Dictionary(uniqueKeysWithValues: self.members.keys.map { ($0, generations[$0] ?? UUID()) })
    snapshots = snapshots.filter { self.members[$0.key] != nil }
    remotes = remotes.filter { self.members[$0.key] != nil }
    notifyWatchers()
    return Set(self.members.keys) != previous
  }

  func refresh() async {
    let peers = Array(members.values)
    await withTaskGroup(of: Void.self) { tasks in
      var iterator = peers.makeIterator()
      for _ in 0..<min(8, peers.count) {
        if let peer = iterator.next() { tasks.addTask { await self.refresh(peer) } }
      }
      while await tasks.next() != nil {
        if !Task.isCancelled, let peer = iterator.next() { tasks.addTask { await self.refresh(peer) } }
      }
    }
  }

  func snapshot(version: Int, cluster: String) throws -> ServiceSnapshot {
    guard !stopped else { throw IndraError("Receptionist is stopping") }
    guard version == 1, cluster == self.cluster else {
      throw IndraError("Peer uses a different cluster or receptionist protocol version")
    }
    return ServiceSnapshot(
      version: 1, cluster: cluster, node: node, revision: revision, registrations: Array(local.keys))
  }

  func accept(_ snapshot: ServiceSnapshot, from peer: NodeIdentity) throws {
    guard !stopped, members[peer.endpoint] != nil else { return }
    guard snapshot.version == 1, snapshot.cluster == cluster,
      snapshot.node.id == peer.id, snapshot.node.endpoint == peer.endpoint
    else { throw IndraError("Service snapshot owner mismatch") }
    try snapshot.node.validate()
    guard snapshot.registrations.count <= 1024,
      Set(snapshot.registrations).count == snapshot.registrations.count
    else { throw IndraError("Invalid service snapshot registrations") }
    for registration in snapshot.registrations { try registration.validate(owner: peer) }
    if let previous = snapshots[peer.endpoint] {
      guard snapshot.revision >= previous.revision else { return }
      guard snapshot.revision != previous.revision || Set(snapshot.registrations) == Set(previous.registrations) else {
        throw IndraError("Conflicting service snapshot revision")
      }
    }
    snapshots[peer.endpoint] = snapshot
    notifyWatchers()
  }

  func shutdown() {
    stopped = true
    system.unpublish(ReceptionistService.serviceName)
    local.removeAll()
    members.removeAll()
    generations.removeAll()
    snapshots.removeAll()
    remotes.removeAll()
    for watcher in watchers.values { watcher.finish() }
    watchers.removeAll()
  }

  private func refresh(_ peer: NodeIdentity) async {
    guard !stopped, !Task.isCancelled, let generation = generations[peer.endpoint] else { return }
    do {
      let remote: ReceptionistService
      if let cached = remotes[peer.endpoint] {
        remote = cached
      } else {
        remote = try await system.service(
          named: ReceptionistService.serviceName, host: peer.endpoint.host,
          port: peer.endpoint.port, as: ReceptionistService.self)
      }
      guard remote.id.endpoint == peer.endpoint else { throw IndraError("Receptionist endpoint mismatch") }
      let snapshot = try await remote.snapshot(version: 1, cluster: cluster)
      try Task.checkCancellation()
      guard generations[peer.endpoint] == generation else { return }
      try accept(snapshot, from: peer)
      if !stopped, members[peer.endpoint] != nil { remotes[peer.endpoint] = remote }
    } catch {
      guard !stopped, !Task.isCancelled, generations[peer.endpoint] == generation else { return }
      snapshots[peer.endpoint] = nil
      remotes[peer.endpoint] = nil
      notifyWatchers()
    }
  }

  private func registrations<Act>(for key: ServiceKey<Act>) -> [ServiceRegistration] {
    services().map(\.registration).filter { $0.key == key.name && $0.actorType == String(reflecting: Act.self) }
  }

  private func notifyWatchers() {
    let services = services()
    for (id, watcher) in watchers {
      let registrations = services.map(\.registration).filter {
        $0.key == watcher.key && $0.actorType == watcher.actorType
      }
      if registrations != watcher.previous {
        watchers[id]?.previous = registrations
        watcher.yield(registrations)
      }
    }
  }

  private func removeWatcher(_ id: UUID) {
    watchers[id] = nil
  }
}

struct ServiceSnapshot: Codable, Sendable {
  let version: Int
  let cluster: String
  let node: NodeIdentity
  let revision: UInt64
  let registrations: [ServiceRegistration]
}

private struct ServiceWatcher {
  let key: String
  let actorType: String
  var previous: [ServiceRegistration]
  let yield: @Sendable ([ServiceRegistration]) -> Void
  let finish: @Sendable () -> Void
}

distributed actor ReceptionistService {
  typealias ActorSystem = TCPActorSystem
  static let serviceName = "indra.receptionist.v1"
  private let receptionist: Receptionist

  init(actorSystem: TCPActorSystem, receptionist: Receptionist) {
    self.actorSystem = actorSystem
    self.receptionist = receptionist
  }

  distributed func snapshot(version: Int, cluster: String) async throws -> ServiceSnapshot {
    try await receptionist.snapshot(version: version, cluster: cluster)
  }
}
