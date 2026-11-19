import Distributed
import Foundation
import TCPActorSystem

public actor Receptionist {
  private static let registrationLimit = 1024
  private let system: TCPActorSystem
  private let cluster: String
  private var node: NodeIdentity
  private var stopped = false
  private var revision: UInt64 = 0
  private var membershipRevision: UInt64 = 0
  private var local: [ServiceRegistration: any DistributedActor] = [:]
  private var peers: [ActorEndpoint: ServicePeer] = [:]
  private var watchers: [UUID: ServiceWatcher] = [:]
  private var commands: [String: @Sendable (Data) async throws -> Data] = [:]
  private var queries: [String: @Sendable (isolated Receptionist, NodeStatus) async throws -> Data] = [:]

  init(system: TCPActorSystem, cluster: String, node: NodeIdentity) {
    self.system = system
    self.cluster = cluster
    self.node = node
  }

  func requireRegistrationCapacity(for count: Int) throws {
    guard !stopped else { throw IndraError("Receptionist is stopping") }
    guard count <= Self.registrationLimit - local.count else {
      throw IndraError("Too many local service registrations")
    }
  }

  func query(_ name: String, status: NodeStatus) async throws -> Data {
    guard !stopped else { throw IndraError("Receptionist is stopping") }
    guard let run = queries[name] else { throw IndraError("Unknown service query \(name)") }
    return try await run(self, status)
  }

  func command(_ name: String, payload: Data) async throws -> Data {
    guard !stopped else { throw IndraError("Receptionist is stopping") }
    guard let run = commands[name] else { throw IndraError("Unknown local service command \(name)") }
    return try await run(payload)
  }

  public func registerCommand<Input: Codable & Sendable, Output: Codable & Sendable>(
    _ name: String, run: @escaping @Sendable (Input) async throws -> Output
  ) throws {
    guard !stopped else { throw IndraError("Receptionist is stopping") }
    guard commands[name] == nil else { throw IndraError("Duplicate local service command \(name)") }
    commands[name] = { payload in
      let input = try JSONDecoder().decode(Input.self, from: payload)
      let output = try await run(input)
      return try JSONEncoder().encode(output)
    }
  }

  public func register<Act>(_ actor: Act, with key: ServiceKey<Act>) throws {
    guard !stopped else { throw IndraError("Receptionist is stopping") }
    let registration = ServiceRegistration(key: key.name, actorType: String(reflecting: Act.self), address: actor.id)
    try registration.validate(owner: node)
    guard try Act.resolve(id: actor.id, using: system) === actor else {
      throw IndraError("Only local actors can register with this receptionist")
    }
    guard local[registration] == nil else { return }
    try requireRegistrationCapacity(for: 1)
    local[registration] = actor
    revision += 1
    notifyWatchers()
  }

  public func registerQuery<Act, Value: Codable & Sendable>(
    _ actor: Act, with key: ServiceKey<Act>, timeout: Duration,
    sample: @escaping @Sendable (Act, String) async throws -> Value
  ) throws {
    guard queries[key.name] == nil else { throw IndraError("Duplicate service query name \(key.name)") }
    try register(actor, with: key)
    queries[key.name] = { receptionist, status in
      let actors = try receptionist.lookup(key)
      let reports = try await collectReports(actors: actors, status: status, timeout: timeout, sample: sample)
      return try JSONEncoder().encode(reports)
    }
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
    let discovered = peers.values.flatMap { peer in
      (peer.snapshot?.registrations ?? []).map {
        DiscoveredService(node: peer.identity, registration: $0)
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

  func update(node: NodeIdentity, reachable: [ActorEndpoint: NodeIdentity], membershipRevision: UInt64) -> Bool {
    guard !stopped, membershipRevision > self.membershipRevision else { return false }
    self.membershipRevision = membershipRevision
    self.node = node
    let endpointsChanged = reachable.count != peers.count || reachable.keys.contains { peers[$0] == nil }
    guard endpointsChanged || reachable.contains(where: { peers[$0.key]?.identity != $0.value }) else { return false }
    peers = reachable.mapValues { identity in
      var peer = peers[identity.endpoint] ?? ServicePeer(identity: identity, generation: UUID(), snapshot: nil)
      peer.identity = identity
      return peer
    }
    notifyWatchers()
    return endpointsChanged
  }

  func refresh() async {
    let identities = peers.values.map(\.identity)
    await withTaskGroup(of: Void.self) { tasks in
      var iterator = identities.makeIterator()
      for _ in 0..<min(8, identities.count) {
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
    guard !stopped, let current = peers[peer.endpoint] else { return }
    guard snapshot.version == 1, snapshot.cluster == cluster,
      snapshot.node.id == peer.id, snapshot.node.endpoint == peer.endpoint
    else { throw IndraError("Service snapshot owner mismatch") }
    try snapshot.node.validate()
    guard snapshot.registrations.count <= Self.registrationLimit else {
      throw IndraError("Invalid service snapshot registrations")
    }
    let registrations = Set(snapshot.registrations)
    guard registrations.count == snapshot.registrations.count else {
      throw IndraError("Invalid service snapshot registrations")
    }
    for registration in snapshot.registrations { try registration.validate(owner: peer) }
    var changed = !registrations.isEmpty
    if let previous = current.snapshot {
      guard snapshot.revision >= previous.revision else { return }
      changed = registrations != Set(previous.registrations)
      guard snapshot.revision != previous.revision || !changed else {
        throw IndraError("Conflicting service snapshot revision")
      }
    }
    peers[peer.endpoint]?.snapshot = snapshot
    if changed { notifyWatchers() }
  }

  func shutdown() {
    stopped = true
    local.removeAll()
    peers.removeAll()
    queries.removeAll()
    commands.removeAll()
    for watcher in watchers.values { watcher.finish() }
    watchers.removeAll()
  }

  private func refresh(_ peer: NodeIdentity) async {
    guard !stopped, !Task.isCancelled, let generation = peers[peer.endpoint]?.generation else { return }
    do {
      let remote = try ReceptionistService.resolve(
        id: ActorAddress(endpoint: peer.endpoint, id: ReceptionistService.fixedID), using: system)
      let snapshot = try await remote.snapshot(version: 1, cluster: cluster)
      try Task.checkCancellation()
      guard peers[peer.endpoint]?.generation == generation else { return }
      try accept(snapshot, from: peer)
    } catch {
      guard !stopped, !Task.isCancelled, let current = peers[peer.endpoint],
        current.generation == generation, let snapshot = current.snapshot
      else { return }
      peers[peer.endpoint]?.snapshot = nil
      if !snapshot.registrations.isEmpty { notifyWatchers() }
    }
  }

  private func registrations<Act>(for key: ServiceKey<Act>) -> [ServiceRegistration] {
    services().map(\.registration).filter { $0.key == key.name && $0.actorType == String(reflecting: Act.self) }
  }

  private func notifyWatchers() {
    guard !watchers.isEmpty else { return }
    let registrations = services().map(\.registration)
    for (id, watcher) in watchers {
      let listing = registrations.filter {
        $0.key == watcher.key && $0.actorType == watcher.actorType
      }
      if listing != watcher.previous {
        watchers[id]?.previous = listing
        watcher.yield(listing)
      }
    }
  }

  private func removeWatcher(_ id: UUID) {
    watchers[id] = nil
  }
}

private struct ServicePeer {
  var identity: NodeIdentity
  let generation: UUID
  var snapshot: ServiceSnapshot?
}

private struct ServiceWatcher {
  let key: String
  let actorType: String
  var previous: [ServiceRegistration]
  let yield: @Sendable ([ServiceRegistration]) -> Void
  let finish: @Sendable () -> Void
}
