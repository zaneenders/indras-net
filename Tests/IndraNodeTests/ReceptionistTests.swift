import Distributed
import Foundation
import NIOCore
import TCPActorSystem
import Testing

@testable import IndraNode

@Suite(.timeLimit(.minutes(1)))
struct ReceptionistTests {
  @Test func typedKeysWatchRegistrationAndWithdrawal() async throws {
    try await withServiceNodes { nodes in
      let node = nodes[0]
      let key = ServiceKey<ReceptionistWorker>("workers")
      let otherKey = ServiceKey<OtherReceptionistWorker>("workers")
      let receptionist = node.runtime.receptionist
      var updates = try await receptionist.watch(key).makeAsyncIterator()
      #expect(await updates.next()?.isEmpty == true)
      let first = ReceptionistWorker(actorSystem: node.system, name: "first")
      let second = ReceptionistWorker(actorSystem: node.system, name: "second")
      let other = OtherReceptionistWorker(actorSystem: node.system)
      try await receptionist.register(first, with: key)
      #expect(await updates.next()?.map(\.id) == [first.id])
      try await receptionist.register(first, with: key)
      try await receptionist.register(other, with: otherKey)
      try await receptionist.register(second, with: key)
      let both = try #require(await updates.next())
      #expect(Set(both.map(\.id)) == [first.id, second.id])
      #expect(try await receptionist.lookup(otherKey).map(\.id) == [other.id])
      await receptionist.unregister(first, from: key)
      #expect(await updates.next()?.map(\.id) == [second.id])
      await receptionist.unregister(second, from: key)
      #expect(await updates.next()?.isEmpty == true)
      await receptionist.shutdown()
      #expect(await updates.next() == nil)
      await #expect(throws: IndraError.self) { try await receptionist.lookup(key) }
      await #expect(throws: IndraError.self) { try await receptionist.register(first, with: key) }
    }
  }

  @Test func registrationsRetainActorsAndRejectRemoteActorsAndInvalidKeys() async throws {
    try await withServiceNodes { nodes in
      let owner = nodes[0]
      let client = nodes[1]
      let key = ServiceKey<ReceptionistWorker>("workers")
      var worker: ReceptionistWorker? = ReceptionistWorker(actorSystem: owner.system, name: "retained")
      let address = try #require(worker?.id)
      try await owner.runtime.receptionist.register(try #require(worker), with: key)
      worker = nil
      #expect(try ReceptionistWorker.resolve(id: address, using: owner.system).id == address)
      let remote = try ReceptionistWorker.resolve(id: address, using: client.system)
      await #expect(throws: IndraError.self) {
        try await client.runtime.receptionist.register(remote, with: key)
      }
      do {
        let retained = try ReceptionistWorker.resolve(id: address, using: owner.system)
        for invalid in ["", String(repeating: "x", count: 129)] {
          await #expect(throws: IndraError.self) {
            try await owner.runtime.receptionist.register(retained, with: ServiceKey<ReceptionistWorker>(invalid))
          }
        }
        await owner.runtime.receptionist.unregister(retained, from: key)
      }
      #expect(throws: RPCError.unknownActor) { try ReceptionistWorker.resolve(id: address, using: owner.system) }
    }
  }

  @Test func threeNodesDiscoverWorkersCallThemAndObserveWithdrawalAndRestart() async throws {
    try await withServiceNodes { nodes in
      let first = nodes[0]
      let second = nodes[1]
      let client = nodes[2]
      let key = ServiceKey<ReceptionistWorker>("workers")
      let alpha = ReceptionistWorker(actorSystem: first.system, name: "alpha")
      let beta = ReceptionistWorker(actorSystem: second.system, name: "beta")
      try await first.runtime.receptionist.register(alpha, with: key)
      try await second.runtime.receptionist.register(beta, with: key)
      let pool = DiscoveredWorkerPool()
      let listings = try await client.runtime.receptionist.watch(key)
      let watching = Task {
        for await workers in listings { await pool.update(workers) }
      }
      defer { watching.cancel() }
      try await servicesEventually { await pool.addresses() == Set([alpha.id, beta.id]) }
      #expect(try await pool.run("hello").sorted() == ["alpha: hello", "beta: hello"])

      await second.runtime.receptionist.unregister(beta, from: key)
      try await servicesEventually { await pool.addresses() == Set([alpha.id]) }
      try await second.runtime.receptionist.register(beta, with: key)
      try await servicesEventually { await pool.addresses().count == 2 }
      try await second.stop()
      try await servicesEventually { await pool.addresses() == Set([alpha.id]) }
      let replacement = try await ServiceTestNode.start(configuration: second.configuration)
      do {
        let restarted = ReceptionistWorker(actorSystem: replacement.system, name: "beta-restarted")
        try await replacement.runtime.receptionist.register(restarted, with: key)
        try await servicesEventually { await pool.addresses() == Set([alpha.id, restarted.id]) }
        #expect(restarted.id.endpoint.node != beta.id.endpoint.node)
        #expect(try await pool.run("again").sorted() == ["alpha: again", "beta-restarted: again"])
        try await replacement.stop()
      } catch {
        try? await replacement.stop()
        throw error
      }
    }
  }

  @Test func snapshotsValidateAtomicallyAndCannotRestoreOldRegistrations() async throws {
    try await withServiceNodes { nodes in
      let owner = nodes[0]
      let client = nodes[1]
      let receptionist = client.runtime.receptionist
      let peer = await owner.runtime.status().node
      let local = await client.runtime.status().node
      let worker = ReceptionistWorker(actorSystem: owner.system, name: "worker")
      let key = ServiceKey<ReceptionistWorker>("snapshot-workers")
      let registration = ServiceRegistration(
        key: key.name, actorType: String(reflecting: ReceptionistWorker.self), address: worker.id)
      func gossip(_ status: GossipStatus, sequenceNumber: UInt32) async throws {
        _ = try await client.runtime.receive(
          MembershipRequest(
            version: 1, cluster: "test", node: peer,
            probe: .ping(
              sequenceNumber: sequenceNumber,
              payload: [GossipMember(node: peer, status: status, protocolPeriod: 0)])))
      }
      // Stop background refresh so manually ordered snapshots remain deterministic.
      for node in nodes { node.task.cancel() }
      for node in nodes { _ = try? await node.task.value }
      try await gossip(.alive(1), sequenceNumber: 1)
      let initial = ServiceSnapshot(version: 1, cluster: "test", node: peer, revision: 1, registrations: [registration])
      try await receptionist.accept(initial, from: peer)
      #expect(try await receptionist.lookup(key).map(\.id) == [worker.id])
      let renamed = NodeIdentity(id: peer.id, name: "renamed", endpoint: peer.endpoint, build: peer.build)
      _ = try await client.runtime.receive(
        MembershipRequest(version: 1, cluster: "test", node: renamed, probe: .ping(sequenceNumber: 2, payload: [])))
      #expect(await receptionist.services().first { $0.registration == registration }?.node == renamed)
      let foreign = ServiceRegistration(
        key: key.name, actorType: registration.actorType,
        address: ActorAddress(endpoint: client.system.endpoint, id: UUID()))
      for invalid in [
        ServiceSnapshot(version: 2, cluster: "test", node: peer, revision: 2, registrations: []),
        ServiceSnapshot(version: 1, cluster: "foreign", node: peer, revision: 2, registrations: []),
        ServiceSnapshot(version: 1, cluster: "test", node: local, revision: 2, registrations: []),
        ServiceSnapshot(version: 1, cluster: "test", node: peer, revision: 2, registrations: [foreign]),
        ServiceSnapshot(
          version: 1, cluster: "test", node: peer, revision: 2, registrations: [registration, registration]),
        ServiceSnapshot(version: 1, cluster: "test", node: peer, revision: 1, registrations: [])
      ] {
        await #expect(throws: IndraError.self) { try await receptionist.accept(invalid, from: peer) }
        #expect(try await receptionist.lookup(key).map(\.id) == [worker.id])
      }
      try await receptionist.accept(
        ServiceSnapshot(version: 1, cluster: "test", node: peer, revision: 2, registrations: []), from: peer)
      try await receptionist.accept(initial, from: peer)
      #expect(try await receptionist.lookup(key).isEmpty)

      try await receptionist.accept(
        ServiceSnapshot(version: 1, cluster: "test", node: peer, revision: 3, registrations: [registration]), from: peer
      )
      try await gossip(.unreachable(2), sequenceNumber: 3)
      #expect(!(await receptionist.update(node: local, reachable: [peer.endpoint: peer], membershipRevision: 1)))
      #expect(try await receptionist.lookup(key).isEmpty)
      try await receptionist.accept(initial, from: peer)
      #expect(try await receptionist.lookup(key).isEmpty)
      try await gossip(.alive(3), sequenceNumber: 4)
      await receptionist.refresh()
      // Recovery must obtain the owner's current snapshot, which has no worker registrations.
      #expect(try await receptionist.lookup(key).isEmpty)
      #expect(await receptionist.services().contains { $0.node == peer })
      let remote = try ReceptionistService.resolve(
        id: ActorAddress(endpoint: peer.endpoint, id: ReceptionistService.fixedID), using: client.system)
      await #expect(throws: RemoteActorError.self) { try await remote.snapshot(version: 1, cluster: "foreign") }
    }
  }
}

private distributed actor ReceptionistWorker {
  typealias ActorSystem = TCPActorSystem
  let name: String

  init(actorSystem: TCPActorSystem, name: String) {
    self.actorSystem = actorSystem
    self.name = name
  }

  distributed func run(_ input: String) -> String { "\(name): \(input)" }
}

private distributed actor OtherReceptionistWorker {
  typealias ActorSystem = TCPActorSystem
}

private actor DiscoveredWorkerPool {
  private var workers: [ReceptionistWorker] = []

  func update(_ workers: [ReceptionistWorker]) { self.workers = workers }
  func addresses() -> Set<ActorAddress> { Set(workers.map(\.id)) }

  func run(_ input: String) async throws -> [String] {
    try await withThrowingTaskGroup(of: String.self) { tasks in
      for worker in workers { tasks.addTask { try await worker.run(input) } }
      var results: [String] = []
      for try await result in tasks { results.append(result) }
      return results
    }
  }
}

private struct ServiceTestNode: Sendable {
  let configuration: NodeConfiguration
  let system: TCPActorSystem
  let runtime: NodeRuntime
  let task: Task<Void, any Error>

  static func start(configuration: NodeConfiguration) async throws -> Self {
    let system = try await TCPActorSystem(
      host: configuration.listenHost, advertisedHost: configuration.advertisedHost, port: configuration.port,
      callTimeout: .seconds(2), connectTimeout: .seconds(1))
    let runtime = NodeRuntime(
      configuration: configuration, configurationPath: "/unused", peerHistoryPath: nil, system: system)
    do {
      try await runtime.start()
      var configuration = configuration
      configuration.port = system.endpoint.port
      return Self(
        configuration: configuration, system: system, runtime: runtime,
        task: Task { try await runtime.runMembership() })
    } catch {
      await runtime.shutdown()
      throw error
    }
  }

  func stop() async throws {
    task.cancel()
    await runtime.shutdown()
    do { try await task.value } catch is CancellationError {}
  }
}

private func withServiceNodes(body: @Sendable ([ServiceTestNode]) async throws -> Void) async throws {
  var nodes: [ServiceTestNode] = []
  do {
    for name in ["first", "second", "third"] {
      let seeds = nodes.last.map { [PeerAddress(host: $0.system.endpoint.host, port: $0.system.endpoint.port)] } ?? []
      nodes.append(try await ServiceTestNode.start(configuration: makeConfiguration(name: name, seeds: seeds)))
    }
    try await body(nodes)
    for node in nodes.reversed() { try await node.stop() }
  } catch {
    for node in nodes.reversed() { try? await node.stop() }
    throw error
  }
}

private func servicesEventually(_ condition: @Sendable () async throws -> Bool) async throws {
  let deadline = ContinuousClock.now.advanced(by: .seconds(15))
  while try await !condition() {
    guard ContinuousClock.now < deadline else { throw IndraError("Services did not converge") }
    try await Task.sleep(for: .milliseconds(25))
  }
}
