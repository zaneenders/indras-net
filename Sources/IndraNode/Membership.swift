import Distributed
public import Foundation
public import TCPActorSystem

public struct NodeIdentity: Codable, Hashable, Sendable {
  public let id: UUID
  public let name: String
  public let endpoint: ActorEndpoint

  var address: PeerAddress { PeerAddress(host: endpoint.host, port: endpoint.port) }

  func validate() throws {
    try address.validate()
    guard !name.isEmpty, name.utf8.count <= 128 else { throw IndraError("Invalid peer name") }
  }
}

public struct Member: Codable, Sendable {
  public enum State: String, Codable, Sendable { case unknown, reachable, unreachable }
  public let node: NodeIdentity
  public var state: State
  public var lastSeen: Double?
  public var error: String?
}

public struct NodeStatus: Codable, Sendable {
  public let node: NodeIdentity
  public let cluster: String
  public let members: [Member]
  public let seeds: [PeerAddress]
  public let configurationError: String?
  public let discoveryErrors: [String: String]
}

public struct PingResult: Codable, Sendable {
  public let node: NodeIdentity
  public let milliseconds: Double
}

struct MembershipExchange: Codable, Sendable {
  let version: Int
  let cluster: String
  let node: NodeIdentity
  let peers: [NodeIdentity]
}

distributed actor MembershipService {
  typealias ActorSystem = TCPActorSystem
  static let serviceName = "indra.membership.v1"
  private let runtime: NodeRuntime

  init(actorSystem: TCPActorSystem, runtime: NodeRuntime) {
    self.actorSystem = actorSystem
    self.runtime = runtime
  }

  distributed func exchange(_ message: MembershipExchange) async throws -> MembershipExchange {
    try await runtime.receive(message)
  }
}

public actor NodeRuntime {
  public let system: TCPActorSystem
  private let configurationPath: String
  private var configuration: NodeConfiguration
  private var members: [UUID: Member] = [:]
  private var membershipServices: [PeerAddress: MembershipService] = [:]
  private var hints: Set<PeerAddress> = []
  private var configurationError: String?
  private var discoveryErrors: [String: String] = [:]
  private var stopped = false
  private var discovering = false
  private var discoveryRequested = false
  private var discoverySleep: Task<Void, any Error>?

  public init(configuration: NodeConfiguration, configurationPath: String, system: TCPActorSystem) {
    self.configuration = configuration
    self.configurationPath = configurationPath
    self.system = system
  }

  public func start() throws {
    try system.publish(MembershipService(actorSystem: system, runtime: self), as: MembershipService.serviceName)
  }

  public func status() -> NodeStatus {
    NodeStatus(
      node: identity, cluster: configuration.cluster,
      members: members.values.sorted { $0.node.id.uuidString < $1.node.id.uuidString },
      seeds: configuration.seeds, configurationError: configurationError, discoveryErrors: discoveryErrors)
  }

  public func reload() throws {
    do {
      let updated = try NodeConfiguration.load(path: configurationPath)
      guard configuration.canReload(updated) else {
        throw IndraError("Identity, cluster, listen/advertised address, port, and timeout changes require a restart")
      }
      apply(updated)
      configurationError = nil
    } catch {
      configurationError = String(describing: error)
      throw error
    }
  }

  public func join(_ address: PeerAddress) async throws -> PingResult {
    try address.validate()
    try reload()
    if !configuration.seeds.contains(address) {
      var updated = configuration
      updated.seeds.append(address)
      try updated.save(path: configurationPath, overwrite: true)
      apply(updated)
    }
    return try await contact(address)
  }

  public func ping(_ target: String) async throws -> PingResult {
    let matches = ([identity] + members.values.map(\.node)).filter {
      $0.id.uuidString.lowercased() == target.lowercased() || $0.name == target
    }
    if matches.count > 1 { throw IndraError("Ambiguous node name; use its UUID or host:port") }
    if let match = matches.first {
      if match.id == identity.id { return PingResult(node: identity, milliseconds: 0) }
      let result = try await contact(match.address)
      guard result.node.id == match.id else { throw IndraError("Address now belongs to a different node") }
      return result
    }
    return try await contact(PeerAddress(target))
  }

  public func discover() async {
    guard !stopped, !discovering else { return }
    discovering = true
    defer { discovering = false }
    let targets = Set(configuration.seeds).union(hints).union(members.values.map { $0.node.address })
      .subtracting([identity.address])
    // Bound concurrent probes so unavailable peers cannot serialize the whole sweep.
    await withTaskGroup(of: Void.self) { tasks in
      var pending = targets.makeIterator()
      for _ in 0..<8 {
        if let address = pending.next() { tasks.addTask { _ = try? await self.contact(address) } }
      }
      while await tasks.next() != nil {
        if Task.isCancelled {
          tasks.cancelAll()
          break
        }
        if let address = pending.next() { tasks.addTask { _ = try? await self.contact(address) } }
      }
    }
  }

  public func runDiscovery() async throws {
    while !stopped {
      try Task.checkCancellation()
      discoveryRequested = false
      await discover()
      guard !stopped else { return }
      if discoveryRequested { continue }
      let interval = configuration.probeIntervalMilliseconds
      let sleep = Task { try await Task.sleep(for: .milliseconds(interval)) }
      discoverySleep = sleep
      await withTaskCancellationHandler {
        _ = try? await sleep.value
      } onCancel: {
        sleep.cancel()
      }
      discoverySleep = nil
    }
  }

  public func shutdown() async {
    stopped = true
    discoverySleep?.cancel()
    membershipServices.removeAll()
    await system.shutdown()
  }

  func receive(_ message: MembershipExchange) throws -> MembershipExchange {
    guard !stopped else { throw IndraError("Node is stopping") }
    try validate(message)
    learn(message.node)
    for peer in message.peers { learn(peer) }
    return exchange(excluding: message.node.endpoint)
  }

  private var identity: NodeIdentity {
    NodeIdentity(id: configuration.id, name: configuration.name, endpoint: system.endpoint)
  }

  private func apply(_ updated: NodeConfiguration) {
    guard configuration != updated else { return }
    configuration = updated
    discoveryRequested = true
    discoverySleep?.cancel()
  }

  private func exchange(excluding endpoint: ActorEndpoint) -> MembershipExchange {
    let address = PeerAddress(host: endpoint.host, port: endpoint.port)
    return MembershipExchange(
      version: 1, cluster: configuration.cluster, node: identity,
      peers: members.values.filter { $0.state == .reachable && $0.node.address != address }.map(\.node))
  }

  private func validate(_ message: MembershipExchange) throws {
    guard message.version == 1, message.cluster == configuration.cluster else {
      throw IndraError("Peer uses a different cluster or protocol version")
    }
    guard message.peers.count <= 256 else { throw IndraError("Too many peers in membership exchange") }
    for node in [message.node] + message.peers {
      try node.validate()
      if node.id == identity.id, node.endpoint != identity.endpoint {
        throw IndraError("Duplicate node ID; generate a separate configuration for each node")
      }
    }
  }

  private func learn(_ node: NodeIdentity) {
    guard node.id != identity.id else { return }
    if let current = members[node.id] {
      if current.node.endpoint != node.endpoint, hints.count < 256 { hints.insert(node.address) }
    } else if members.count < 256 {
      members[node.id] = Member(node: node, state: .unknown, lastSeen: nil, error: nil)
    }
  }

  private func contact(_ address: PeerAddress) async throws -> PingResult {
    guard !stopped else { throw IndraError("Node is stopping") }
    let clock = ContinuousClock()
    let start = clock.now
    do {
      let (remote, response) = try await exchangeMembership(with: address)
      guard !stopped else { throw CancellationError() }
      try validate(response)
      guard response.node.endpoint == remote.id.endpoint else { throw IndraError("Peer endpoint mismatch") }
      for id in Array(members.keys) where id != response.node.id && members[id]?.node.address == response.node.address {
        members[id]?.state = .unreachable
        members[id]?.error = "Address now belongs to a different node"
      }
      if response.node.id != identity.id {
        guard members[response.node.id] != nil || members.count < 256 else { throw IndraError("Peer registry is full") }
        members[response.node.id] = Member(
          node: response.node, state: .reachable, lastSeen: Date().timeIntervalSince1970, error: nil)
      }
      for node in response.peers { learn(node) }
      if membershipServices[address] != nil || membershipServices.count < 256 {
        membershipServices[address] = remote
      }
      hints.remove(address)
      discoveryErrors[address.description] = nil
      let elapsed = start.duration(to: clock.now).components
      return PingResult(
        node: response.node, milliseconds: Double(elapsed.seconds) * 1000 + Double(elapsed.attoseconds) / 1e15)
    } catch {
      if !Task.isCancelled && !stopped {
        let message = String(describing: error)
        if discoveryErrors.count < 512 || discoveryErrors[address.description] != nil {
          discoveryErrors[address.description] = message
        }
        for id in Array(members.keys) where members[id]?.node.address == address {
          members[id]?.state = .unreachable
          members[id]?.error = message
        }
      }
      throw error
    }
  }

  private func exchangeMembership(with address: PeerAddress) async throws -> (MembershipService, MembershipExchange) {
    if let remote = membershipServices[address] {
      do {
        return (remote, try await remote.exchange(exchange(excluding: remote.id.endpoint)))
      } catch let error as RPCError where error == .disconnected || error == .unknownActor {
        if membershipServices[address]?.id == remote.id { membershipServices[address] = nil }
      }
    }
    try Task.checkCancellation()
    guard !stopped else { throw IndraError("Node is stopping") }
    let remote = try await system.service(
      named: MembershipService.serviceName, host: address.host, port: address.port, as: MembershipService.self)
    return (remote, try await remote.exchange(exchange(excluding: remote.id.endpoint)))
  }
}
