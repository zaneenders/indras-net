import ClusterMembership
public import Foundation
import SWIM
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
  public enum State: String, Codable, Sendable { case alive, suspect, unreachable, dead }
  public let node: NodeIdentity
  public var state: State
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

private enum MembershipWork: Sendable {
  case bootstrap(PeerAddress)
  case ping(ClusterMembership.Node, SWIM.GossipPayload, Duration, UInt32)
  case indirect(SWIM.Instance.SendPingRequestDirective)
}

public actor NodeRuntime {
  public let system: TCPActorSystem
  private let configurationPath: String
  private var configuration: NodeConfiguration
  private var swim: SWIM.Instance
  private var identities: [ClusterMembership.Node: NodeIdentity] = [:]
  private var membershipServices: [PeerAddress: MembershipService] = [:]
  private var configurationError: String?
  private var discoveryErrors: [String: String] = [:]
  private let work: AsyncStream<MembershipWork>
  private let pendingWork: AsyncStream<MembershipWork>.Continuation
  private var bootstrapping: Set<PeerAddress> = []
  private var seedOffset = 0
  private var stopped = false
  private var membershipRequested = false
  private var membershipSleep: Task<Void, any Error>?

  public init(configuration: NodeConfiguration, configurationPath: String, system: TCPActorSystem) {
    self.configuration = configuration
    self.configurationPath = configurationPath
    self.system = system
    var settings = SWIM.Settings()
    settings.probeInterval = .milliseconds(configuration.probeIntervalMilliseconds)
    settings.pingTimeout = .milliseconds(configuration.requestTimeoutMilliseconds)
    // Membership is informational: unreachable peers can recover after a partition.
    settings.unreachability = .enabled
    let node = NodeIdentity(id: configuration.id, name: configuration.name, endpoint: system.endpoint)
    self.swim = SWIM.Instance(settings: settings, myself: node.swimNode)
    (work, pendingWork) = AsyncStream.makeStream(bufferingPolicy: .unbounded)
  }

  public func start() throws {
    try system.publish(MembershipService(actorSystem: system, runtime: self), as: MembershipService.serviceName)
  }

  static func transportTimeoutMilliseconds(configuration: NodeConfiguration) -> Int64 {
    Int64(configuration.requestTimeoutMilliseconds) * Int64(1 + SWIM.Settings().lifeguard.maxLocalHealthMultiplier)
  }

  public func status() -> NodeStatus {
    var members: [Member] = []
    for member in swim.members where member.node != identity.swimNode {
      guard let node = identities[member.node] else { continue }
      let state: Member.State
      switch member.status {
      case .alive: state = .alive
      case .suspect: state = .suspect
      case .unreachable: state = .unreachable
      case .dead: state = .dead
      }
      members.append(Member(node: node, state: state))
    }
    return NodeStatus(
      node: identity, cluster: configuration.cluster,
      members: members.sorted {
        if $0.node.id == $1.node.id { return $0.node.endpoint.node.uuidString < $1.node.endpoint.node.uuidString }
        return $0.node.id.uuidString < $1.node.id.uuidString
      },
      seeds: configuration.seeds, configurationError: configurationError, discoveryErrors: discoveryErrors)
  }

  public func reload() throws {
    do {
      let updated = try NodeConfiguration.load(path: configurationPath)
      guard configuration.canReload(updated) else {
        throw IndraError("Identity, cluster, listen/advertised address, port, and timeout changes require a restart")
      }
      if updated != configuration {
        configuration = updated
        membershipRequested = true
        membershipSleep?.cancel()
      }
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
      try reload()
    }
    return try await contact(address)
  }

  public func ping(_ target: String) async throws -> PingResult {
    let matches = ([identity] + status().members.map(\.node)).filter {
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

  public func runMembership() async throws {
    try await withThrowingDiscardingTaskGroup { tasks in
      tasks.addTask {
        while !Task.isCancelled {
          let delay = await self.tick()
          try await self.waitForTick(delay)
        }
      }
      for await item in work {
        try Task.checkCancellation()
        tasks.addTask { await self.perform(item) }
      }
      tasks.cancelAll()
    }
  }

  public func shutdown() async {
    stopped = true
    pendingWork.finish()
    membershipSleep?.cancel()
    membershipServices.removeAll()
    await system.shutdown()
  }

  func receive(_ message: MembershipRequest) async throws -> MembershipReply {
    try validate(
      version: message.version, cluster: message.cluster, sender: message.node, payload: message.probe.payload)
    if case .pingRequest(let target, _, _) = message.probe {
      try target.validate()
      guard target.id != identity.id || target.endpoint == identity.endpoint else {
        throw IndraError("Indirect probe targets a previous local process")
      }
    }
    learn(sender: message.node, payload: message.probe.payload)
    let response: MembershipResponse
    switch message.probe {
    case .ping(let sequenceNumber, let payload):
      let directives = swim.onPing(
        pingOrigin: message.node.swimNode,
        payload: gossip(payload), sequenceNumber: sequenceNumber)
      guard
        let ack = try directives.compactMap({ directive -> MembershipResponse? in
          if case .sendAck(_, let target, let incarnation, let payload, let sequenceNumber) = directive {
            return .ack(
              target: try identity(for: target), incarnation: incarnation,
              payload: try wire(payload), sequenceNumber: sequenceNumber)
          }
          return nil
        }).first
      else { throw IndraError("SWIM did not acknowledge ping") }
      response = ack
    case .pingRequest(let target, let sequenceNumber, let payload):
      identities[target.swimNode] = target
      let directives = swim.onPingRequest(
        target: target.swimNode, pingRequestOrigin: message.node.swimNode,
        payload: gossip(payload), sequenceNumber: sequenceNumber)
      var forwarded: MembershipResponse?
      for directive in directives {
        if case .sendPing(let node, let payload, let origin, let originSequence, let timeout, let number) = directive {
          let result = await probe(
            node, payload: payload, timeout: timeout, sequenceNumber: number, pingRequestOrigin: origin)
          guard !stopped, !Task.isCancelled else { throw CancellationError() }
          for reply in swim.onPingResponse(
            response: result, pingRequestOrigin: origin,
            pingRequestSequenceNumber: originSequence)
          {
            switch reply {
            case .sendAck(_, let number, let node, let incarnation, let payload):
              forwarded = .ack(
                target: try identity(for: node), incarnation: incarnation,
                payload: try wire(payload), sequenceNumber: number)
            case .sendNack(_, let number, let node):
              forwarded = .nack(target: try identity(for: node), sequenceNumber: number)
            case .gossipProcessed: break
            case .sendPingRequests:
              throw IndraError("Unexpected recursive indirect SWIM probe")
            }
          }
        }
      }
      response = forwarded ?? .nack(target: target, sequenceNumber: sequenceNumber)
    }
    return MembershipReply(version: 1, cluster: configuration.cluster, node: identity, response: response)
  }

  private var identity: NodeIdentity {
    NodeIdentity(id: configuration.id, name: configuration.name, endpoint: system.endpoint)
  }

  private func identity(for node: ClusterMembership.Node) throws -> NodeIdentity {
    if node == identity.swimNode { return identity }
    guard let identity = identities[node] else { throw IndraError("Missing SWIM peer identity") }
    return identity
  }

  private func tick() -> Duration {
    guard !stopped else { return .zero }
    // Retry bootstrap addresses; SWIM selects the established peers to probe.
    let seeds = configuration.seeds
    for offset in 0..<min(8, seeds.count) {
      let address = seeds[(seedOffset + offset) % seeds.count]
      let cachedEndpoint = membershipServices[address]?.id.endpoint
      let known = swim.members.contains {
        let sameAddress = $0.node.host == address.host && $0.node.port == address.port
        let samePeer = cachedEndpoint != nil && identities[$0.node]?.endpoint == cachedEndpoint
        return (sameAddress || samePeer) && ($0.status.isAlive || $0.status.isSuspect)
      }
      if address != identity.address, !known, bootstrapping.insert(address).inserted {
        pendingWork.yield(.bootstrap(address))
      }
    }
    if !seeds.isEmpty { seedOffset = (seedOffset + min(8, seeds.count)) % seeds.count }
    var nextDelay = Duration.milliseconds(configuration.probeIntervalMilliseconds)
    for directive in swim.onPeriodicPingTick() {
      switch directive {
      case .sendPing(let target, let payload, let timeout, let sequenceNumber):
        pendingWork.yield(.ping(target, payload, timeout, sequenceNumber))
      case .scheduleNextTick(let delay):
        nextDelay =
          delay * (Duration.milliseconds(configuration.probeIntervalMilliseconds) / swim.settings.probeInterval)
      case .membershipChanged: break
      }
    }
    return nextDelay
  }

  private func waitForTick(_ delay: Duration) async throws {
    if membershipRequested {
      membershipRequested = false
      return
    }
    let sleep = Task { try await Task.sleep(for: delay) }
    membershipSleep = sleep
    await withTaskCancellationHandler {
      _ = try? await sleep.value
    } onCancel: {
      sleep.cancel()
    }
    membershipSleep = nil
    try Task.checkCancellation()
  }

  private func perform(_ item: MembershipWork) async {
    guard !stopped, !Task.isCancelled else { return }
    switch item {
    case .bootstrap(let address):
      defer { bootstrapping.remove(address) }
      _ = try? await contact(address)
    case .ping(let target, let payload, let timeout, let sequenceNumber):
      let response = await probe(
        target, payload: payload, timeout: timeout, sequenceNumber: sequenceNumber, pingRequestOrigin: nil)
      guard !stopped, !Task.isCancelled else { return }
      for directive in swim.onPingResponse(response: response, pingRequestOrigin: nil, pingRequestSequenceNumber: nil) {
        if case .sendPingRequests(let directive) = directive { pendingWork.yield(.indirect(directive)) }
      }
    case .indirect(let directive): await probeIndirectly(directive)
    }
  }

  private func probe(
    _ target: ClusterMembership.Node, payload: SWIM.GossipPayload,
    timeout: Duration, sequenceNumber: UInt32, pingRequestOrigin: ClusterMembership.Node?
  ) async -> SWIM.PingResponse {
    do {
      let reply = try await request(
        .ping(sequenceNumber: sequenceNumber, payload: wire(payload)),
        address: PeerAddress(host: target.host, port: target.port), timeout: timeout)
      return reply.response.swimResponse(excludingStaleIncarnationsOf: identity)
    } catch {
      return .timeout(
        target: target, pingRequestOrigin: pingRequestOrigin, timeout: timeout, sequenceNumber: sequenceNumber)
    }
  }

  private func probeIndirectly(_ directive: SWIM.Instance.SendPingRequestDirective) async {
    guard let target = try? identity(for: directive.target) else { return }
    let response = await withTaskGroup(of: SWIM.PingResponse.self) { tasks in
      for detail in directive.requestDetails {
        tasks.addTask { await self.indirectRequest(detail, target: target, timeout: directive.timeout) }
      }
      var firstAck: SWIM.PingResponse?
      while let response = await tasks.next() {
        if case .ack = response, firstAck == nil {
          firstAck = response
          // Apply the first ack immediately; allow the other probes to finish for Lifeguard accounting.
          self.acceptIndirect(response, target: directive.target)
        }
      }
      return firstAck
    }
    if response == nil, !stopped, !Task.isCancelled {
      acceptIndirect(
        .timeout(
          target: directive.target, pingRequestOrigin: nil,
          timeout: directive.timeout, sequenceNumber: 0), target: directive.target)
    }
  }

  private func indirectRequest(
    _ detail: SWIM.Instance.SendPingRequestDirective.PingRequestDetail,
    target: NodeIdentity, timeout: Duration
  ) async -> SWIM.PingResponse {
    let response: SWIM.PingResponse
    do {
      let reply = try await request(
        .pingRequest(
          target: target, sequenceNumber: detail.sequenceNumber,
          payload: wire(detail.payload)),
        address: PeerAddress(
          host: detail.peerToPingRequestThrough.host,
          port: detail.peerToPingRequestThrough.port), timeout: timeout)
      response = reply.response.swimResponse(excludingStaleIncarnationsOf: identity)
    } catch {
      response = .timeout(
        target: target.swimNode, pingRequestOrigin: identity.swimNode,
        timeout: timeout, sequenceNumber: detail.sequenceNumber)
    }
    if !stopped, !Task.isCancelled { _ = swim.onEveryPingRequestResponse(response, pinged: target.swimNode) }
    return response
  }

  private func acceptIndirect(_ response: SWIM.PingResponse, target: ClusterMembership.Node) {
    guard !stopped, !Task.isCancelled else { return }
    _ = swim.onPingRequestResponse(response, pinged: target)
  }

  private func contact(_ address: PeerAddress) async throws -> PingResult {
    let start = ContinuousClock.now
    let sequenceNumber = swim.nextSequenceNumber()
    let payload = try wire(swim.makeGossipPayload(to: nil))
    let reply = try await request(
      .ping(sequenceNumber: sequenceNumber, payload: payload), address: address,
      timeout: .milliseconds(configuration.requestTimeoutMilliseconds))
    guard case .ack = reply.response else { throw IndraError("Expected SWIM acknowledgement") }
    _ = swim.onPingResponse(
      response: reply.response.swimResponse(excludingStaleIncarnationsOf: identity), pingRequestOrigin: nil,
      pingRequestSequenceNumber: nil)
    let elapsed = start.duration(to: .now).components
    return PingResult(
      node: reply.node,
      milliseconds: Double(elapsed.seconds) * 1000 + Double(elapsed.attoseconds) / 1e15)
  }

  private func request(_ probe: MembershipProbe, address: PeerAddress, timeout: Duration) async throws
    -> MembershipReply
  {
    guard !stopped else { throw IndraError("Node is stopping") }
    do {
      let message = MembershipRequest(version: 1, cluster: configuration.cluster, node: identity, probe: probe)
      let reply = try await withMembershipDeadline(timeout: timeout) {
        try await self.exchange(message, address: address)
      }
      try Task.checkCancellation()
      try validate(version: reply.version, cluster: reply.cluster, sender: reply.node, payload: reply.response.payload)
      try reply.response.target.validate()
      guard reply.response.sequenceNumber == probe.sequenceNumber else { throw IndraError("SWIM sequence mismatch") }
      switch probe {
      case .ping:
        guard case .ack = reply.response, reply.response.target == reply.node else {
          throw IndraError("Invalid direct SWIM acknowledgement")
        }
      case .pingRequest(let target, _, _):
        guard reply.response.target.id == target.id, reply.response.target.endpoint == target.endpoint else {
          throw IndraError("Invalid indirect SWIM acknowledgement")
        }
      }
      learn(sender: reply.node, payload: reply.response.payload)
      discoveryErrors[address.description] = nil
      return reply
    } catch {
      if !Task.isCancelled, !stopped {
        let message = String(describing: error)
        if discoveryErrors.count < 512 || discoveryErrors[address.description] != nil {
          discoveryErrors[address.description] = message
        }
      }
      throw error
    }
  }

  private func exchange(_ message: MembershipRequest, address: PeerAddress) async throws -> MembershipReply {
    if let remote = membershipServices[address] {
      do {
        let reply = try await remote.probe(message)
        guard reply.node.endpoint == remote.id.endpoint else { throw IndraError("Peer endpoint mismatch") }
        return reply
      } catch let error as RPCError where error == .disconnected || error == .unknownActor {
        if membershipServices[address]?.id == remote.id { membershipServices[address] = nil }
      }
    }
    try Task.checkCancellation()
    let remote = try await system.service(
      named: MembershipService.serviceName, host: address.host,
      port: address.port, as: MembershipService.self)
    let reply = try await remote.probe(message)
    guard reply.node.endpoint == remote.id.endpoint else { throw IndraError("Peer endpoint mismatch") }
    if membershipServices.count < 256 || membershipServices[address] != nil { membershipServices[address] = remote }
    return reply
  }

  private func validate(version: Int, cluster: String, sender: NodeIdentity, payload: [GossipMember]) throws {
    guard !stopped else { throw IndraError("Node is stopping") }
    guard version == 1, cluster == configuration.cluster else {
      throw IndraError("Peer uses a different cluster or protocol version")
    }
    try sender.validate()
    guard sender.id != identity.id || sender.endpoint == identity.endpoint else {
      throw IndraError("Duplicate node ID; generate a separate configuration for each node")
    }
    guard payload.count <= 64 else { throw IndraError("Too many members in SWIM gossip") }
    for member in payload {
      try member.node.validate()
      try member.status.validate()
    }
  }

  private func learn(sender: NodeIdentity, payload: [GossipMember]) {
    for member in payload where member.node.id != identity.id {
      identities[member.node.swimNode] = member.node
    }
    identities[sender.swimNode] = sender
  }

  private func gossip(_ payload: [GossipMember]) -> SWIM.GossipPayload {
    .membership(payload.filter { $0.node.id != identity.id || $0.node.endpoint == identity.endpoint }.map(\.member))
  }

  private func wire(_ payload: SWIM.GossipPayload) throws -> [GossipMember] {
    var members: [SWIM.Member]
    switch payload {
    case .none: members = []
    case .membership(let payload): members = payload
    }
    members.removeAll { $0.node == identity.swimNode }
    members.append(swim.member)
    return try members.map {
      GossipMember(node: try identity(for: $0.node), status: GossipStatus($0.status), protocolPeriod: $0.protocolPeriod)
    }
  }
}

private func withMembershipDeadline<Value: Sendable>(
  timeout: Duration, operation: @escaping @Sendable () async throws -> Value
) async throws -> Value {
  try await withThrowingTaskGroup(of: Value.self) { tasks in
    tasks.addTask { try await operation() }
    tasks.addTask {
      try await Task.sleep(for: timeout)
      throw IndraError("SWIM probe timed out")
    }
    defer { tasks.cancelAll() }
    guard let result = try await tasks.next() else { throw CancellationError() }
    return result
  }
}
