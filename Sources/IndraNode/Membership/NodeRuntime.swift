import ClusterMembership
import Foundation
import SWIM
public import TCPActorSystem

private enum MembershipWork: Sendable {
  case refreshServices
  case ping(ClusterMembership.Node, SWIM.GossipPayload, Duration, UInt32)
  case indirect(SWIM.Instance.SendPingRequestDirective)
}

private struct BootstrapRetry: Sendable {
  let delay: Duration
  let nextAttempt: ContinuousClock.Instant
}

private struct Bootstrap: Sendable {
  var task: Task<Void, Never>?
  var retry: BootstrapRetry?
  var retryWhileConnected: Bool
  var waiters: [UUID: CheckedContinuation<PingResult, any Error>]
}

public actor NodeRuntime {
  public let system: TCPActorSystem
  public nonisolated let receptionist: Receptionist
  private let configurationPath: String
  private let peerHistoryPath: String?
  private var peerHistory: PeerHistory
  private var peerHistoryNeedsSave = false
  private var peerHistorySaveAttempt: ContinuousClock.Instant?
  private var peerHistorySave: Task<Void, Never>?
  private var configuration: NodeConfiguration
  private var configurationUpdate: Task<Void, any Error>?
  private var swim: SWIM.Instance
  private var peers = PeerDirectory()
  private var configurationError: String?
  private var discoveryErrors: [String: String] = [:]
  private let work: AsyncStream<MembershipWork>
  private let pendingWork: AsyncStream<MembershipWork>.Continuation
  private var bootstraps: [PeerAddress: Bootstrap] = [:]
  private var bootstrapOffset = 0
  private var stopped = false
  private var shutdownTask: Task<Void, Never>?
  private var serviceShutdown: [@Sendable () async -> Void] = []
  private var serviceMembershipChanged = false
  private var serviceMembershipRevision: UInt64 = 0
  private var refreshingServices = false
  private var serviceRefreshRequested = false
  private var telemetry = MembershipTelemetry(capacity: 128)

  public init(
    configuration: NodeConfiguration, configurationPath: String, peerHistoryPath: String?, system: TCPActorSystem
  ) {
    self.configuration = configuration
    self.configurationPath = configurationPath
    self.peerHistoryPath = peerHistoryPath
    self.peerHistory = PeerHistory(nodeID: configuration.id, cluster: configuration.cluster)
    self.system = system
    var settings = SWIM.Settings()
    settings.probeInterval = .milliseconds(configuration.probeIntervalMilliseconds)
    settings.pingTimeout = .milliseconds(configuration.requestTimeoutMilliseconds)
    // Membership is informational: unreachable peers can recover after a partition.
    settings.unreachability = .enabled
    let node = NodeIdentity(id: configuration.id, name: configuration.name, endpoint: system.endpoint, build: .current)
    self.receptionist = Receptionist(system: system, cluster: configuration.cluster, node: node)
    self.swim = SWIM.Instance(settings: settings, myself: node.swimNode)
    (work, pendingWork) = AsyncStream.makeStream(bufferingPolicy: .unbounded)
  }

  public func start(services: [ServiceInstaller]) async throws {
    if let peerHistoryPath {
      do {
        peerHistory = try await PeerHistory.load(
          path: peerHistoryPath, nodeID: configuration.id, cluster: configuration.cluster)
      } catch {
        discoveryErrors["peer-history"] = String(describing: error)
      }
    }
    try await installBuiltInServices(in: receptionist)
    let context = ServiceContext(
      actorSystem: system, receptionist: receptionist, cluster: configuration.cluster,
      configurationPath: configurationPath, started: telemetry.started,
      runtime: self)
    for install in services {
      try Task.checkCancellation()
      try await install(context)
    }
  }

  private func installBuiltInServices(in registry: isolated Receptionist) throws {
    try registry.requireRegistrationCapacity(for: 2)
    try system.reserveFixedActorIDs([ReceptionistService.fixedID, MembershipService.fixedID])
    let discovery = ReceptionistService(actorSystem: system, receptionist: registry)
    let membership = MembershipService(actorSystem: system, runtime: self)
    try registry.register(discovery, with: ServiceKey<ReceptionistService>(ReceptionistService.serviceName))
    try registry.register(membership, with: ServiceKey<MembershipService>(MembershipService.serviceName))
  }

  func addServiceShutdown(_ action: @escaping @Sendable () async -> Void) throws {
    guard !stopped else { throw IndraError("Node is stopping") }
    serviceShutdown.append(action)
  }

  static func transportTimeoutMilliseconds(configuration: NodeConfiguration) -> Int64 {
    Int64(configuration.requestTimeoutMilliseconds) * Int64(1 + SWIM.Settings().lifeguard.maxLocalHealthMultiplier)
  }

  public func status() -> NodeStatus {
    var members: [Member] = []
    for member in swim.members where member.node != swim.swimNode {
      guard let node = peers[member.node], peers.accepts(node) else { continue }
      members.append(Member(node: node, state: Member.State(member.status)))
    }
    return NodeStatus(
      node: identity, cluster: configuration.cluster,
      members: members.sorted {
        if $0.node.id == $1.node.id {
          return $0.node.endpoint.systemID.uuidString < $1.node.endpoint.systemID.uuidString
        }
        return $0.node.id.uuidString < $1.node.id.uuidString
      },
      seeds: configuration.seeds, configurationError: configurationError, discoveryErrors: discoveryErrors)
  }

  public func monitor() -> MonitorSnapshot {
    MonitorSnapshot(
      status: status(), uptimeSeconds: telemetry.uptimeSeconds, protocolPeriod: swim.protocolPeriod,
      localHealthMultiplier: swim.localHealthMultiplier,
      probeIntervalMilliseconds: configuration.probeIntervalMilliseconds,
      requestTimeoutMilliseconds: configuration.requestTimeoutMilliseconds,
      outbound: telemetry.outbound, inbound: telemetry.inbound, inFlight: telemetry.inFlight,
      events: Array(telemetry.events), changes: Array(telemetry.changes))
  }

  public func reload() async throws {
    try await reloadConfiguration(addingSeed: nil)
    await updateServices()
  }

  private func reloadConfiguration(addingSeed seed: PeerAddress?) async throws {
    try Task.checkCancellation()
    guard !stopped else { throw IndraError("Node is stopping") }
    let previous = configurationUpdate
    // Serialize reading, updating, and publishing the configuration across suspension points.
    let update = Task {
      _ = await previous?.result
      try Task.checkCancellation()
      try await applyConfiguration(addingSeed: seed)
    }
    configurationUpdate = update
    try await withTaskCancellationHandler {
      try await update.value
    } onCancel: {
      update.cancel()
    }
  }

  private func applyConfiguration(addingSeed seed: PeerAddress?) async throws {
    guard !stopped else { throw IndraError("Node is stopping") }
    var updated: NodeConfiguration
    do {
      updated = try await NodeConfiguration.load(path: configurationPath)
      guard configuration.canReload(updated) else {
        throw IndraError(
          "Identity, cluster, listen/advertised address, port, and SWIM timing changes require a restart")
      }
    } catch is CancellationError {
      throw CancellationError()
    } catch {
      configurationError = String(describing: error)
      throw error
    }
    guard !stopped else { throw IndraError("Node is stopping") }
    configurationError = nil
    if let seed, !updated.seeds.contains(seed) {
      updated.seeds.append(seed)
      try await updated.save(path: configurationPath, overwrite: true)
    }
    guard updated != configuration else { return }
    let addedSeeds = updated.seeds.filter { !configuration.seeds.contains($0) }
    if updated.name != configuration.name { serviceMembershipChanged = true }
    configuration = updated
    guard !stopped else { return }
    for address in addedSeeds where address != identity.address && address != seed {
      startBootstrap(address, waiting: nil)
    }
  }

  public func join(_ address: PeerAddress) async throws -> PingResult {
    try address.validate()
    try await reloadConfiguration(addingSeed: address)
    let waiter = UUID()
    return try await withTaskCancellationHandler {
      try await withCheckedThrowingContinuation { continuation in
        do {
          try Task.checkCancellation()
          guard !stopped else { throw IndraError("Node is stopping") }
          startBootstrap(address, waiting: (waiter, continuation))
        } catch {
          continuation.resume(throwing: error)
        }
      }
    } onCancel: {
      Task { await self.cancelJoin(waiter, address: address) }
    }
  }

  public func ping(_ target: String) async throws -> PingResult {
    if let match = try peers.resolve(target, local: identity, isMember: { swim.status(of: $0.swimNode) != nil }) {
      if match.id == identity.id { return PingResult(node: identity, milliseconds: 0) }
      let result = try await contact(match.address, retryOnDisconnect: true)
      guard result.node.id == match.id else { throw IndraError("Address now belongs to a different node") }
      return result
    }
    return try await contact(PeerAddress(target), retryOnDisconnect: true)
  }

  public func runMembership() async throws {
    let result: Result<Void, any Error>
    do {
      try await withThrowingDiscardingTaskGroup { tasks in
        tasks.addTask {
          while !Task.isCancelled {
            let delay = await self.tick()
            try await Task.sleep(for: delay)
          }
        }
        for await item in work {
          try Task.checkCancellation()
          tasks.addTask { await self.perform(item) }
        }
        tasks.cancelAll()
      }
      result = .success(())
    } catch {
      result = .failure(error)
    }
    await cancelBootstraps(keepingJoins: true)
    try result.get()
  }

  public func shutdown() async {
    if let shutdownTask {
      await shutdownTask.value
      return
    }
    stopped = true
    let task = Task { await stopResources() }
    shutdownTask = task
    await task.value
  }

  private func stopResources() async {
    pendingWork.finish()
    _ = await configurationUpdate?.result
    await cancelBootstraps(keepingJoins: false)
    bootstraps.removeAll()
    await savePeerHistory(force: true)
    for action in serviceShutdown.reversed() { await action() }
    serviceShutdown.removeAll()
    await receptionist.shutdown()
    await system.shutdown()
  }

  func receive(_ message: MembershipRequest) async throws -> MembershipReply {
    try validate(
      version: message.version, cluster: message.cluster, sender: message.node, payload: message.probe.payload)
    guard isCurrent(message.node) else {
      verifyCandidate(message.node)
      throw IndraError("Verifying replacement peer process")
    }
    if case .pingRequest(let target, _, _) = message.probe {
      try target.validate()
      guard isCurrent(target) else {
        throw IndraError("Indirect probe targets a previous process")
      }
    }
    remember(message.node)
    learn(message.probe.payload, from: message.node)
    record(message.probe, direction: .inbound, peer: message.node.address)
    let response: MembershipResponse
    switch message.probe {
    case .ping(let sequenceNumber, let payload):
      let directives = swim.onPing(
        pingOrigin: message.node.swimNode,
        payload: gossip(payload), sequenceNumber: sequenceNumber)
      for directive in directives {
        if case .gossipProcessed(let gossip) = directive { record(gossip) }
      }
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
      remember(target)
      let targetNode = target.swimNode
      let previousStatus = swim.status(of: targetNode)
      let directives = swim.onPingRequest(
        target: targetNode, pingRequestOrigin: message.node.swimNode,
        payload: gossip(payload), sequenceNumber: sequenceNumber)
      // SWIM can add the target without reporting it in a directive.
      noteReachabilityChange(targetNode, previous: previousStatus, current: swim.status(of: targetNode))
      var forwarded: MembershipResponse?
      for directive in directives {
        if case .gossipProcessed(let gossip) = directive { record(gossip) }
        if case .sendPing(let node, let payload, let origin, let originSequence, let timeout, let number) = directive {
          let result = await probe(
            node, payload: payload, timeout: timeout, sequenceNumber: number, pingRequestOrigin: origin)
          guard !stopped, !Task.isCancelled else { throw CancellationError() }
          for reply in onPingResponse(
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
            case .gossipProcessed(let gossip): record(gossip)
            case .sendPingRequests:
              throw IndraError("Unexpected recursive indirect SWIM probe")
            }
          }
        }
      }
      response = forwarded ?? .nack(target: target, sequenceNumber: sequenceNumber)
    }
    await updateServices()
    record(response, direction: .outbound, peer: message.node.address, roundTripMilliseconds: nil)
    return MembershipReply(version: 1, cluster: configuration.cluster, node: identity, response: response)
  }

  private var identity: NodeIdentity {
    NodeIdentity(id: configuration.id, name: configuration.name, endpoint: system.endpoint, build: .current)
  }

  private func identity(for node: ClusterMembership.Node) throws -> NodeIdentity {
    if node == swim.swimNode { return identity }
    guard let identity = peers[node] else { throw IndraError("Missing SWIM peer identity") }
    return identity
  }

  private func updateServices() async {
    await savePeerHistory(force: false)
    guard serviceMembershipChanged, !stopped else { return }
    serviceMembershipChanged = false
    serviceMembershipRevision += 1
    var reachable: [ActorEndpoint: NodeIdentity] = [:]
    for member in swim.members where member.node != swim.swimNode && isReachable(member.status) {
      if let peer = peers[member.node], peers.accepts(peer) { reachable[peer.endpoint] = peer }
    }
    if await receptionist.update(node: identity, reachable: reachable, membershipRevision: serviceMembershipRevision) {
      requestServiceRefresh()
    }
  }

  private func rememberAlive(_ node: NodeIdentity) {
    guard !stopped, node.id != configuration.id else { return }
    peerHistory.record(node, at: .now)
    peerHistoryNeedsSave = true
  }

  private func savePeerHistory(force: Bool) async {
    if force { await peerHistorySave?.value }
    guard force || !stopped else { return }
    guard peerHistorySave == nil, peerHistoryNeedsSave, let peerHistoryPath else { return }
    let now = ContinuousClock.now
    if !force, let peerHistorySaveAttempt, peerHistorySaveAttempt.duration(to: now) < .seconds(5) { return }
    peerHistorySaveAttempt = now
    let snapshot = peerHistory
    peerHistoryNeedsSave = false
    let save = Task {
      defer { peerHistorySave = nil }
      do {
        try await snapshot.save(path: peerHistoryPath)
        discoveryErrors["peer-history"] = nil
      } catch {
        peerHistoryNeedsSave = true
        discoveryErrors["peer-history"] = String(describing: error)
      }
    }
    peerHistorySave = save
    await save.value
  }

  private func requestServiceRefresh() {
    guard !stopped else { return }
    serviceRefreshRequested = true
    if !refreshingServices {
      refreshingServices = true
      pendingWork.yield(.refreshServices)
    }
  }

  private func startBootstrap(
    _ address: PeerAddress, waiting: (id: UUID, continuation: CheckedContinuation<PingResult, any Error>)?
  ) {
    var bootstrap =
      bootstraps[address]
      ?? Bootstrap(task: nil, retry: nil, retryWhileConnected: false, waiters: [:])
    if let waiting {
      bootstrap.retryWhileConnected = true
      bootstrap.waiters[waiting.id] = waiting.continuation
    }
    if bootstrap.task == nil {
      bootstrap.task = Task { await self.performBootstrap(address) }
    }
    bootstraps[address] = bootstrap
  }

  private func performBootstrap(_ address: PeerAddress) async {
    let result: Result<PingResult, any Error>
    do {
      try Task.checkCancellation()
      let reply = try await contact(address, retryOnDisconnect: false)
      try Task.checkCancellation()
      result = .success(reply)
    } catch {
      await updateServices()
      result = .failure(error)
    }
    guard var bootstrap = bootstraps.removeValue(forKey: address) else { return }
    let waiters = bootstrap.waiters
    if case .failure = result, !stopped, !Task.isCancelled {
      let previous = bootstrap.retry?.delay ?? .milliseconds(configuration.probeIntervalMilliseconds)
      let delay = min(previous * 2, .seconds(60))
      bootstrap.retry = BootstrapRetry(delay: delay, nextAttempt: .now.advanced(by: delay))
      bootstrap.task = nil
      bootstrap.waiters.removeAll()
      bootstraps[address] = bootstrap
    }
    for waiter in waiters.values { waiter.resume(with: result) }
  }

  private func cancelJoin(_ waiter: UUID, address: PeerAddress) {
    bootstraps[address]?.waiters.removeValue(forKey: waiter)?.resume(throwing: CancellationError())
  }

  private func cancelBootstraps(keepingJoins: Bool) async {
    let tasks = bootstraps.values.compactMap { bootstrap in
      keepingJoins && !bootstrap.waiters.isEmpty ? nil : bootstrap.task
    }
    for task in tasks { task.cancel() }
    for task in tasks { await task.value }
  }

  private func tick() async -> Duration {
    guard !stopped else { return .zero }
    // Retry remembered peers and pending joins even while connected.
    let connected = swim.members.contains {
      $0.node != swim.swimNode && isReachable($0.status)
    }
    var addresses = peerHistory.addresses
    let remembered = Set(addresses)
    let configuredAddresses = remembered.union(configuration.seeds)
    bootstraps = bootstraps.filter { address, bootstrap in
      bootstrap.task != nil || (bootstrap.retry != nil && configuredAddresses.contains(address))
    }
    addresses += configuration.seeds.filter {
      !remembered.contains($0) && (!connected || bootstraps[$0]?.retryWhileConnected == true)
    }
    for offset in 0..<min(8, addresses.count) {
      let address = addresses[(bootstrapOffset + offset) % addresses.count]
      let known = swim.members.contains {
        let sameAddress = $0.node.host == address.host && $0.node.port == address.port
        return sameAddress && isReachable($0.status)
      }
      let bootstrap = bootstraps[address]
      let ready = bootstrap?.retry.map { $0.nextAttempt <= .now } ?? true
      if address != identity.address, !known, ready, bootstrap?.task == nil {
        startBootstrap(address, waiting: nil)
      }
    }
    if !addresses.isEmpty { bootstrapOffset = (bootstrapOffset + min(8, addresses.count)) % addresses.count }
    var nextDelay = Duration.milliseconds(configuration.probeIntervalMilliseconds)
    for directive in swim.onPeriodicPingTick() {
      switch directive {
      case .sendPing(let target, let payload, let timeout, let sequenceNumber):
        pendingWork.yield(.ping(target, payload, timeout, sequenceNumber))
      case .scheduleNextTick(let delay): nextDelay = delay
      case .membershipChanged(let change): record(change)
      }
    }
    await updateServices()
    requestServiceRefresh()
    return nextDelay
  }

  private func perform(_ item: MembershipWork) async {
    guard !stopped, !Task.isCancelled else { return }
    switch item {
    case .refreshServices:
      repeat {
        serviceRefreshRequested = false
        await receptionist.refresh()
      } while serviceRefreshRequested && !stopped && !Task.isCancelled
      refreshingServices = false
    case .ping(let target, let payload, let timeout, let sequenceNumber):
      let response = await probe(
        target, payload: payload, timeout: timeout, sequenceNumber: sequenceNumber, pingRequestOrigin: nil)
      guard !stopped, !Task.isCancelled else { return }
      // SWIM intentionally omits a directive for direct timeout -> suspect transitions.
      let previous = swim.status(of: target).map(Member.State.init)
      let directives = onPingResponse(response: response, pingRequestOrigin: nil, pingRequestSequenceNumber: nil)
      for directive in directives {
        if case .gossipProcessed(let gossip) = directive { record(gossip) }
        if case .sendPingRequests(let directive) = directive { pendingWork.yield(.indirect(directive)) }
      }
      if case .timeout = response, let current = swim.status(of: target), let node = peers[target] {
        telemetry.recordChange(node: node, previous: previous, current: Member.State(current))
      }
    case .indirect(let directive): await probeIndirectly(directive)
    }
    await updateServices()
  }

  private func probe(
    _ target: ClusterMembership.Node, payload: SWIM.GossipPayload,
    timeout: Duration, sequenceNumber: UInt32, pingRequestOrigin: ClusterMembership.Node?
  ) async -> SWIM.PingResponse {
    do {
      let reply = try await request(
        .ping(sequenceNumber: sequenceNumber, payload: wire(payload)),
        address: PeerAddress(host: target.host, port: target.port), deadline: .now.advanced(by: timeout),
        reuseConnection: true)
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
          await self.acceptIndirect(response, target: directive.target)
        }
      }
      return firstAck
    }
    if response == nil, !stopped, !Task.isCancelled {
      await acceptIndirect(
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
          port: detail.peerToPingRequestThrough.port), deadline: .now.advanced(by: timeout), reuseConnection: true)
      response = reply.response.swimResponse(excludingStaleIncarnationsOf: identity)
    } catch {
      response = .timeout(
        target: target.swimNode, pingRequestOrigin: identity.swimNode,
        timeout: timeout, sequenceNumber: detail.sequenceNumber)
    }
    if !stopped, !Task.isCancelled {
      _ = swim.onEveryPingRequestResponse(response, pinged: target.swimNode)
      await updateServices()
    }
    return response
  }

  private func acceptIndirect(_ response: SWIM.PingResponse, target: ClusterMembership.Node) async {
    guard !stopped, !Task.isCancelled, isCurrent(target), let response = currentResponse(response) else { return }
    let previous = swim.status(of: target)
    for directive in swim.onPingRequestResponse(response, pinged: target) {
      if case .gossipProcessed(.applied(let change)) = directive, let change, change.member.node != target {
        record(change)
      }
    }
    // The target can appear in both gossip and response directives; record its resulting change once.
    let current = swim.status(of: target)
    noteReachabilityChange(target, previous: previous, current: current)
    if let current, let node = peers[target] {
      telemetry.recordChange(node: node, previous: previous.map(Member.State.init), current: Member.State(current))
    }
    await updateServices()
  }

  private func contact(_ address: PeerAddress, retryOnDisconnect: Bool) async throws -> PingResult {
    let start = ContinuousClock.now
    let deadline = start.advanced(by: .milliseconds(configuration.requestTimeoutMilliseconds))
    let reply: MembershipReply
    do {
      reply = try await requestPing(address, deadline: deadline, reuseConnection: true)
    } catch RPCError.disconnected where retryOnDisconnect {
      reply = try await requestPing(address, deadline: deadline, reuseConnection: false)
    }
    guard case .ack = reply.response else { throw IndraError("Expected SWIM acknowledgement") }
    for directive in onPingResponse(
      response: reply.response.swimResponse(excludingStaleIncarnationsOf: identity), pingRequestOrigin: nil,
      pingRequestSequenceNumber: nil)
    {
      if case .gossipProcessed(let gossip) = directive { record(gossip) }
    }
    await updateServices()
    let elapsed = start.duration(to: .now).components
    return PingResult(
      node: reply.node,
      milliseconds: Double(elapsed.seconds) * 1000 + Double(elapsed.attoseconds) / 1e15)
  }

  private func requestPing(_ address: PeerAddress, deadline: ContinuousClock.Instant, reuseConnection: Bool)
    async throws -> MembershipReply
  {
    let sequenceNumber = swim.nextSequenceNumber()
    let payload = try wire(swim.makeGossipPayload(to: nil))
    return try await request(
      .ping(sequenceNumber: sequenceNumber, payload: payload), address: address, deadline: deadline,
      reuseConnection: reuseConnection)
  }

  private func request(
    _ probe: MembershipProbe, address: PeerAddress, deadline: ContinuousClock.Instant, reuseConnection: Bool
  ) async throws -> MembershipReply {
    guard !stopped else { throw IndraError("Node is stopping") }
    try Task.checkCancellation()
    let started = ContinuousClock.now
    let timeoutError = IndraError("SWIM probe timed out")
    guard started < deadline else { throw timeoutError }
    record(probe, direction: .outbound, peer: address)
    telemetry.inFlight += 1
    defer { telemetry.inFlight -= 1 }
    let observed = peers.verification
    var peerAddress = address
    do {
      let message = MembershipRequest(version: 1, cluster: configuration.cluster, node: identity, probe: probe)
      let reply = try await withDeadline(deadline: deadline, timeoutError: timeoutError) {
        try await self.exchange(message, address: address, reuseConnection: reuseConnection)
      }
      peerAddress = reply.node.address
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
      let incumbent = peers.incumbent(for: reply.node)
      if let incumbent, incumbent.address != peerAddress {
        try await checkIncumbent(incumbent, candidate: reply.node, deadline: deadline)
        try Task.checkCancellation()
        guard !stopped else { throw IndraError("Node is stopping") }
      }
      let update = try peers.verify(reply.node, observed: observed, checkedIncumbent: incumbent?.endpoint)
      if update.identityChanged { noteIdentityChange(reply.node.swimNode) }
      for previous in update.retired {
        if case .applied(let change) = swim.confirmDead(node: previous.swimNode) { record(change) }
      }
      learn(reply.response.payload, from: reply.node)
      if case .ack = reply.response, isCurrent(reply.response.target) { rememberAlive(reply.response.target) }
      bootstraps[address]?.retry = nil
      discoveryErrors[address.description] = nil
      record(
        reply.response, direction: .inbound, peer: address,
        roundTripMilliseconds: started.duration(to: .now).secondsValue * 1000)
      return reply
    } catch {
      if !Task.isCancelled, !stopped, peers.verification.unchanged(at: peerAddress, since: observed) {
        let message = String(describing: error)
        telemetry.record(
          direction: .outbound, kind: .failure, peer: address, target: nil,
          sequenceNumber: probe.sequenceNumber, gossipMembers: 0,
          roundTripMilliseconds: started.duration(to: .now).secondsValue * 1000, error: message)
        if discoveryErrors.count < 512 || discoveryErrors[address.description] != nil {
          discoveryErrors[address.description] = message
        }
      }
      throw error
    }
  }

  private func checkIncumbent(
    _ incumbent: NodeIdentity, candidate: NodeIdentity, deadline: ContinuousClock.Instant
  ) async throws {
    let listening: Bool
    do {
      listening = try await withDeadline(deadline: deadline, timeoutError: RPCError.timedOut) { [system] in
        try await system.isListening(incumbent.endpoint)
      }
    } catch let error as RPCError where error == .disconnected || error == .timedOut {
      // An inconclusive handshake alone cannot displace a reachable peer.
      guard swim.status(of: incumbent.swimNode)?.isUnreachable == true else { throw error }
      return
    }
    guard !listening else { throw IndraError("Two live processes are using node UUID \(candidate.id)") }
  }

  private func verifyCandidate(_ peer: NodeIdentity) {
    guard peer.id != identity.id, !peers.retired.contains(peer.endpoint),
      bootstraps[peer.address]?.task == nil,
      bootstraps[peer.address]?.retry.map({ $0.nextAttempt <= .now }) ?? true
    else { return }
    startBootstrap(peer.address, waiting: nil)
  }

  private func record(_ probe: MembershipProbe, direction: ProbeEvent.Direction, peer: PeerAddress) {
    let kind: ProbeEvent.Kind
    let target: PeerAddress?
    switch probe {
    case .ping:
      kind = .ping
      target = nil
    case .pingRequest(let node, _, _):
      kind = .pingRequest
      target = node.address
    }
    telemetry.record(
      direction: direction, kind: kind, peer: peer, target: target,
      sequenceNumber: probe.sequenceNumber, gossipMembers: probe.payload.count,
      roundTripMilliseconds: nil, error: nil)
  }

  private func record(
    _ response: MembershipResponse, direction: ProbeEvent.Direction, peer: PeerAddress,
    roundTripMilliseconds: Double?
  ) {
    let kind: ProbeEvent.Kind
    switch response {
    case .ack: kind = .ack
    case .nack: kind = .nack
    }
    telemetry.record(
      direction: direction, kind: kind, peer: peer, target: response.target.address,
      sequenceNumber: response.sequenceNumber, gossipMembers: response.payload.count,
      roundTripMilliseconds: roundTripMilliseconds, error: nil)
  }

  private func record(_ directive: SWIM.Instance.GossipProcessedDirective) {
    if case .applied(let change) = directive, let change { record(change) }
  }

  private func record(_ change: SWIM.MemberStatusChangedEvent) {
    guard let node = try? identity(for: change.member.node) else { return }
    noteReachabilityChange(change.member.node, previous: change.previousStatus, current: change.status)
    if case .alive = change.status { rememberAlive(node) }
    telemetry.recordChange(
      node: node, previous: change.previousStatus.map(Member.State.init),
      current: Member.State(change.status))
  }

  private func exchange(_ message: MembershipRequest, address: PeerAddress, reuseConnection: Bool) async throws
    -> MembershipReply
  {
    let endpoint = try await system.peerEndpoint(
      host: address.host, port: address.port, reuseConnection: reuseConnection)
    try Task.checkCancellation()
    let remote = try MembershipService.resolve(
      id: ActorAddress(endpoint: endpoint, id: MembershipService.fixedID), using: system)
    let reply = try await remote.probe(message)
    guard reply.node.endpoint == endpoint else { throw IndraError("Peer endpoint mismatch") }
    return reply
  }

  private func validate(version: Int, cluster: String, sender: NodeIdentity, payload: [GossipMember]) throws {
    guard !stopped else { throw IndraError("Node is stopping") }
    guard version == 1, cluster == configuration.cluster else {
      throw IndraError("Peer uses a different cluster or protocol version")
    }
    try sender.validate()
    guard !peers.retired.contains(sender.endpoint) else { throw IndraError("Peer process has been retired") }
    guard sender.id != identity.id || sender.endpoint == identity.endpoint else {
      throw IndraError("Duplicate node ID; generate a separate configuration for each node")
    }
    guard payload.count <= 64 else { throw IndraError("Too many members in SWIM gossip") }
    for member in payload {
      try member.node.validate()
      try member.status.validate()
    }
  }

  private func learn(_ payload: [GossipMember], from sender: NodeIdentity) {
    for member in payload where member.node.id != identity.id && member.node.id != sender.id {
      if isCurrent(member.node) {
        remember(member.node)
      } else if case .alive = member.status, let incumbent = peers.incumbent(for: member.node),
        incumbent.address != member.node.address
      {
        verifyCandidate(member.node)
      }
    }
  }

  private func remember(_ node: NodeIdentity) {
    if peers.learn(node) { noteIdentityChange(node.swimNode) }
  }

  private func noteIdentityChange(_ node: ClusterMembership.Node) {
    if node != swim.swimNode, isReachable(swim.status(of: node)) { serviceMembershipChanged = true }
  }

  private func noteReachabilityChange(
    _ node: ClusterMembership.Node, previous: SWIM.Status?, current: SWIM.Status?
  ) {
    if node != swim.swimNode, isReachable(previous) != isReachable(current) { serviceMembershipChanged = true }
  }

  private func isReachable(_ status: SWIM.Status?) -> Bool {
    guard let status else { return false }
    return status.isAlive || status.isSuspect
  }

  private func gossip(_ payload: [GossipMember]) -> SWIM.GossipPayload {
    .membership(payload.filter { isCurrent($0.node) }.map(\.member))
  }

  private func isCurrent(_ node: NodeIdentity) -> Bool {
    if node.id == identity.id { return node.endpoint == identity.endpoint }
    return peers.accepts(node)
  }

  private func isCurrent(_ node: ClusterMembership.Node) -> Bool {
    node == identity.swimNode || peers[node].map { isCurrent($0) } == true
  }

  private func currentResponse(_ response: SWIM.PingResponse) -> SWIM.PingResponse? {
    guard case .ack(let target, let incarnation, var payload, let sequenceNumber) = response else { return response }
    guard isCurrent(target) else { return nil }
    if case .membership(let members) = payload {
      payload = .membership(members.filter { isCurrent($0.node) })
    }
    return .ack(target: target, incarnation: incarnation, payload: payload, sequenceNumber: sequenceNumber)
  }

  private func onPingResponse(
    response: SWIM.PingResponse, pingRequestOrigin: ClusterMembership.Node?, pingRequestSequenceNumber: UInt32?
  ) -> [SWIM.Instance.PingResponseDirective] {
    guard let response = currentResponse(response) else { return [] }
    let target: ClusterMembership.Node
    switch response {
    case .ack(let node, _, _, _), .nack(let node, _), .timeout(let node, _, _, _): target = node
    }
    let previous = swim.status(of: target)
    let directives = swim.onPingResponse(
      response: response, pingRequestOrigin: pingRequestOrigin, pingRequestSequenceNumber: pingRequestSequenceNumber)
    // An acknowledgement can change reachability without listing the target in gossip.
    noteReachabilityChange(target, previous: previous, current: swim.status(of: target))
    return directives
  }

  private func wire(_ payload: SWIM.GossipPayload) throws -> [GossipMember] {
    var members: [SWIM.Member]
    switch payload {
    case .none: members = []
    case .membership(let payload): members = payload
    }
    members.removeAll { $0.node == identity.swimNode }
    members.append(swim.member)
    return try members.filter { isCurrent($0.node) }.map {
      GossipMember(node: try identity(for: $0.node), status: GossipStatus($0.status), protocolPeriod: $0.protocolPeriod)
    }
  }
}
