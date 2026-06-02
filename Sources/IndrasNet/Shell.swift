import Logging
import NIOCore

private let log = Logger(label: "indras-net.shell")

public actor Shell {
  var instance: Instance
  let peerId: String
  let transport: IndrasNetTCPTransport

  private typealias Job = @Sendable () async -> Void
  private var supervisor: Task<Void, Never>?
  private var cancelableJobs: AsyncStream<Job>.Continuation?

  public init(_ node: ClusterEndpoint, transport: IndrasNetTCPTransport) {
    self.peerId = node.addressKey
    self.transport = transport
    self.instance = Instance(node.addressKey)
  }

  public func start(with peers: [ClusterEndpoint]) async throws {
    guard self.supervisor == nil else { return }
    instance.peers.formUnion(peers.map(\.addressKey))

    let (stream, continuation) = AsyncStream.makeStream(of: Job.self)
    self.cancelableJobs = continuation
    self.supervisor = Task {
      await withDiscardingTaskGroup { group in
        for await job in stream {
          group.addTask { await job() }
        }
      }
    }

    try await transport.start { message, from in
      await self.receiveMessage(message: message, from: from)
    }
    self.enqueue { await self.runHelloTimer() }
  }

  public func stop() async {
    self.cancelableJobs?.finish()
    self.cancelableJobs = nil
    self.supervisor?.cancel()
    _ = await self.supervisor?.value
    self.supervisor = nil
  }

  func receiveMessage(message: Message, from peer: PeerID) {
    switch message.type {
    case .hello: onHello(from: peer)
    case .ping:
      log.info("[\(self.peerId)] ping <- \(peer)")
      onPing(from: peer)
    case .pong:
      log.info("[\(self.peerId)] pong <- \(peer)")
      onPong(from: peer)
    default:
      log.info("Shell: default[\(message)], from: \(peer)")
    }
  }

  func onPing(from peer: PeerID) {
    for action in instance.ping(peer, ContinuousClock.now) {
      switch action {
      case .callPong: sendPong(to: peer)
      }
    }
  }

  func onPong(from peer: PeerID) {
    for action in instance.pong(peer, ContinuousClock.now) {
      switch action {
      case .callPing: sendPing(to: peer)
      }
    }
  }

  func onHello(from peer: PeerID) {
    for action in instance.hello(peer, ContinuousClock.now) {
      switch action {
      case .callPing: sendPing(to: peer)
      }
    }
  }

  private func enqueue(_ job: @escaping Job) {
    self.cancelableJobs?.yield(job)
  }

  private func runHelloTimer() async {
    while !Task.isCancelled {
      var nextWake: ContinuousClock.Instant?
      for action in instance.update(ContinuousClock.now) {
        switch action {
        case .next(let time):
          nextWake = time
        case .hellosToSend(let hellos):
          for case .callPing(let peer) in hellos {
            self.enqueue { await self.sendHello(to: peer) }
          }
        }
      }
      guard let nextWake else { return }
      let sleepDuration = ContinuousClock.now.duration(to: nextWake)
      if sleepDuration > .zero {
        do {
          try await Task.sleep(for: sleepDuration)
        } catch {
          return
        }
      }
    }
  }

  private func sendPing(to peer: PeerID) {
    self.enqueue { await self.deliverPing(to: peer) }
  }

  private func sendPong(to peer: PeerID) {
    self.enqueue { await self.deliverPong(to: peer) }
  }

  private func sendHello(to peer: PeerID) async {
    log.info("\(self.peerId) hello to: \(peer)")
    // TODO: Send message
  }

  private func deliverPing(to peer: PeerID) async {
    do {
      try await Task.sleep(for: getJitter())
    } catch {
      return  // cancelled during shutdown
    }
    log.info("[\(self.peerId)] ping -> \(peer)")
  }

  private func deliverPong(to peer: PeerID) async {
    do {
      try await Task.sleep(for: getJitter())
    } catch {
      return  // cancelled during shutdown
    }
    // TODO: Send Message
    log.info("[\(self.peerId)] pong -> \(peer)")
  }

  private func getJitter() -> Duration {
    Duration(.milliseconds(Int64.random(in: 1..<500)))
  }
}
