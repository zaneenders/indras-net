import Foundation
import Logging
import NIOCore

public enum ShellError: Error, LocalizedError {
  case noListenPort

  public var errorDescription: String? {
    switch self {
    case .noListenPort: "no port bound"
    }
  }
}

public actor Shell {
  var instance: Instance
  let peerId: PeerID
  let transport: TCPTransport
  private let logger: Logger
  private var endpoints: [PeerID: NodeAddress] = [:]

  private typealias Job = @Sendable () async -> Void
  private var supervisor: Task<Void, Never>?
  private var cancelableJobs: AsyncStream<Job>.Continuation?

  public init(_ node: NodeAddress, transport: TCPTransport, logger: Logger? = nil) {
    self.peerId = node.addressKey
    self.transport = transport
    self.instance = Instance(node.addressKey)
    self.logger = logger ?? Logger(label: "indras-net.shell")
  }

  public init(_ node: NodeAddress, logger: Logger? = nil) {
    self.init(node, transport: TCPTransport(configuration: node.tcpConfiguration()), logger: logger)
  }

  public func start(with peers: [NodeAddress]) async throws -> Int {
    if self.supervisor != nil {
      guard let port = await transport.listenPort() else { throw ShellError.noListenPort }
      return port
    }

    self.endpoints = Dictionary(uniqueKeysWithValues: peers.map { ($0.addressKey, $0) })
    self.instance.members = Set(self.endpoints.keys)

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
    self.enqueue { await self.runMaintenanceTimer() }

    guard let port = await transport.listenPort() else {
      await stop()
      try await transport.shutdown()
      throw ShellError.noListenPort
    }
    return port
  }

  public func shutdown() async throws {
    await stop()
    try await transport.shutdown()
  }

  func connectedPeers() async -> Set<PeerID> {
    await transport.connectedPeers()
  }

  public func stop() async {
    self.cancelableJobs?.finish()
    self.cancelableJobs = nil
    self.supervisor?.cancel()
    _ = await self.supervisor?.value
    self.supervisor = nil
  }

  func receiveMessage(message: AppMessage, from peer: PeerID) {
    switch message {
    case .ping:
      logEvent(kind: "ping", direction: "in", peer: peer)
      onPing(from: peer)
    case .pong:
      logEvent(kind: "pong", direction: "in", peer: peer)
    }
  }

  func onPing(from peer: PeerID) {
    for action in instance.ping(peer) {
      switch action {
      case .callPong: sendPong(to: peer)
      }
    }
  }

  private func enqueue(_ job: @escaping Job) {
    self.cancelableJobs?.yield(job)
  }

  private func runMaintenanceTimer() async {
    while !Task.isCancelled {
      let connected = await transport.connectedPeers()

      var nextWake: ContinuousClock.Instant?
      for action in instance.update(ContinuousClock.now, connected: connected) {
        switch action {
        case .next(let time):
          nextWake = time
        case .dialsToStart(let peers):
          for peer in peers {
            guard let endpoint = self.endpoints[peer] else { continue }
            await transport.connect(to: endpoint)
          }
        case .pingsToSend(let peers):
          for peer in peers {
            sendPing(to: peer)
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

  private func deliverPing(to peer: PeerID) async {
    do {
      try await Task.sleep(for: getJitter())
      try await transport.send(.ping, to: peer)
      logEvent(kind: "ping", direction: "out", peer: peer)
    } catch is CancellationError {
      return  // shutting down
    } catch IndrasNetTransportError.peerNotConnected {
      return
    } catch {
      self.logger.notice("[\(self.peerId)] ping -> \(peer) failed: \(error)")
    }
  }

  private func deliverPong(to peer: PeerID) async {
    do {
      try await Task.sleep(for: getJitter())
      try await transport.send(.pong, to: peer)
      logEvent(kind: "pong", direction: "out", peer: peer)
    } catch is CancellationError {
      return  // shutting down
    } catch IndrasNetTransportError.peerNotConnected {
      return
    } catch {
      self.logger.notice("[\(self.peerId)] pong -> \(peer) failed: \(error)")
    }
  }

  private func getJitter() -> Duration {
    Duration(.milliseconds(Int64.random(in: 1..<500)))
  }

  private func logEvent(kind: String, direction: String, peer: PeerID) {
    let arrow = direction == "out" ? "->" : "<-"
    self.logger.info(
      "[\(self.peerId)] \(kind) \(arrow) \(peer)",
      metadata: [
        ShellLogKey.kind: .string(kind),
        ShellLogKey.direction: .string(direction),
        ShellLogKey.peer: .string(peer),
      ]
    )
  }
}

enum ShellLogKey {
  static let kind = "shell.kind"
  static let direction = "shell.direction"
  static let peer = "shell.peer"
}
