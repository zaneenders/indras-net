import NIOCore

// Shell owns the Instance and the Peer and glues the two together
actor Shell {
  var instance: Instance
  let peer: IndrasNetPeer
  let eventLogger: EventLogger
  let transport: IndrasNetTCPTransport

  init(_ node: ClusterEndpoint, transport: IndrasNetTCPTransport, events: EventLogger) {
    self.peer = IndrasNetPeer(id: node.peerID, transport)
    self.transport = transport
    self.instance = Instance(node.peerID)
    self.eventLogger = events
  }

  func start(with peers: [ClusterEndpoint]) async throws {
    instance.peers.formUnion(peers.map(\.peerID))
    try await transport.start { message, from in
      await self.recieveMessage(message: message, from: from)
    }
    sendHellos()
  }

  func sendHellos() {
    for action in instance.update(ContinuousClock.now) {
      switch action {
      case .next(let time):
        Task {
          let sleep = ContinuousClock.now.duration(to: time)
          try await Task.sleep(for: sleep)
          sendHellos()
        }
      case .hellosToSend(let hellos):
        for helloAction in hellos {
          switch helloAction {
          case .callPing(let peer):
            Task {
              ProcessLog.human("\(self.peer.id.rawValue) hello to: \(peer.rawValue)")
              try await transport.send(.hello(peerID: self.peer.id), to: peer)
            }
          }
        }
      }
    }
  }

  private func getJitter() -> Duration {
    Duration(.milliseconds(Int64.random(in: 1..<500)))
  }

  func recieveMessage(message: Message, from peer: PeerID) {
    switch message.type {
    case .hello: onHello(from: peer)
    case .ping:
      eventLogger.emit(.pingReceived(node: self.peer.id.rawValue, from: peer.rawValue))
      ProcessLog.human("[\(self.peer.id.rawValue)] ping <- \(peer.rawValue)")
      onPing(from: peer)
    case .pong:
      eventLogger.emit(.pongReceived(node: self.peer.id.rawValue, from: peer.rawValue))
      ProcessLog.human("[\(self.peer.id.rawValue)] pong <- \(peer.rawValue)")
      onPong(from: peer)
    default:
      ProcessLog.human("Shell: default[\(message)], from: \(peer)")
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

  private func sendPong(to: PeerID) {
    Task {
      do {
        try await Task.sleep(for: getJitter())
        try await self.peer.send(message: Message(type: .pong, payload: ByteBuffer()), to: to)
        eventLogger.emit(.pongSent(from: self.peer.id.rawValue, to: to.rawValue))
      } catch {
        eventLogger.emit(.failedToPong(node: self.peer.id.rawValue, peer: to.rawValue, error: "\(error)"))
      }
    }
  }

  private func sendPing(to: PeerID) {
    Task {
      do {
        try await Task.sleep(for: getJitter())
        try await self.peer.send(message: Message(type: .ping, payload: ByteBuffer()), to: to)
        eventLogger.emit(.pingSent(from: self.peer.id.rawValue, to: to.rawValue))
      } catch {
        eventLogger.emit(.failedToPing(node: self.peer.id.rawValue, peer: to.rawValue, error: "\(error)"))
      }
    }
  }
}
