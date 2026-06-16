import Foundation

@testable import IndrasNet

package typealias SimulatedShell = Shell<SimulatedTransport>

package actor SimulatedTransport: NodeTransport {
  private let localPeerID: PeerId
  private let listenPortValue: Int
  private let mesh: Mesh
  private var isStarted = false

  /// Peers whose next outbound `appendEntries` blocks in `send` until released.
  private var gatedPeers: Set<PeerId> = []
  private var sendWaiters: [PeerId: [CheckedContinuation<Void, Never>]] = [:]

  /// AppendEntries payloads delivered to the mesh, in wire-send order.
  package private(set) var appendEntriesWireOrder: [[LogEntry]] = []

  package init(peer: NodeAddress, mesh: Mesh) {
    self.localPeerID = peer.addressKey
    self.listenPortValue = peer.port
    self.mesh = mesh
  }

  /// The next `appendEntries` to `peer` will not be delivered until
  /// ``releaseHeldSend(to:)`` resumes it.
  package func holdNextAppendEntriesSend(to peer: PeerId) {
    gatedPeers.insert(peer)
  }

  package func hasHeldSend(to peer: PeerId) -> Bool {
    sendWaiters[peer]?.isEmpty == false
  }

  package func releaseHeldSend(to peer: PeerId) {
    if let waiter = sendWaiters[peer]?.removeFirst() {
      waiter.resume()
    }
    if sendWaiters[peer]?.isEmpty == true {
      sendWaiters.removeValue(forKey: peer)
    }
  }

  package func start(onMessage: @escaping IndrasNetInboundHandler) async throws {
    guard !isStarted else { return }
    isStarted = true
    await mesh.register(peer: localPeerID, listenPort: listenPortValue)
    await mesh.setHandler(peer: localPeerID, handler: onMessage)
  }

  package func shutdown() async throws {
    await mesh.setHandler(peer: localPeerID, handler: nil)
    await mesh.unregister(peer: localPeerID)
    isStarted = false
    for waiters in sendWaiters.values {
      for waiter in waiters {
        waiter.resume()
      }
    }
    sendWaiters.removeAll()
    gatedPeers.removeAll()
    appendEntriesWireOrder.removeAll()
  }

  package func listenPort() async -> Int? {
    listenPortValue
  }

  package func connectedPeers() async -> Set<PeerId> {
    await mesh.connectedPeers(for: localPeerID)
  }

  package func isConnected(to peer: PeerId) async -> Bool {
    await mesh.canDeliver(from: localPeerID, to: peer)
  }

  package func waitForConnection(to peer: PeerId, timeout: Duration) async -> Bool {
    if await mesh.canDeliver(from: localPeerID, to: peer) {
      return true
    }
    if await mesh.isPartitioned(from: localPeerID, to: peer) {
      return false
    }
    return await mesh.waitForConnection(from: localPeerID, to: peer, timeout: timeout)
  }

  package func connect(to peer: NodeAddress) async {
    // In-memory peers are connected once both sides have registered with the mesh.
  }

  package func send(_ message: RaftMessage, to peer: PeerId) async throws {
    if case .appendEntries(let args) = message, gatedPeers.contains(peer) {
      gatedPeers.remove(peer)
      await withCheckedContinuation { continuation in
        sendWaiters[peer, default: []].append(continuation)
      }
    }

    if case .appendEntries(let args) = message {
      appendEntriesWireOrder.append(args.entries)
    }
    try await mesh.deliver(from: localPeerID, to: peer, message: message)
  }
}
