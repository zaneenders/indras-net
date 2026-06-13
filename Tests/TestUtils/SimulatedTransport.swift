import Foundation
import IndrasNet

package typealias SimulatedShell = Shell<SimulatedTransport>

package actor SimulatedTransport: NodeTransport {
  private let localPeerID: PeerId
  private let listenPortValue: Int
  private let mesh: Mesh
  private var isStarted = false

  package init(peer: NodeAddress, mesh: Mesh) {
    self.localPeerID = peer.addressKey
    self.listenPortValue = peer.port
    self.mesh = mesh
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
    await mesh.waitForConnection(from: localPeerID, to: peer, timeout: timeout)
  }

  package func connect(to peer: NodeAddress) async {
    // In-memory peers are connected once both sides have registered with the mesh.
  }

  package func send(_ message: RaftMessage, to peer: PeerId) async throws {
    try await mesh.deliver(from: localPeerID, to: peer, message: message)
  }
}
