import Foundation

package typealias IndrasNetInboundHandler = @Sendable (RaftMessage, PeerId) async -> Void

/// Transport surface area used by `Shell` for peer messaging.
package protocol NodeTransport: Actor {
  func start(onMessage: @escaping IndrasNetInboundHandler) async throws
  func shutdown() async throws
  func listenPort() async -> Int?
  func connectedPeers() async -> Set<PeerId>
  func isConnected(to peer: PeerId) async -> Bool
  func waitForConnection(to peer: PeerId, timeout: Duration) async -> Bool
  func connect(to peer: NodeAddress) async
  func send(_ message: RaftMessage, to peer: PeerId) async throws
}
