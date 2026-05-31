import NIOCore

actor PeerConnectionManager {
  private var outboundWriters: [PeerID: NIOAsyncChannelOutboundWriter<Message>] = [:]

  func register(peerID: PeerID, outbound: NIOAsyncChannelOutboundWriter<Message>) {
    self.outboundWriters[peerID] = outbound
  }

  func unregister(peerID: PeerID) {
    self.outboundWriters.removeValue(forKey: peerID)
  }

  func send(_ message: Message, to peerID: PeerID) async throws {
    guard let writer = self.outboundWriters[peerID] else {
      throw IndrasNetTransportError.peerNotConnected(peerID)
    }
    try await writer.write(message)
  }

  func contains(peerID: PeerID) -> Bool {
    self.outboundWriters[peerID] != nil
  }
}
