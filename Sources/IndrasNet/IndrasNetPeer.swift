struct IndrasNetPeer {
  let id: PeerID
  let transport: IndrasNetTCPTransport

  init(id: PeerID, _ transport: IndrasNetTCPTransport) {
    self.id = id
    self.transport = transport
  }

  func send(message: Message, to peerId: PeerID) async throws {
    try await transport.send(message, to: peerId)
  }
}
