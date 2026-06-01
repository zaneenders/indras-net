enum IndrasNetTransportError: Error, Equatable, Sendable {
  case peerNotConnected(PeerID)
}
