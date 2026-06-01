struct IndrasNetTCPConfiguration: Sendable {
  var localPeerID: PeerID
  var host: String
  var port: Int  // Use `0` to bind an ephemeral port in tests.
  var peers: [ClusterEndpoint]

  init(
    localPeerID: PeerID,
    host: String,
    port: Int,
    peers: [ClusterEndpoint] = []
  ) {
    self.localPeerID = localPeerID
    self.host = host
    self.port = port
    self.peers = peers
  }
}
