public struct IndrasNetTCPConfiguration: Sendable {
  public var localPeerID: PeerID
  public var host: String
  public var port: Int  // Use `0` to bind an ephemeral port in tests.
  public var peers: [ClusterEndpoint]

  public init(
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
