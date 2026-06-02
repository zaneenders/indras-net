public struct IndrasNetTCPConfiguration: Sendable {
  let magic: UInt8 = Message.magic
  let version: UInt8 = Message.version
  var localPeerID: String
  var host: String
  var port: Int  // Use `0` to bind an ephemeral port in tests.
  var peers: [ClusterEndpoint]

  public init(
    localPeerID: String,
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
