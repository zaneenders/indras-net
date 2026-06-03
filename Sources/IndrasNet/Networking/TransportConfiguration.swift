public struct TransportConfiguration: Sendable {
  let magic: UInt8 = HandshakeFrame.magic
  let version: UInt8 = HandshakeFrame.version
  var localPeerID: String
  var host: String
  var port: Int  // Use `0` to bind an ephemeral port in tests.

  public init(
    localPeerID: String,
    host: String,
    port: Int
  ) {
    self.localPeerID = localPeerID
    self.host = host
    self.port = port
  }
}
