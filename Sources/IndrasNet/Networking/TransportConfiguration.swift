public struct TransportConfiguration: Sendable {
  let magic: UInt8 = HandshakeFrame.magic
  let version: UInt8 = HandshakeFrame.version
  var localPeerID: String
  var host: String
  var port: Int  // Use `0` to bind an ephemeral port in tests.
  // How long a freshly opened channel has to complete the handshake (signal +
  // greet/hello + adoption) before it is forcibly closed. Bounds the time a
  // stalled peer can pin a `dialing` slot or a half-open accepted channel.
  var handshakeTimeout: Duration

  public init(
    localPeerID: String,
    host: String,
    port: Int,
    handshakeTimeout: Duration = .seconds(5)
  ) {
    self.localPeerID = localPeerID
    self.host = host
    self.port = port
    self.handshakeTimeout = handshakeTimeout
  }
}
