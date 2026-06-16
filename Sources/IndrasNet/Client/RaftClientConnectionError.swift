public enum RaftClientConnectionError: Error, Equatable, Sendable {
  case handshakeFailed
  case connectionClosed
  case timedOut
}
