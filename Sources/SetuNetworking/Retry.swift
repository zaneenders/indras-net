import NIOCore
import NIOPosix
import Networking
import Setu

#if canImport(Darwin)
import Darwin
#elseif canImport(Musl)
import Musl
#else
import Glibc
#endif

func retryTransient<Value: Sendable>(
  _ operation: @Sendable () async throws -> Value
) async throws -> Value {
  for attempt in 0..<3 {
    try Task.checkCancellation()
    do { return try await operation() } catch {
      try Task.checkCancellation()
      guard attempt < 2, isTransient(error) else { throw error }
      try await Task.sleep(for: .milliseconds(250 * (1 << attempt)))
    }
  }
  throw TransferError("Retry limit reached")
}

func isTransient(_ error: any Error) -> Bool {
  if let error = error as? NIOConnectionError {
    return !error.connectionErrors.isEmpty && error.connectionErrors.allSatisfy { isTransient($0.error) }
  }
  if let error = error as? FrameError { return error == .truncatedFrame }
  if let error = error as? PeerTransportError {
    switch error {
    case .disconnected, .timedOut: return true
    case .closed: return false
    }
  }
  if let error = error as? IOError {
    return [ECONNRESET, ECONNABORTED, ECONNREFUSED, ETIMEDOUT, EPIPE, ENETUNREACH, EHOSTUNREACH]
      .contains(error.errnoCode)
  }
  if let error = error as? ChannelError {
    switch error {
    case .connectTimeout, .ioOnClosedChannel, .eof, .inputClosed, .outputClosed: return true
    default: return false
    }
  }
  return false
}
