import Foundation
import Logging
import NIOCore
import Networking
import Setu
import _NIOFileSystem

#if canImport(Darwin)
import Darwin
#elseif canImport(Musl)
import Musl
#else
import Glibc
#endif

package struct NodeControlClient: Sendable {
  private let socket: String
  private let connections = ServerConnections()

  package init(socket: String) throws {
    guard socket.hasPrefix("/"), !socket.utf8.contains(0) else { throw TransferError("Invalid control socket path") }
    self.socket = socket
  }

  package func request(_ command: NodeRequest) async throws -> NodeReply {
    guard !connections.isClosed else { throw PeerTransportError.closed }
    return try await withDeadline(timeout: .seconds(35)) {
      let connection: Connection
      do { connection = try await TCP.connect(unixDomainSocketPath: socket, connectTimeout: .seconds(10)) } catch let
        error as IOError
      {
        if let failure = connectionError(error) { throw failure }
        throw error
      }
      connections.add(connection.channel)
      return try await connection.executeThenClose { inbound, outbound in
        try await outbound.write(SetuWire.encode(command, target: SetuWire.control, opcode: SetuWire.request))
        var iterator = inbound.makeAsyncIterator()
        guard let frame = try await iterator.next() else { throw PeerTransportError.disconnected }
        let reply = try SetuWire.decode(frame, target: SetuWire.control, opcode: SetuWire.response, as: NodeReply.self)
        if case .failure(let message) = reply { throw TransferError(message) }
        return reply
      }
    }
  }

  package func close() async { await connections.close() }

  private func connectionError(_ error: IOError) -> TransferError? {
    let problem: String
    let recovery: String
    switch error.errnoCode {
    case ENOENT, ECONNREFUSED:
      problem = error.errnoCode == ENOENT ? "control socket not found" : "connection refused; the node may have stopped"
      var startup =
        "Start the node with 'setu node --temporary-directory /absolute/path' (from source: 'swift run setu node --temporary-directory /absolute/path')."
      #if os(Linux)
      startup += "\nIf installed as a user service, run 'systemctl --user start setu.service'."
      #endif
      recovery = startup
    case EACCES, EPERM:
      problem = "permission denied"
      recovery = "Run the CLI as the same user as the node and check the control socket directory's permissions."
    case ENOTDIR, ENOTSOCK:
      problem = "the control socket path is not a Unix socket or contains a non-directory component"
      recovery = "Check the node's configured control socket path."
    default:
      return nil
    }
    return TransferError(
      """
      Cannot connect to the local Setu node at '\(socket)': \(problem).
      \(recovery)
      If the node uses a different socket, set --socket or SETU_SOCKET to match.
      """)
  }
}
