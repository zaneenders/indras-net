package import Setu

package struct PeerTarget: Codable, Hashable, Sendable {
  package let host: String
  package let port: Int
  package let connections: Int
  package let chunkSize: Int

  package init(
    host: String, port: Int, connections: Int,
    chunkSize: Int
  ) {
    self.host = host
    self.port = port
    self.connections = connections
    self.chunkSize = chunkSize
  }
}

package struct NodeJob: Codable, Sendable {
  package enum State: String, Codable, Sendable {
    case running, completed, failed, cancelled
  }
  package let id: String
  package let operation: String
  package let createdAt: Double
  package var state: State
  package var result: TransferResult?
  package var error: String?
  package var progress: TransferProgress? = nil
  package var peer: String? = nil
  package var destination: String? = nil
  package var recovery: DownloadRecovery? = nil

  package init(
    id: String, operation: String, createdAt: Double, state: State, result: TransferResult?, error: String?,
    progress: TransferProgress?, peer: String?, destination: String?, recovery: DownloadRecovery?
  ) {
    self.id = id
    self.operation = operation
    self.createdAt = createdAt
    self.state = state
    self.result = result
    self.error = error
    self.progress = progress
    self.peer = peer
    self.destination = destination
    self.recovery = recovery
  }
}

package enum NodeRequest: Codable, Sendable {
  case status
  case list(PeerTarget?)
  case send(peer: PeerTarget, file: String)
  case sendTo(peer: PeerTarget, file: String, destination: String)
  case sendToCreatingDirectory(peer: PeerTarget, file: String, destination: String)
  case get(peer: PeerTarget, fileID: String, output: String?)
  case finish(id: String, output: String?)
  case jobs
  case job(String)
  case cancel(String)

  // Keep targeted sends distinct so older local nodes cannot ignore the directory.
  package static func upload(
    peer: PeerTarget, file: String, destination: String?, createDestination: Bool
  ) throws(any Error) -> NodeRequest {
    if createDestination {
      guard let destination else { throw TransferError("Creating a destination requires a receiver directory path") }
      return .sendToCreatingDirectory(peer: peer, file: file, destination: destination)
    }
    if let destination { return .sendTo(peer: peer, file: file, destination: destination) }
    return .send(peer: peer, file: file)
  }
}

package enum NodeReply: Codable, Sendable {
  case status(node: String, port: Int, directory: String)
  case files([FileEntry])
  case job(NodeJob)
  case jobs([NodeJob])
  case failure(String)
}
