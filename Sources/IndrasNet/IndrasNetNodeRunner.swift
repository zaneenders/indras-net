import Foundation
import NIOCore
import NIOPosix

public struct IndrasNetNodeRunner: Sendable {
  public let local: ClusterEndpoint
  public let cluster: ClusterFile
  public let pingInterval: Duration
  public let jsonEventLog: Bool

  /// Default ping cadence, jittered so nodes don't all fire in lockstep.
  public static func defaultPingInterval() -> Duration {
    .milliseconds(Int.random(in: 200..<500))
  }

  public init(
    local: ClusterEndpoint,
    cluster: ClusterFile,
    pingInterval: Duration = IndrasNetNodeRunner.defaultPingInterval(),
    jsonEventLog: Bool = false
  ) {
    self.local = local
    self.cluster = cluster
    self.pingInterval = pingInterval
    self.jsonEventLog = jsonEventLog
  }

  private var meshPeers: [ClusterEndpoint] {
    cluster.peerEndpoints(listenHost: local.host, listenPort: local.port)
  }

  public func run(untilInterrupted: @Sendable () async -> Void) async throws {
    let events = EventLogger(enabled: jsonEventLog)
    let peers = meshPeers
    let meshNode = IndrasNetTCPNode(configuration: local.tcpConfiguration(peers: peers))
    let nodeName = local.addressKey

    try await meshNode.start { message, from in
      await Self.handleMeshMessage(
        message: message,
        from: from,
        meshNode: meshNode,
        nodeName: nodeName,
        events: events
      )
    }

    let meshPort = await meshNode.listenPort() ?? local.port
    events.emit(.listening(node: nodeName, host: local.host, port: meshPort))
    ProcessLog.human("node \(nodeName) mesh \(local.host):\(meshPort)")
    if peers.isEmpty {
      ProcessLog.human("no peers in cluster.json")
    } else {
      let peerList = peers.map(\.peerID.description).joined(separator: ", ")
      ProcessLog.human("peers: \(peerList)")
    }

    let redialTask = Task {
      while !Task.isCancelled {
        await meshNode.connectMissingPeers()
        try? await Task.sleep(for: .seconds(1))
      }
    }

    let pingTask = Task {
      while !Task.isCancelled {
        try? await Task.sleep(for: pingInterval)
        for peer in peers {
          guard await meshNode.isConnected(to: peer.peerID) else { continue }
          do {
            try await meshNode.send(Message(type: .ping, payload: .init()), to: peer.peerID)
            events.emit(.pingSent(from: nodeName, to: peer.peerID.description))
            ProcessLog.human("[\(nodeName)] ping -> \(peer.peerID)")
          } catch {
            events.emit(
              .failedToPing(
                node: nodeName,
                peer: peer.peerID.description,
                error: String(describing: error)
              )
            )
          }
        }
      }
    }

    events.emit(.running)
    ProcessLog.human("running (Ctrl+C to stop)")
    await untilInterrupted()
    ProcessLog.human("shutting down")

    redialTask.cancel()
    pingTask.cancel()
    _ = await redialTask.value
    _ = await pingTask.value
    try await meshNode.shutdown()
  }

  private static func handleMeshMessage(
    message: Message,
    from: PeerID,
    meshNode: IndrasNetTCPNode,
    nodeName: String,
    events: EventLogger
  ) async {
    let remoteID = from.description
    switch message.type {
    case .ping:
      events.emit(.pingReceived(node: nodeName, from: remoteID))
      ProcessLog.human("[\(nodeName)] ping <- \(from)")
      do {
        try await meshNode.send(Message(type: .pong, payload: message.payload), to: from)
        events.emit(.pongSent(from: nodeName, to: remoteID))
      } catch {
        events.emit(.failedToPong(node: nodeName, peer: remoteID, error: String(describing: error)))
      }
    case .pong:
      events.emit(.pongReceived(node: nodeName, from: remoteID))
    default:
      break
    }
  }
}
