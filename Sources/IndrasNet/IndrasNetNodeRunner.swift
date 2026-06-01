import Foundation
import NIOCore
import NIOPosix

public struct IndrasNetNodeRunner: Sendable {
  public let local: ClusterEndpoint
  public let cluster: ClusterFile
  public let jsonEventLog: Bool

  /// Default ping cadence, jittered so nodes don't all fire in lockstep.
  public static func defaultPingInterval() -> Duration {
    .milliseconds(Int.random(in: 200..<500))
  }

  public init(
    local: ClusterEndpoint,
    cluster: ClusterFile,
    jsonEventLog: Bool = false
  ) {
    self.local = local
    self.cluster = cluster
    self.jsonEventLog = jsonEventLog
  }

  private var meshPeers: [ClusterEndpoint] {
    cluster.peerEndpoints(listenHost: local.host, listenPort: local.port)
  }

  public func run(untilInterrupted: @Sendable () async -> Void) async throws {
    let events = EventLogger(enabled: jsonEventLog)
    let peers = meshPeers
    let transport = IndrasNetTCPTransport(configuration: local.tcpConfiguration(peers: peers))
    let shell = Shell(self.local, transport: transport, events: events)
    let nodeName = local.addressKey

    try await shell.start(with: meshPeers)
    guard let meshPort = await transport.listenPort() else {
      enum StartError: Error { case noPortBound }
      throw StartError.noPortBound
    }

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
        await transport.connectMissingPeers()
        try? await Task.sleep(for: .seconds(1))
      }
    }

    events.emit(.running)
    ProcessLog.human("running (Ctrl+C to stop)")
    await untilInterrupted()
    ProcessLog.human("shutting down")

    redialTask.cancel()
    _ = await redialTask.value
    try await transport.shutdown()
  }
}
