import Foundation
import IndrasNet
import Logging
import Synchronization

private let log = Logger(label: "indras-net")

@main
struct IndrasNetCommand {
  static func main() async {
    LoggingSystem.bootstrap { StreamLogHandler.standardError(label: $0) }

    do {
      if CommandLine.arguments.contains("--help") || CommandLine.arguments.contains("-h") {
        print(usage)
        return
      }

      let (local, clusterPath) = try parseArguments()
      let peers = try loadPeers(from: clusterPath, excluding: local)
      try await runNode(local: local, peers: peers)
    } catch let error as CLIError {
      fputs("error: \(error.message)\n\n\(usage)\n", stderr)
      exit(1)
    } catch {
      fputs("error: \(error)\n", stderr)
      exit(1)
    }
  }

  private static let usage = """
    Usage: indras-net <host> <port> [options]

    Runs a mesh node at <host>:<port>. Peers come from the shared cluster.json
    (same file on every node). Each peer is addressed as host:port.

    Example cluster.json:

      {
        "peers": [
          { "host": "127.0.0.1", "port": 9001 },
          { "host": "127.0.0.1", "port": 9002 }
        ]
      }

    Options:
      --cluster <path>       Shared cluster file (default: ./cluster.json)
    """

  private struct CLIError: Error {
    let message: String
  }

  private struct ClusterFile: Decodable {
    var peers: [ClusterEndpoint]
  }

  private static func parseArguments() throws -> (ClusterEndpoint, String) {
    var args = Array(CommandLine.arguments.dropFirst())
    var clusterPath = "cluster.json"

    if let index = args.firstIndex(of: "--cluster") {
      args.remove(at: index)
      guard index < args.count else {
        throw CLIError(message: "missing path for --cluster")
      }
      clusterPath = args.remove(at: index)
    }

    guard args.count >= 2 else {
      throw CLIError(message: "expected <host> and <port>")
    }

    let host = args[0]
    guard let port = Int(args[1]) else {
      throw CLIError(message: "port must be an integer")
    }

    return (ClusterEndpoint(host: host, port: port), clusterPath)
  }

  private static func loadPeers(
    from path: String,
    excluding local: ClusterEndpoint
  ) throws -> [ClusterEndpoint] {
    let data = try Data(contentsOf: URL(fileURLWithPath: path))
    let cluster = try JSONDecoder().decode(ClusterFile.self, from: data)
    return cluster.peers.filter { $0.host != local.host || $0.port != local.port }
  }

  private static func runNode(local: ClusterEndpoint, peers: [ClusterEndpoint]) async throws {
    let transport = IndrasNetTCPTransport(
      configuration: local.tcpConfiguration()
    )
    let shell = Shell(local, transport: transport)
    let nodeName = local.addressKey

    try await shell.start(with: peers)
    guard let meshPort = await transport.listenPort() else {
      await shell.stop()
      try? await transport.shutdown()
      throw CLIError(message: "no port bound")
    }

    log.info("node \(nodeName) mesh \(local.host):\(meshPort)")
    if peers.isEmpty {
      log.info("no peers in cluster.json")
    } else {
      log.info("peers: \(peers.map(\.addressKey).joined(separator: ", "))")
    }

    log.info("running (Ctrl+C to stop)")
    await waitForInterrupt()
    log.info("shutting down")

    await shell.stop()
    try await transport.shutdown()
  }

  private static func waitForInterrupt() async {
    await withCheckedContinuation { (continuation: CheckedContinuation<Void, Never>) in
      let once = SignalOnce()
      let signals: [Int32] = [SIGINT, SIGTERM]
      let sources = signals.map {
        DispatchSource.makeSignalSource(signal: $0, queue: .global())
      }
      for (sig, source) in zip(signals, sources) {
        source.setEventHandler {
          guard once.fire() else { return }
          sources.forEach { $0.cancel() }
          continuation.resume()
        }
        signal(sig, SIG_IGN)
        source.resume()
      }
    }
  }
}

private final class SignalOnce: Sendable {
  private let fired = Atomic<Bool>(false)

  func fire() -> Bool {
    fired.compareExchange(expected: false, desired: true, ordering: .relaxed).exchanged
  }
}
