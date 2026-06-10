import Foundation
import IndrasNet
import Logging
import Synchronization
import SystemPackage

@main
struct IndrasNetCommand {
  static func main() async {
    do {
      if CommandLine.arguments.contains("--help") || CommandLine.arguments.contains("-h") {
        print(usage)
        return
      }

      let (local, clusterPath, logLevel) = try parseArguments()
      LoggingSystem.bootstrap { label in
        var handler = StreamLogHandler.standardError(label: label)
        handler.logLevel = logLevel
        return handler
      }

      let cluster = try ClusterConfig.load(from: clusterPath)
      let peers = cluster.peers(excluding: local)
      try await runNode(local: local, peers: peers, timing: cluster.timing, logLevel: logLevel)
    } catch let error as CLIError {
      writeStderr("error: \(error.message)\n\n\(usage)\n")
      exit(1)
    } catch {
      writeStderr("error: \(error)\n")
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
        ],
        "heartbeatIntervalMs": 50,
        "electionTimeoutMinMs": 150,
        "electionTimeoutMaxMs": 300
      }

    Options:
      --cluster <path>       Shared cluster file (default: ./cluster.json)
      --log-level <level>    Log level: trace, debug, info, notice, warning, error (default: info)
    """

  private struct CLIError: Error {
    let message: String
  }

  private static func writeStderr(_ text: String) {
    _ = try? FileDescriptor.standardError.writeAll(text.utf8)
  }

  private static func parseArguments() throws -> (NodeAddress, String, Logger.Level) {
    var args = Array(CommandLine.arguments.dropFirst())
    var clusterPath = "cluster.json"
    var logLevel: Logger.Level = .info

    while let flagIndex = args.firstIndex(where: { $0 == "--cluster" || $0 == "--log-level" }) {
      let flag = args.remove(at: flagIndex)
      guard flagIndex < args.count else {
        throw CLIError(message: "missing value for \(flag)")
      }
      let value = args.remove(at: flagIndex)
      switch flag {
      case "--cluster":
        clusterPath = value
      case "--log-level":
        guard let parsed = Logger.Level(rawValue: value.lowercased()) else {
          throw CLIError(message: "invalid log level '\(value)'")
        }
        logLevel = parsed
      default:
        break
      }
    }

    guard args.count >= 2 else {
      throw CLIError(message: "expected <host> and <port>")
    }

    let host = args[0]
    guard let port = Int(args[1]) else {
      throw CLIError(message: "port must be an integer")
    }

    return (NodeAddress(host: host, port: port), clusterPath, logLevel)
  }

  private static func makeLogger(label: String, level: Logger.Level) -> Logger {
    var logger = Logger(label: label)
    logger.logLevel = level
    return logger
  }

  private static func runNode(
    local: NodeAddress,
    peers: [NodeAddress],
    timing: NodeTiming,
    logLevel: Logger.Level
  ) async throws {
    let log = makeLogger(label: "indras-net", level: logLevel)
    let transport = TCPTransport(
      configuration: local.tcpConfiguration(),
      logger: makeLogger(label: "indras-net.transport", level: logLevel)
    )
    let shell = Shell(
      local,
      timing: timing,
      transport: transport,
      logger: makeLogger(label: "indras-net.shell", level: logLevel)
    )
    let port = try await shell.start(with: peers)

    log.info("node \(local.addressKey) mesh \(local.host):\(port)")
    if peers.isEmpty {
      log.info("no peers in cluster.json")
    } else {
      log.info("peers: \(peers.map(\.addressKey).joined(separator: ", "))")
    }

    log.info("running (Ctrl+C to stop)")
    await waitForInterrupt()
    log.info("shutting down")

    try await shell.shutdown()
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
