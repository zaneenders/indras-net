import Foundation
import IndrasNet
import Synchronization

@main
struct IndrasNetCommand {
  static func main() async {
    do {
      if CommandLine.arguments.contains("--help") || CommandLine.arguments.contains("-h") {
        print(Self.usage)
        return
      }

      let clusterPath = try Self.parseClusterPath()
      let cluster = try ClusterFile.load(fromPath: clusterPath)
      let local = try Self.parseLocalEndpoint()
      let jsonEventLog = CommandLine.arguments.contains("--json-event-log")

      let runner = IndrasNetNodeRunner(
        local: local,
        cluster: cluster,
        jsonEventLog: jsonEventLog
      )
      try await runner.run(untilInterrupted: Self.waitForInterrupt)
    } catch let error as ConfigurationError {
      fputs("error: \(error.message)\n\n\(Self.usage)\n", stderr)
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
      --json-event-log       Emit NDJSON cluster events on stdout (human logs on stderr)
    """

  private enum ConfigurationError: Error {
    case message(String)

    var message: String {
      switch self {
      case .message(let text): text
      }
    }
  }

  private static func parseClusterPath() throws -> String {
    var args = Array(CommandLine.arguments.dropFirst())
    guard let index = args.firstIndex(of: "--cluster") else {
      return "cluster.json"
    }
    args.remove(at: index)
    guard index < args.count else {
      throw ConfigurationError.message("missing path for --cluster")
    }
    let path = args[index]
    args.remove(at: index)
    return path
  }

  private static func parseLocalEndpoint() throws -> ClusterEndpoint {
    var args = Array(CommandLine.arguments.dropFirst())

    args.removeAll { $0 == "--json-event-log" }

    if let index = args.firstIndex(of: "--cluster") {
      args.remove(at: index)
      if index < args.count {
        args.remove(at: index)
      }
    }

    guard args.count >= 2 else {
      throw ConfigurationError.message("expected <host> and <port>")
    }

    let host = args[0]
    guard let port = Int(args[1]) else {
      throw ConfigurationError.message("port must be an integer")
    }

    return ClusterEndpoint(host: host, port: port)
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
          // Both handlers may fire (e.g. SIGINT then SIGTERM); resume only once.
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
