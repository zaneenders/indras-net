import Foundation
import IndrasNet

@main
enum IndrasNetCommand {
  private static let usage = """
    Usage: indras-net [options] [host] [port]

    By default a node binds [host] [port] and serves inbound connections.
    With --connect it dials [host] [port] and runs a client script instead.

    Options:
      --json-event-log    Emit NDJSON events on stdout (human logs on stderr)
      --connect           Dial [host] [port] and run a client script instead of listening
      --script <path>     JSON array of send/expect steps (implies --connect)
    """

  static func main() async throws {
    var args = Array(CommandLine.arguments.dropFirst().filter { $0 != "--" })
    var jsonEventLog = false
    var mode = Config.Mode.serve
    var script = ClientScriptStep.defaultPingHello

    while let flag = args.first, flag.hasPrefix("--") {
      args.removeFirst()
      switch flag {
      case "--json-event-log":
        jsonEventLog = true
      case "--connect":
        mode = .connect
      case "--script":
        guard let path = args.first else {
          ProcessLog.human("error: missing path for --script\n\n\(usage)")
          throw ExitCode.failure
        }
        args.removeFirst()
        script = try ClientScriptStep.load(from: URL(fileURLWithPath: path))
        mode = .connect
      case "--help", "-h":
        print(usage)
        return
      default:
        ProcessLog.human("error: unknown option \(flag)\n\n\(usage)")
        throw ExitCode.failure
      }
    }

    let host = args.first ?? "127.0.0.1"
    let port = args.dropFirst().first.flatMap(Int.init) ?? 7878

    let node = IndrasNet(
      config: Config(
        mode: mode,
        host: host,
        port: port,
        jsonEventLog: jsonEventLog,
        clientScript: script
      )
    )
    try await node.runNode()
  }
}

private enum ExitCode: Error {
  case failure
}
