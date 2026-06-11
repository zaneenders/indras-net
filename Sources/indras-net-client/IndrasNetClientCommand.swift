import Foundation
import IndrasNet
import SystemPackage

@main
struct IndrasNetClientCommand {
  static func main() async {
    do {
      if CommandLine.arguments.contains("--help") || CommandLine.arguments.contains("-h") {
        print(usage)
        return
      }

      let options = try parseArguments()
      let command = Data(options.message.utf8)
      let target = NodeAddress(host: options.host, port: options.port)

      var result = try await IndrasNetClient.submit(
        command: command,
        to: target,
        timeout: .seconds(options.timeoutSeconds)
      )

      if case .notLeader(let leader) = result.status, options.followLeader, let leader {
        let leaderAddress = try parsePeerAddress(leader)
        result = try await IndrasNetClient.submit(
          command: command,
          to: leaderAddress,
          timeout: .seconds(options.timeoutSeconds)
        )
      }

      switch result.status {
      case .ok:
        print("ok logIndex=\(result.logIndex)")
      case .notLeader(let leader):
        if let leader {
          writeStderr("not leader (try leader \(leader))\n")
        } else {
          writeStderr("not leader (leader unknown)\n")
        }
        exit(2)
      }
    } catch let error as CLIError {
      writeStderr("error: \(error.message)\n\n\(usage)\n")
      exit(1)
    } catch {
      writeStderr("error: \(error)\n")
      exit(1)
    }
  }

  private static let usage = """
    Usage: indras-net-client <host> <port> <message> [options]

    Submits a log command to a Raft node over TCP.

    Example:
      indras-net-client 127.0.0.1 9001 "set x=1"

    Options:
      --no-follow-leader     Do not retry on the leader when redirected (default: follow)
      --timeout <seconds>    Response timeout (default: 10)
    """

  private struct CLIError: Error {
    let message: String
  }

  private struct Options {
    let host: String
    let port: Int
    let message: String
    let followLeader: Bool
    let timeoutSeconds: Int
  }

  private static func writeStderr(_ text: String) {
    _ = try? FileDescriptor.standardError.writeAll(text.utf8)
  }

  private static func parseArguments() throws -> Options {
    var args = Array(CommandLine.arguments.dropFirst())
    var followLeader = true
    var timeoutSeconds = 10

    while let flagIndex = args.firstIndex(where: { $0 == "--no-follow-leader" || $0 == "--timeout" }) {
      let flag = args.remove(at: flagIndex)
      switch flag {
      case "--no-follow-leader":
        followLeader = false
      case "--timeout":
        guard flagIndex < args.count else {
          throw CLIError(message: "missing value for --timeout")
        }
        let value = args.remove(at: flagIndex)
        guard let parsed = Int(value), parsed > 0 else {
          throw CLIError(message: "timeout must be a positive integer")
        }
        timeoutSeconds = parsed
      default:
        break
      }
    }

    guard args.count >= 3 else {
      throw CLIError(message: "expected <host>, <port>, and <message>")
    }

    let host = args[0]
    guard let port = Int(args[1]) else {
      throw CLIError(message: "port must be an integer")
    }

    let message = args.dropFirst(2).joined(separator: " ")
    guard !message.isEmpty else {
      throw CLIError(message: "message must not be empty")
    }

    return Options(
      host: host,
      port: port,
      message: message,
      followLeader: followLeader,
      timeoutSeconds: timeoutSeconds
    )
  }

  private static func parsePeerAddress(_ peer: String) throws -> NodeAddress {
    let parts = peer.split(separator: ":", maxSplits: 1).map(String.init)
    guard parts.count == 2, let port = Int(parts[1]) else {
      throw CLIError(message: "invalid leader address '\(peer)'")
    }
    return NodeAddress(host: parts[0], port: port)
  }
}
