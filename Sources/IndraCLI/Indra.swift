import ArgumentParser
import Foundation
import IndraNode
import TCPActorSystem
import UnixSignals

@main
struct Indra: AsyncParsableCommand {
  static let configuration = CommandConfiguration(
    commandName: "indra", abstract: "Discover and communicate with Indra nodes.",
    subcommands: [Initialize.self, Daemon.self, Status.self, Nodes.self, Ping.self, Join.self, Reload.self])
}

struct LocalOptions: ParsableArguments {
  @Option(help: "Private daemon control socket (or INDRA_SOCKET).") var socket = NodePaths.socket
  @Flag(help: "Print JSON.") var json = false
}

struct Initialize: AsyncParsableCommand {
  static let configuration = CommandConfiguration(commandName: "init", abstract: "Create a node configuration.")
  @Option(help: "Configuration file.") var config = NodePaths.configuration
  @Option(help: "Node name.") var name = ProcessInfo.processInfo.hostName
  @Option(help: "Cluster name.") var cluster = "indra"
  @Option(help: "Address to bind.") var listenHost = "127.0.0.1"
  @Option(help: "Address other nodes can reach.") var advertisedHost = "127.0.0.1"
  @Option(help: "TCP port.") var port = 7331
  @Option(help: "Seed host:port; repeat for multiple seeds.") var seed: [String] = []

  func run() throws {
    let configuration = NodeConfiguration(
      version: 1, id: UUID(), name: name, cluster: cluster, listenHost: listenHost, advertisedHost: advertisedHost,
      port: port, seeds: try seed.map { try PeerAddress($0) }, probeIntervalMilliseconds: 3000,
      requestTimeoutMilliseconds: 2000)
    try configuration.save(path: config, overwrite: false)
    print("Created \(config)")
  }
}

struct Daemon: AsyncParsableCommand {
  static let configuration = CommandConfiguration(
    abstract: "Run the node in the foreground for launchd/systemd; reload config every second or on SIGHUP.")
  @Option(help: "Configuration file.") var config = NodePaths.configuration
  @Option(help: "Private control socket (or INDRA_SOCKET).") var socket = NodePaths.socket

  func run() async throws {
    let signals = await UnixSignalsSequence(trapping: [.sigint, .sigterm])
    do {
      try await withThrowingTaskGroup(of: Void.self) { tasks in
        tasks.addTask {
          try await runNode(configurationPath: config, socket: socket) { status in
            print(
              "indra \(status.node.name) ready at \(status.node.endpoint.host):\(status.node.endpoint.port); control: \(socket)"
            )
          }
        }
        tasks.addTask {
          for await _ in signals { return }
        }
        defer { tasks.cancelAll() }
        _ = try await tasks.next()
      }
    } catch is CancellationError {}
  }
}

struct Status: AsyncParsableCommand {
  static let configuration = CommandConfiguration(abstract: "Show the local node and configuration health.")
  @OptionGroup var options: LocalOptions

  func run() async throws {
    guard case .status(let status) = try await NodeControl.request(.status, socket: options.socket) else {
      throw IndraError("Unexpected daemon response")
    }
    try printStatus(status, json: options.json)
  }
}

struct Nodes: AsyncParsableCommand {
  static let configuration = CommandConfiguration(
    abstract: "List known nodes and their directly observed reachability.")
  @OptionGroup var options: LocalOptions

  func run() async throws {
    guard case .status(let status) = try await NodeControl.request(.status, socket: options.socket) else {
      throw IndraError("Unexpected daemon response")
    }
    if options.json {
      try printJSON(status)
      return
    }
    print("ID  NAME  ADDRESS  STATE")
    print("\(status.node.id)  \(status.node.name)  \(status.node.endpoint.host):\(status.node.endpoint.port)  local")
    for member in status.members {
      let node = member.node
      let address = PeerAddress(host: node.endpoint.host, port: node.endpoint.port)
      print("\(node.id)  \(node.name)  \(address)  \(member.state.rawValue)")
    }
  }
}

struct Ping: AsyncParsableCommand {
  static let configuration = CommandConfiguration(abstract: "Ping a node by name, UUID, or host:port.")
  @OptionGroup var options: LocalOptions
  @Argument var target: String

  func run() async throws {
    try printPong(try await NodeControl.request(.ping(target), socket: options.socket), json: options.json)
  }
}

struct Join: AsyncParsableCommand {
  static let configuration = CommandConfiguration(abstract: "Save a seed in the config and register with it.")
  @OptionGroup var options: LocalOptions
  @Argument var address: String

  func run() async throws {
    try printPong(
      try await NodeControl.request(.join(PeerAddress(address)), socket: options.socket), json: options.json)
  }
}

struct Reload: AsyncParsableCommand {
  static let configuration = CommandConfiguration(abstract: "Validate and reload the daemon configuration now.")
  @OptionGroup var options: LocalOptions

  func run() async throws {
    guard case .status(let status) = try await NodeControl.request(.reload, socket: options.socket) else {
      throw IndraError("Unexpected daemon response")
    }
    try printStatus(status, json: options.json)
  }
}

private func printJSON<Value: Encodable>(_ value: Value) throws {
  let encoder = JSONEncoder()
  encoder.outputFormatting = [.prettyPrinted, .sortedKeys, .withoutEscapingSlashes]
  print(String(decoding: try encoder.encode(value), as: UTF8.self))
}

private func printStatus(_ status: NodeStatus, json: Bool) throws {
  if json {
    try printJSON(status)
    return
  }
  print("\(status.node.name) (\(status.node.id)) cluster=\(status.cluster)")
  print("Address: \(PeerAddress(host: status.node.endpoint.host, port: status.node.endpoint.port))")
  print("Peers: \(status.members.count); seeds: \(status.seeds.map(\.description).joined(separator: ", "))")
  if let error = status.configurationError { print("Configuration: \(error)") }
  for (address, error) in status.discoveryErrors.sorted(by: { $0.key < $1.key }) {
    print("Discovery \(address): \(error)")
  }
}

private func printPong(_ reply: ControlReply, json: Bool) throws {
  guard case .pong(let pong) = reply else { throw IndraError("Unexpected daemon response") }
  if json {
    try printJSON(pong)
    return
  }
  let address = PeerAddress(host: pong.node.endpoint.host, port: pong.node.endpoint.port)
  print(
    "pong \(pong.node.name) (\(pong.node.id)) at \(address) \(pong.milliseconds.formatted(.number.precision(.fractionLength(2)))) ms"
  )
}
