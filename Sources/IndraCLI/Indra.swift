import ArgumentParser
import Foundation
import IndraHealth
import IndraMonitor
import IndraNode
import IndraRaft
import IndraSetu
import TCPActorSystem
import UnixSignals

@main
struct Indra: AsyncParsableCommand {
  static let configuration = CommandConfiguration(
    commandName: "indra", abstract: "Discover and communicate with Indra nodes.",
    version: BuildIdentity.current.summary,
    subcommands: [
      Initialize.self, Daemon.self, Status.self, Nodes.self, Monitor.self, Services.self, Health.self, Ping.self,
      Join.self, Reload.self, SetuCommand.self, RaftCommand.self
    ])
}

struct LocalOptions: ParsableArguments {
  @Option(help: "Private daemon control socket (or INDRA_SOCKET).") var socket: String?
  @Flag(help: "Print JSON.") var json = false
}

struct Initialize: AsyncParsableCommand {
  static let configuration = CommandConfiguration(commandName: "init", abstract: "Create a node configuration.")
  @Option(help: "Configuration file.") var config: String?
  @Option(help: "Node name.") var name = ProcessInfo.processInfo.hostName
  @Option(help: "Cluster name.") var cluster = "indra"
  @Option(help: "Address to bind.") var listenHost = "127.0.0.1"
  @Option(help: "Address other nodes can reach.") var advertisedHost = "127.0.0.1"
  @Option(help: "TCP port.") var port = 7331
  @Option(help: "Seed host:port; repeat for multiple seeds.") var seed: [String] = []

  func run() async throws {
    let configuration = NodeConfiguration(
      version: 1, id: UUID(), name: name, cluster: cluster, listenHost: listenHost, advertisedHost: advertisedHost,
      port: port, seeds: try seed.map { try PeerAddress($0) }, probeIntervalMilliseconds: 3000,
      requestTimeoutMilliseconds: 2000)
    let config = try await NodePaths.configuration(override: config)
    try await configuration.save(path: config, overwrite: false)
    print("Created \(config)")
  }
}

struct Daemon: AsyncParsableCommand {
  static let configuration = CommandConfiguration(
    abstract: "Run the node in the foreground for launchd/systemd.")
  @Option(help: "Configuration file.") var config: String?
  @Option(help: "Private control socket (or INDRA_SOCKET).") var socket: String?
  @OptionGroup var setu: SetuOptions
  @OptionGroup var raft: RaftOptions

  func run() async throws {
    let config = try await NodePaths.configuration(override: config)
    let socket = try await NodePaths.socket(override: socket)
    let setuServices = try setu.installer().map { [$0] } ?? []
    let services: [ServiceInstaller] = [HealthService.install, try raft.installer()] + setuServices
    let signals = await UnixSignalsSequence(trapping: [.sigint, .sigterm])
    do {
      try await withThrowingTaskGroup(of: Void.self) { tasks in
        tasks.addTask {
          try await runNode(configurationPath: config, socket: socket, services: services) { status in
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
    guard
      case .status(let status) = try await NodeControl.request(
        .status, socket: NodePaths.socket(override: options.socket), timeout: .seconds(65))
    else {
      throw IndraError("Unexpected daemon response")
    }
    try printStatus(status, json: options.json)
  }
}

struct Nodes: AsyncParsableCommand {
  static let configuration = CommandConfiguration(
    abstract: "List cluster members and their SWIM states.")
  @OptionGroup var options: LocalOptions

  func run() async throws {
    guard
      case .status(let status) = try await NodeControl.request(
        .status, socket: NodePaths.socket(override: options.socket), timeout: .seconds(65))
    else {
      throw IndraError("Unexpected daemon response")
    }
    if options.json {
      try printJSON(status)
      return
    }
    print("ID  NAME  ADDRESS  MEMBERSHIP  BUILD")
    print(
      "\(status.node.id)  \(status.node.name)  \(status.node.endpoint.host):\(status.node.endpoint.port)  local  \(status.node.build?.summary ?? "unknown")"
    )
    for member in status.members {
      let node = member.node
      let address = PeerAddress(host: node.endpoint.host, port: node.endpoint.port)
      print("\(node.id)  \(node.name)  \(address)  \(member.state.rawValue)  \(node.build?.summary ?? "unknown")")
    }
  }
}

struct Monitor: AsyncParsableCommand {
  static let configuration = CommandConfiguration(abstract: "Monitor live SWIM membership and probe traffic.")
  @OptionGroup var options: LocalOptions

  func run() async throws {
    let socket = try await NodePaths.socket(override: options.socket)
    if options.json {
      guard
        case .monitor(let snapshot) = try await NodeControl.request(
          .monitor, socket: socket, timeout: .seconds(65))
      else {
        throw IndraError("Unexpected daemon response")
      }
      try printJSON(snapshot)
    } else {
      try await runMonitor(socket: socket)
    }
  }
}

struct Services: AsyncParsableCommand {
  static let configuration = CommandConfiguration(abstract: "List available actors and their hosting nodes.")
  @OptionGroup var options: LocalOptions

  func run() async throws {
    guard
      case .services(let services) = try await NodeControl.request(
        .services, socket: NodePaths.socket(override: options.socket), timeout: .seconds(65))
    else {
      throw IndraError("Unexpected daemon response")
    }
    if options.json {
      try printJSON(services)
      return
    }
    print("KEY  TYPE  NODE  ADDRESS  ACTOR")
    for service in services {
      let registration = service.registration
      let endpoint = registration.address.endpoint
      let address = PeerAddress(host: endpoint.host, port: endpoint.port)
      print(
        "\(registration.key)  \(registration.actorType)  \(service.node.name)  \(address)  \(registration.address.id)")
    }
  }
}

struct Health: AsyncParsableCommand {
  static let configuration = CommandConfiguration(
    abstract: "Check daemon uptime and free disk space across the cluster.")
  @OptionGroup var options: LocalOptions

  func run() async throws {
    let reports = try await NodeControl.query(
      HealthService.key.name, returning: HealthMetrics.self, socket: NodePaths.socket(override: options.socket),
      timeout: .seconds(65))
    if options.json {
      try printJSON(reports)
      return
    }
    print("NAME  ADDRESS  HEALTH")
    for report in reports {
      let node = report.node
      let address = PeerAddress(host: node.endpoint.host, port: node.endpoint.port)
      let summary: String
      switch report.outcome {
      case .responded(let metrics):
        let uptime = Duration.seconds(metrics.uptimeSeconds).formatted(
          .units(allowed: [.days, .hours, .minutes, .seconds], width: .abbreviated, maximumUnitCount: 2))
        let space = metrics.freeDiskBytes.formatted(.byteCount(style: .file))
        summary = "\(uptime) up, \(space) free"
      case .timedOut: summary = "Timed out"
      case .unreachable: summary = "Unreachable"
      case .unavailable: summary = "No health service advertised"
      case .failed(let message): summary = "Failed (\(message))"
      }
      print("\(node.name)  \(address)  \(summary)")
    }
  }
}

struct Ping: AsyncParsableCommand {
  static let configuration = CommandConfiguration(abstract: "Ping a node by name, UUID, or host:port.")
  @OptionGroup var options: LocalOptions
  @Argument var target: String

  func run() async throws {
    try printPong(
      try await NodeControl.request(
        .ping(target), socket: NodePaths.socket(override: options.socket), timeout: .seconds(65)), json: options.json)
  }
}

struct Join: AsyncParsableCommand {
  static let configuration = CommandConfiguration(abstract: "Save a seed in the config and register with it.")
  @OptionGroup var options: LocalOptions
  @Argument var address: String

  func run() async throws {
    try printPong(
      try await NodeControl.request(
        .join(PeerAddress(address)), socket: NodePaths.socket(override: options.socket), timeout: .seconds(65)),
      json: options.json)
  }
}

struct Reload: AsyncParsableCommand {
  static let configuration = CommandConfiguration(abstract: "Validate and reload the daemon configuration now.")
  @OptionGroup var options: LocalOptions

  func run() async throws {
    guard
      case .status(let status) = try await NodeControl.request(
        .reload, socket: NodePaths.socket(override: options.socket), timeout: .seconds(65))
    else {
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
  print("Build: \(status.node.build?.summary ?? "unknown")")
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
