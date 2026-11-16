import ArgumentParser
import Configuration
import NIOCore

@main
struct IndrasCounter: AsyncParsableCommand {
  static let configuration = CommandConfiguration(
    commandName: "indras-counter", subcommands: [Serve.self, Check.self])
}

struct Serve: AsyncParsableCommand {
  @OptionGroup var options: CounterOptions
  @Option var port: Int?
  @Option var lifetimeSeconds: Int?

  mutating func run() async throws {
    let config = try await options.reader(
      overrides: [
        "counter.port": port.map { ConfigValue(.int($0), isSecret: false) },
        "counter.lifetimeSeconds": lifetimeSeconds.map { ConfigValue(.int($0), isSecret: false) }
      ].compactMapValues { $0 })
    let settings = try CounterSettings(config: config)
    let port = try config.requiredInt(forKey: "port")
    let lifetime = try config.requiredInt(forKey: "lifetimeSeconds")
    guard (0...65535).contains(port), lifetime > 0 else {
      throw ValidationError("Port must be in 0...65535 and lifetime must be positive.")
    }
    try await CounterDemo.serve(
      host: settings.host, port: port,
      callTimeout: settings.callTimeout, connectTimeout: settings.connectTimeout, lifetime: .seconds(lifetime))
  }
}

struct Check: AsyncParsableCommand {
  @OptionGroup var options: CounterOptions
  @Option var peerHost: String?
  @Option var peerPort: Int?
  @Option var delayMilliseconds: Int?
  @Option var concurrentCalls: Int?

  mutating func run() async throws {
    let config = try await options.reader(
      overrides: [
        "counter.peerHost": peerHost.map { ConfigValue(.string($0), isSecret: false) },
        "counter.peerPort": peerPort.map { ConfigValue(.int($0), isSecret: false) },
        "counter.delayMilliseconds": delayMilliseconds.map { ConfigValue(.int($0), isSecret: false) },
        "counter.concurrentCalls": concurrentCalls.map { ConfigValue(.int($0), isSecret: false) }
      ].compactMapValues { $0 })
    let settings = try CounterSettings(config: config)
    let peerHost = try config.requiredString(forKey: "peerHost")
    let peerPort = try config.requiredInt(forKey: "peerPort")
    let delay = try config.requiredInt(forKey: "delayMilliseconds")
    let concurrentCalls = try config.requiredInt(forKey: "concurrentCalls")
    guard !peerHost.isEmpty, (1...65535).contains(peerPort),
      .milliseconds(delay) > settings.callTimeout, (1...1000).contains(concurrentCalls)
    else {
      throw ValidationError(
        "Supply a peer host/port, a delay longer than the call timeout, and 1...1000 concurrent calls.")
    }
    try await CounterDemo.check(
      host: settings.host, peerHost: peerHost, peerPort: peerPort, callTimeout: settings.callTimeout,
      connectTimeout: settings.connectTimeout, delayMilliseconds: delay,
      concurrentCalls: concurrentCalls)
  }
}
