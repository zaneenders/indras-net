import ArgumentParser
import Configuration
import NIOCore
import SystemPackage

struct CounterOptions: ParsableArguments {
  @Option(help: "JSON configuration file; CLI options override COUNTER_* environment variables, then JSON.")
  var config: String?
  @Option var host: String?
  @Option var callTimeoutMilliseconds: Int?
  @Option var connectTimeoutMilliseconds: Int?

  func reader(overrides: [AbsoluteConfigKey: ConfigValue]) async throws -> ConfigReader {
    var values = overrides
    values["counter.host"] = host.map { ConfigValue(.string($0), isSecret: false) }
    values["counter.callTimeoutMilliseconds"] = callTimeoutMilliseconds.map { ConfigValue(.int($0), isSecret: false) }
    values["counter.connectTimeoutMilliseconds"] = connectTimeoutMilliseconds.map {
      ConfigValue(.int($0), isSecret: false)
    }
    var providers: [any ConfigProvider] = [
      InMemoryProvider(name: "command-line", values: values),
      EnvironmentVariablesProvider(secretsSpecifier: .none, bytesDecoder: .base64, arraySeparator: ",")
    ]
    if let config {
      providers.append(
        try await FileProvider<JSONSnapshot>(
          snapshotType: JSONSnapshot.self, parsingOptions: .default, filePath: FilePath(config), allowMissing: false))
    }
    return ConfigReader(providers: providers, accessReporter: nil).scoped(to: "counter")
  }
}

struct CounterSettings {
  let host: String
  let callTimeout: Duration
  let connectTimeout: TimeAmount

  init(config: ConfigReader) throws {
    host = try config.requiredString(forKey: "host")
    let callMilliseconds = try config.requiredInt(forKey: "callTimeoutMilliseconds")
    let connectMilliseconds = try config.requiredInt(forKey: "connectTimeoutMilliseconds")
    guard !host.isEmpty, callMilliseconds > 0,
      (1...Int(Int64.max / 1_000_000)).contains(connectMilliseconds)
    else {
      throw ValidationError("Host is required; timeouts must be positive and fit in nanoseconds.")
    }
    callTimeout = .milliseconds(callMilliseconds)
    connectTimeout = .milliseconds(Int64(connectMilliseconds))
  }
}
