public import Foundation

public struct IndraError: Error, Codable, Sendable, CustomStringConvertible {
  public let description: String

  public init(_ description: String) { self.description = description }
}

public struct PeerAddress: Codable, Hashable, Sendable, CustomStringConvertible {
  public let host: String
  public let port: Int

  public init(host: String, port: Int) {
    self.host = host
    self.port = port
  }

  public init(_ text: String) throws {
    guard let separator = text.lastIndex(of: ":"), let port = Int(text[text.index(after: separator)...]) else {
      throw IndraError("Use host:port or [IPv6]:port for a peer address")
    }
    let host = String(text[..<separator])
    self.host = host.hasPrefix("[") && host.hasSuffix("]") ? String(host.dropFirst().dropLast()) : host
    self.port = port
    try validate()
  }

  public init(from decoder: any Decoder) throws {
    try self.init(decoder.singleValueContainer().decode(String.self))
  }

  public func encode(to encoder: any Encoder) throws {
    var container = encoder.singleValueContainer()
    try container.encode(description)
  }

  public var description: String { host.contains(":") ? "[\(host)]:\(port)" : "\(host):\(port)" }

  func validate() throws {
    guard validHost(host), !["0.0.0.0", "::"].contains(host), (1...65535).contains(port) else {
      throw IndraError("Peers require a reachable host and a port from 1 through 65535")
    }
  }
}

public struct NodeConfiguration: Codable, Sendable, Equatable {
  public var version: Int
  public var id: UUID
  public var name: String
  public var cluster: String
  public var listenHost: String
  public var advertisedHost: String
  public var port: Int
  public var seeds: [PeerAddress]
  public var probeIntervalMilliseconds: Int
  public var requestTimeoutMilliseconds: Int

  public init(
    version: Int, id: UUID, name: String, cluster: String, listenHost: String, advertisedHost: String,
    port: Int, seeds: [PeerAddress], probeIntervalMilliseconds: Int, requestTimeoutMilliseconds: Int
  ) {
    self.version = version
    self.id = id
    self.name = name
    self.cluster = cluster
    self.listenHost = listenHost
    self.advertisedHost = advertisedHost
    self.port = port
    self.seeds = seeds
    self.probeIntervalMilliseconds = probeIntervalMilliseconds
    self.requestTimeoutMilliseconds = requestTimeoutMilliseconds
  }

  public func validate() throws {
    guard version == 1 else { throw IndraError("Unsupported configuration version") }
    guard !name.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty,
      !cluster.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty,
      name.utf8.count <= 128, cluster.utf8.count <= 128
    else { throw IndraError("Node name and cluster must contain 1...128 bytes") }
    guard validHost(listenHost), validHost(advertisedHost), !["0.0.0.0", "::"].contains(advertisedHost),
      (0...65535).contains(port)
    else { throw IndraError("Supply valid listen/advertised hosts and a port from 0 through 65535") }
    guard (100...3_600_000).contains(probeIntervalMilliseconds),
      (50...30_000).contains(requestTimeoutMilliseconds), seeds.count <= 256,
      Set(seeds).count == seeds.count
    else { throw IndraError("Invalid intervals or seed list (maximum 256 unique seeds)") }
    for seed in seeds { try seed.validate() }
  }

  public static func load(path: String) async throws -> Self {
    let configuration = try await readNodeJSON(Self.self, from: path)
    try configuration.validate()
    return configuration
  }

  public func save(path: String, overwrite: Bool) async throws {
    try validate()
    try await writeNodeJSON(self, to: path, overwrite: overwrite)
  }

  func canReload(_ other: Self) -> Bool {
    version == other.version && id == other.id && cluster == other.cluster && listenHost == other.listenHost
      && advertisedHost == other.advertisedHost && port == other.port
      && probeIntervalMilliseconds == other.probeIntervalMilliseconds
      && requestTimeoutMilliseconds == other.requestTimeoutMilliseconds
  }
}

public enum NodePaths: Sendable {
  public static var configuration: String {
    base(variable: "XDG_CONFIG_HOME", fallback: "/.config") + "/indra/config.json"
  }

  public static var socket: String {
    let environment = ProcessInfo.processInfo.environment
    if let configured = environment["INDRA_SOCKET"], configured.hasPrefix("/") { return configured }
    if let runtime = environment["XDG_RUNTIME_DIR"], runtime.hasPrefix("/") {
      return runtime + "/indra/control.sock"
    }
    return base(variable: "XDG_STATE_HOME", fallback: "/.local/state") + "/indra/run/control.sock"
  }

  private static func base(variable: String, fallback: String) -> String {
    if let value = ProcessInfo.processInfo.environment[variable], value.hasPrefix("/") { return value }
    return FileManager.default.homeDirectoryForCurrentUser.path + fallback
  }
}

private func validHost(_ host: String) -> Bool {
  !host.isEmpty && host.utf8.count <= 253 && !host.contains(where: { $0.isWhitespace || $0 == "/" || $0 == "\0" })
}
