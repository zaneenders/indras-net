import Foundation

public struct ClusterConfig: Decodable, Sendable {
  public var peers: [NodeAddress]
  public var heartbeatIntervalMs: Int64
  public var electionTimeoutMinMs: Int64
  public var electionTimeoutMaxMs: Int64

  enum CodingKeys: String, CodingKey {
    case peers
    case heartbeatIntervalMs
    case electionTimeoutMinMs
    case electionTimeoutMaxMs
  }

  public init(from decoder: Decoder) throws {
    let container = try decoder.container(keyedBy: CodingKeys.self)
    peers = try container.decode([NodeAddress].self, forKey: .peers)
    heartbeatIntervalMs = try container.decodeIfPresent(Int64.self, forKey: .heartbeatIntervalMs) ?? 50
    electionTimeoutMinMs = try container.decodeIfPresent(Int64.self, forKey: .electionTimeoutMinMs) ?? 150
    electionTimeoutMaxMs = try container.decodeIfPresent(Int64.self, forKey: .electionTimeoutMaxMs) ?? 300
  }

  public var timing: NodeTiming {
    NodeTiming(
      heartbeatIntervalMs: heartbeatIntervalMs,
      electionTimeoutMinMs: electionTimeoutMinMs,
      electionTimeoutMaxMs: electionTimeoutMaxMs
    )
  }

  public func peers(excluding local: NodeAddress) -> [NodeAddress] {
    peers.filter { $0.host != local.host || $0.port != local.port }
  }

  public static func load(from path: String) throws -> ClusterConfig {
    let data = try Data(contentsOf: URL(fileURLWithPath: path))
    return try JSONDecoder().decode(ClusterConfig.self, from: data)
  }
}
