public struct NodeTiming: Sendable {
  public let heartbeatInterval: Duration
  public let electionTimeoutRange: Range<Int64>

  public static let `default` = NodeTiming(
    heartbeatIntervalMs: 50,
    electionTimeoutMinMs: 150,
    electionTimeoutMaxMs: 300
  )

  public init(heartbeatIntervalMs: Int64, electionTimeoutMinMs: Int64, electionTimeoutMaxMs: Int64) {
    heartbeatInterval = .milliseconds(heartbeatIntervalMs)
    electionTimeoutRange = electionTimeoutMinMs..<electionTimeoutMaxMs
  }
}
