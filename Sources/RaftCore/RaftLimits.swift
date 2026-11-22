/// Size budget used by the current Raft replication and snapshot format.
public enum RaftLimits: Sendable {
  public static let maximumPayloadLength: UInt32 = UInt32(UInt16.max)
}
