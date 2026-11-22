public struct RequestIdentity: Codable, Hashable, Sendable {
  public let client: PeerId
  public let requestId: UInt64

  public init(client: PeerId, requestId: UInt64) {
    self.client = client
    self.requestId = requestId
  }
}
