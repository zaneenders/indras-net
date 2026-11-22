public import Foundation

public struct LogEntry: Codable, Equatable, Sendable {
  public let term: Term
  public let command: Data
  public let request: RequestIdentity?
  public let isNoOp: Bool
  public let membership: ClusterMembership?

  public init(term: Term, command: Data, request: RequestIdentity?, isNoOp: Bool) {
    self.term = term
    self.command = command
    self.request = request
    self.isNoOp = isNoOp
    self.membership = nil
  }

  public init(term: Term, membership: ClusterMembership) {
    self.term = term
    self.command = Data()
    self.request = nil
    self.isNoOp = false
    self.membership = membership
  }

  private enum CodingKeys: String, CodingKey {
    case term, command, request, isNoOp, membership
  }

  public init(from decoder: any Decoder) throws {
    let container = try decoder.container(keyedBy: CodingKeys.self)
    term = try container.decode(Term.self, forKey: .term)
    command = try container.decode(Data.self, forKey: .command)
    request = try container.decodeIfPresent(RequestIdentity.self, forKey: .request)
    isNoOp = try container.decodeIfPresent(Bool.self, forKey: .isNoOp) ?? false
    membership = try container.decodeIfPresent(ClusterMembership.self, forKey: .membership)
  }

  public var encodedLength: Int {
    14 + command.count + (request.map { 12 + $0.client.utf8.count } ?? 0) + (membership?.encodedLength ?? 0)
  }
}
