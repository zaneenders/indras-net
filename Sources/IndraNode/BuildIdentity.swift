public struct BuildIdentity: Codable, Hashable, Sendable {
  public let commit: String
  public let isDirty: Bool?

  public init(commit: String, isDirty: Bool?) {
    self.commit = commit
    self.isDirty = isDirty
  }

  public static let current = BuildIdentity(
    commit: EmbeddedBuildIdentity.fullHash, isDirty: EmbeddedBuildIdentity.isDirty)

  public var summary: String {
    guard commit != "unknown" else { return "unknown build" }
    let state: String
    switch isDirty {
    case true: state = "dirty"
    case false: state = "clean"
    case nil: state = "unknown"
    }
    return "\(commit.prefix(12)) \(state)"
  }

  func validate() throws {
    guard
      commit == "unknown"
        || ((commit.count == 40 || commit.count == 64)
          && commit.utf8.allSatisfy({ (48...57).contains($0) || (97...102).contains($0) }))
    else { throw IndraError("Invalid build commit") }
  }
}
