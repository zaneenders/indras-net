public import Foundation

public let defaultDownloadRecoveryBytes: Int64 = 1_073_741_824

public struct DownloadRecovery: Codable, Sendable {
  public let id: String
  public let expiresAt: Double
  public let sha256: String
  public let size: Int64
  package init(id: String, expiresAt: Double, sha256: String, size: Int64) {
    self.id = id
    self.expiresAt = expiresAt
    self.sha256 = sha256
    self.size = size
  }
}

public struct DownloadPublicationError: Error, LocalizedError, Sendable {
  public let recovery: DownloadRecovery
  public let destination: String
  public let message: String
  public var errorDescription: String? { message }
  package init(recovery: DownloadRecovery, destination: String, message: String) {
    self.recovery = recovery
    self.destination = destination
    self.message = message
  }
}
