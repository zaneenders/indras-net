import Foundation

public struct TransferReceipt: Codable, Sendable {
  public let sha256: String
  public let size: Int64
  public let destination: String?
  public let peer: String

  package init(manifest: Manifest, destination: String?, peer: String) {
    sha256 = manifest.digest
    size = manifest.size
    self.destination = destination
    self.peer = peer
  }
}
