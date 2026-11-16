import Foundation

package let destinationPathFeature = "destination-paths-v1"
package let createDestinationFeature = "create-destination-v1"

package func validDestinationPath(_ path: String) -> Bool {
  path.hasPrefix("/") && path.utf8.count <= 4096
    && !path.unicodeScalars.contains { CharacterSet.controlCharacters.contains($0) }
}

extension Manifest {
  package func uploadID(destination: String?) throws(any Error) -> String {
    guard let destination else { return try id }
    guard validDestinationPath(destination) else {
      throw TransferError("Destination must be an absolute directory on the receiver")
    }
    return try sha256(Data((id + "\0" + destination).utf8))
  }
}
