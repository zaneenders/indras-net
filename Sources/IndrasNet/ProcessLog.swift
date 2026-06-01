import Foundation

enum ProcessLog {
  static func human(_ message: String) {
    if var data = message.data(using: .utf8) {
      data.append(0x0A)
      try? FileHandle.standardError.write(contentsOf: data)
    }
  }
}
