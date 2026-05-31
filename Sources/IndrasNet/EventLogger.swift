import Foundation

struct EventLogger: Sendable {
  var enabled: Bool
  let encoder = JSONEncoder()

  func emit(_ event: IndrasNetEvent) {
    guard self.enabled else { return }
    encoder.outputFormatting = [.sortedKeys]
    guard let data = try? encoder.encode(event) else { return }
    var line = data
    line.append(0x0A)
    try? FileHandle.standardOutput.write(contentsOf: line)
  }
}

public enum ProcessLog {
  public static func human(_ message: String) {
    if var data = message.data(using: .utf8) {
      data.append(0x0A)
      try? FileHandle.standardError.write(contentsOf: data)
    }
  }
}
