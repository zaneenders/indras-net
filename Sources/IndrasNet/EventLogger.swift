import Foundation

struct EventLogger: Sendable {
  var enabled: Bool
  let encoder = JSONEncoder()

  init(enabled: Bool) {
    self.enabled = enabled
    encoder.outputFormatting = [.sortedKeys]
  }

  func emit(_ event: IndrasNetEvent) {
    guard self.enabled else { return }
    guard let data = try? encoder.encode(event) else { return }
    var line = data
    line.append(0x0A)
    try? FileHandle.standardOutput.write(contentsOf: line)
  }
}
