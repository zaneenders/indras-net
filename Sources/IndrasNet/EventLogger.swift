import Foundation

public struct EventLogger: Sendable {
  public var enabled: Bool
  let encoder = JSONEncoder()

  public init(enabled: Bool) {
    self.enabled = enabled
    encoder.outputFormatting = [.sortedKeys]
  }

  public func emit(_ event: IndrasNetEvent) {
    guard self.enabled else { return }
    guard let data = try? encoder.encode(event) else { return }
    var line = data
    line.append(0x0A)
    try? FileHandle.standardOutput.write(contentsOf: line)
  }
}
