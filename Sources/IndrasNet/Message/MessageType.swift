struct MessageType: RawRepresentable, Hashable, Sendable {
  let rawValue: UInt16

  init(rawValue: UInt16) {
    self.rawValue = rawValue
  }
}
