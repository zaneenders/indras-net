import Slate

final class MonitorVimField {
  let input: TextInput
  private(set) var inserting: Bool
  private var pending: InputKey?

  init(_ text: String, capacity: Int, inserting: Bool) throws(TextInputError) {
    input = try TextInput(text, capacity: capacity)
    self.inserting = inserting
  }

  func beginInsert() {
    inserting = true
    pending = nil
  }

  func handle(_ event: InputKey, modifiers: KeyModifiers, copyBuffer: inout CopyBuffer) -> TextInputAction {
    let modifiers = modifiers.withoutLocks
    guard modifiers.isEmpty || modifiers == .shift else { return .ignored }
    var key = event
    if modifiers == .shift {
      if key == .character("i") { key = .character("I") }
      if key == .character("a") { key = .character("A") }
    }
    if case .control = key { return .ignored }
    if key == .escape && inserting {
      inserting = false
      if input.cursorByteOffset > 0 {
        _ = input.handle(.left, modifiers: [], copyBuffer: &copyBuffer)
      }
      return .changed
    }
    if inserting {
      return input.handle(key, modifiers: modifiers, copyBuffer: &copyBuffer)
    }
    let previous = pending
    pending = nil
    switch key {
    case .character("i"), .insert:
      beginInsert()
      return .changed
    case .character("a"), .character("A"), .character("I"):
      let motion: InputKey = key == .character("a") ? .right : key == .character("A") ? .end : .home
      _ = input.handle(motion, modifiers: [], copyBuffer: &copyBuffer)
      beginInsert()
      return .changed
    case .character("c"), .character("d"):
      guard previous == key else {
        pending = key
        return .changed
      }
      input.selectAll()
      _ = input.handle(.delete, modifiers: [], copyBuffer: &copyBuffer)
      if key == .character("c") { beginInsert() }
      return .changed
    case .character("h"), .left:
      return input.handle(.left, modifiers: [], copyBuffer: &copyBuffer)
    case .character("l"), .right, .character("$"), .end:
      let motion: InputKey = key == .right || key == .character("l") ? .right : .end
      _ = input.handle(motion, modifiers: [], copyBuffer: &copyBuffer)
    case .character("0"), .home:
      return input.handle(.home, modifiers: [], copyBuffer: &copyBuffer)
    case .character("x"), .delete:
      _ = input.handle(.delete, modifiers: [], copyBuffer: &copyBuffer)
    case .enter: return .submit
    default: return .ignored
    }
    if input.byteCount > 0, input.cursorByteOffset == input.byteCount {
      _ = input.handle(.left, modifiers: [], copyBuffer: &copyBuffer)
    }
    return .changed
  }
}

enum MonitorWriteAction {
  case ignored, redraw, edited, submit, dismiss
  case rejected(String)

  static let invalidField = Self.rejected("The field is too long or contains a control character.")
}
