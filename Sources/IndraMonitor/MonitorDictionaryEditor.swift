import BTree
import Foundation
import IndraRaft
import Slate

final class MonitorDictionaryEditor: CanvasDrawing {
  static let capacity = 16 * 1024

  private enum Kind: String, CaseIterable {
    case text = "Text"
    case number = "Number"
    case boolean = "Boolean"
    case null = "Null"
    case object = "Object"
    case array = "Array"

    init(_ value: AttributeValue) {
      switch value {
      case .string, .bytes: self = .text
      case .int64, .uint64, .float32, .float64: self = .number
      case .bool: self = .boolean
      case .null: self = .null
      case .map: self = .object
      case .list: self = .array
      }
    }

    var initial: AttributeValue {
      switch self {
      case .text: .string("")
      case .number: .int64(0)
      case .boolean: .bool(false)
      case .null: .null
      case .object: .map([:])
      case .array: .list([])
      }
    }

    var hint: String {
      switch self {
      case .text: "e.g. hello"
      case .number: "e.g. 42 or 3.5"
      case .boolean: "True or false"
      case .null: "An empty value"
      case .object: "A group of named fields"
      case .array: "An ordered list"
      }
    }
  }

  private struct Field {
    var name: String
    var value: AttributeValue
  }

  private struct Level {
    let label: String
    let isArray: Bool
    var fields: [Field]
    var selection = 0
    var selected: Field? { fields.indices.contains(selection) ? fields[selection] : nil }

    var value: AttributeValue {
      if isArray { return .list(fields.map(\.value)) }
      return .map(Dictionary(uniqueKeysWithValues: fields.map { ($0.name, $0.value) }))
    }
  }

  private struct TypeChoice {
    let adding: String?
    var index: Int
    var kind: Kind { Kind.allCases[index] }
  }

  private enum Interaction {
    case browse
    case name(MonitorVimField, adding: Bool)
    case value(MonitorVimField, kind: Kind, replacing: Bool)
    case type(TypeChoice)
  }

  private var levels = [Level(label: "Dictionary", isArray: false, fields: [], selection: 0)]
  private var interaction = Interaction.browse
  private var pendingKey: InputKey?
  private var undo: [[Level]] = []
  private var redo: [[Level]] = []
  var focused = false

  private var index: Int { levels.count - 1 }
  private var level: Level { levels[index] }

  var isBrowsing: Bool {
    if case .browse = interaction { return true }
    return false
  }

  var preferredHeight: Int {
    if case .type = interaction { return Kind.allCases.count + 1 }
    return max(3, level.fields.count + (levels.count > 1 ? 3 : 2))
  }

  var controls: String {
    switch interaction {
    case .browse:
      guard let selection = level.selected else {
        return "Enter Add \(level.isArray ? "item" : "field")   :w Write   :q Close"
      }
      let edit: String
      switch Kind(selection.value) {
      case .array, .object: edit = "Enter Open"
      case .boolean: edit = "Space Toggle"
      case .text, .number: edit = "i Edit"
      case .null: edit = ""
      }
      return ["j/k Move", edit, "a Add", "t Type", ":w Write", ":q Close"].filter { !$0.isEmpty }.joined(
        separator: "  ")
    case .type:
      return "j/k Choose type   Enter Confirm   Esc Cancel"
    case .name(let field, let adding):
      if !field.inserting { return "i Edit name   Enter Confirm   q Cancel" }
      return adding ? "Enter Choose field type   Esc Normal mode" : "Enter Confirm name   Esc Normal mode"
    case .value(let field, _, _):
      return field.inserting ? "Enter Confirm value   Esc Normal mode" : "i Edit value   Enter Confirm   q Cancel"
    }
  }

  var document: RaftValue {
    var value = level.value
    for var ancestor in levels.dropLast().reversed() {
      ancestor.fields[ancestor.selection].value = value
      value = ancestor.value
    }
    guard case .map(let document) = value else { preconditionFailure("A dictionary has an object at its root.") }
    return .document(document)
  }

  func leaveEditing() {
    interaction = .browse
    pendingKey = nil
  }

  func handle(_ event: InputKey, modifiers: KeyModifiers, copyBuffer: inout CopyBuffer) -> MonitorWriteAction {
    var key = event
    let modifiers = modifiers.withoutLocks
    if modifiers == .control && key == .character("r") { key = .control(.r) }
    if modifiers == .shift && key == .character("g") { key = .character("G") }
    switch interaction {
    case .browse:
      guard modifiers.isEmpty || modifiers == .shift || key == .control(.r) else { return .ignored }
      return browse(key)
    case .type(var choice):
      guard modifiers.isEmpty else { return .ignored }
      switch key {
      case .escape: leaveEditing()
      case .enter: return apply(choice)
      case .up, .character("k"), .left, .character("h"):
        choice.index = max(0, choice.index - 1)
        interaction = .type(choice)
      case .down, .character("j"), .right, .character("l"):
        choice.index = min(Kind.allCases.count - 1, choice.index + 1)
        interaction = .type(choice)
      default: return .ignored
      }
      return .redraw
    case .name(let field, _), .value(let field, _, _):
      if !field.inserting, key == .character("q"), modifiers.isEmpty {
        leaveEditing()
        return .redraw
      }
      switch field.handle(key, modifiers: modifiers, copyBuffer: &copyBuffer) {
      case .ignored: return .ignored
      case .changed: return .redraw
      case .rejected: return .invalidField
      case .submit: return applyInput(field.input.snapshot())
      }
    }
  }

  func paste(_ text: UTF8Span) -> MonitorWriteAction {
    switch interaction {
    case .browse:
      guard levels.count == 1 else { return .rejected("Return to the root dictionary to import fields.") }
      guard case .document(let imported) = try? RaftValue(documentJSON: String(copying: text)) else {
        return .rejected("Paste an object to import its fields.")
      }
      let fields = Self.fields(in: imported)
      let existing = Set(level.fields.map(\.name))
      if let duplicate = fields.first(where: { existing.contains($0.name) }) {
        return .rejected("There is already a field named \(Self.preview(duplicate.name)).")
      }
      return change { $0.fields.append(contentsOf: fields) }
    case .name(let field, _), .value(let field, _, _):
      guard field.inserting else { return .ignored }
      let text = String(copying: text)
      guard !text.contains(where: \.isNewline), !text.contains("\t") else { return .rejected("Paste one line.") }
      do { try field.input.insert(text.utf8Span) } catch {
        return .invalidField
      }
      return .redraw
    case .type: return .ignored
    }
  }

  private func browse(_ key: InputKey) -> MonitorWriteAction {
    let pending = pendingKey
    pendingKey = nil
    switch key {
    case .character("d"):
      guard level.selected != nil else { return .ignored }
      if pending == key {
        return change {
          $0.fields.remove(at: $0.selection)
          $0.selection = min($0.selection, $0.fields.count)
        }
      }
      pendingKey = key
    case .character("g"):
      if pending == key { move(to: 0) } else { pendingKey = key }
    case .character("G"), .end: move(to: level.fields.count)
    case .home: move(to: 0)
    case .character("j"), .down: move(to: level.selection + 1)
    case .character("k"), .up: move(to: level.selection - 1)
    case .character("h"), .left:
      if levels.count > 1 {
        let child = levels.removeLast()
        let selection = level.selection
        levels[index].fields[selection].value = child.value
      }
    case .character("l"), .right, .enter, .character("i"): return activate(key)
    case .character("a"): return addField()
    case .character("r"):
      guard !level.isArray, let field = level.selected,
        let input = try? MonitorVimField(field.name, capacity: Self.capacity, inserting: true)
      else { return .ignored }
      interaction = .name(input, adding: false)
    case .character("t"):
      guard let field = level.selected else { return .ignored }
      chooseType(adding: nil, startingAt: Kind(field.value))
    case .character(" "):
      guard case .bool(let value) = level.selected?.value else { return .ignored }
      return change { $0.fields[$0.selection].value = .bool(!value) }
    case .character("u"):
      guard let previous = undo.popLast() else { return .ignored }
      redo.append(levels)
      levels = previous
      return .edited
    case .control(.r):
      guard let next = redo.popLast() else { return .ignored }
      undo.append(levels)
      levels = next
      return .edited
    case .escape: break
    default: return .ignored
    }
    return .redraw
  }

  private func move(to row: Int) {
    levels[index].selection = max(0, min(level.fields.count, row))
  }

  private func addField() -> MonitorWriteAction {
    move(to: level.fields.count)
    if level.isArray {
      chooseType(adding: "", startingAt: .text)
    } else if let field = try? MonitorVimField("", capacity: Self.capacity, inserting: true) {
      interaction = .name(field, adding: true)
    }
    return .redraw
  }

  private func activate(_ key: InputKey) -> MonitorWriteAction {
    guard let field = level.selected else {
      return key == .enter || key == .character("i") ? addField() : .ignored
    }
    let label = level.isArray ? "[\(level.selection)]" : Self.preview(field.name)
    switch field.value {
    case .map(let values):
      levels.append(Level(label: label, isArray: false, fields: Self.fields(in: values), selection: 0))
    case .list(let values):
      levels.append(Level(label: label, isArray: true, fields: values.map { Field(name: "", value: $0) }, selection: 0))
    default:
      guard key != .character("l"), key != .right else { return .ignored }
      if case .bool(let value) = field.value {
        return change { $0.fields[$0.selection].value = .bool(!value) }
      }
      guard editValue(field.value, selectAll: false) else { return .ignored }
    }
    return .redraw
  }

  private func editValue(_ value: AttributeValue, selectAll: Bool) -> Bool {
    let kind = Kind(value)
    guard kind == .text || kind == .number else { return false }
    let text = Self.label(value)
    let replacing = text.unicodeScalars.contains { $0.properties.generalCategory == .control }
    guard let field = try? MonitorVimField(replacing ? "" : text, capacity: Self.capacity, inserting: true)
    else { return false }
    if selectAll { field.input.selectAll() }
    interaction = .value(field, kind: kind, replacing: replacing)
    return true
  }

  private func chooseType(adding: String?, startingAt kind: Kind) {
    interaction = .type(TypeChoice(adding: adding, index: Kind.allCases.firstIndex(of: kind)!))
  }

  private func apply(_ choice: TypeChoice) -> MonitorWriteAction {
    let kind = choice.kind
    let adding = choice.adding
    if adding == nil, let value = level.selected?.value, Kind(value) == kind {
      leaveEditing()
      return .redraw
    }
    let action = change {
      if let adding {
        $0.fields.append(Field(name: adding, value: kind.initial))
        $0.selection = $0.fields.count - 1
      } else {
        $0.fields[$0.selection].value = kind.initial
      }
    }
    if case .edited = action {
      leaveEditing()
      _ = editValue(kind.initial, selectAll: true)
    }
    return action
  }

  private func applyInput(_ input: String) -> MonitorWriteAction {
    switch interaction {
    case .name(_, let adding):
      guard !input.isEmpty else { return .rejected("Enter a field name.") }
      let existing = level.fields.firstIndex { $0.name == input }
      if let existing, adding || existing != level.selection { return .rejected("That field name is already in use.") }
      if adding {
        chooseType(adding: input, startingAt: .text)
        return .redraw
      }
      let action = change { $0.fields[$0.selection].name = input }
      if case .edited = action { leaveEditing() }
      return action
    case .value(_, let kind, _):
      let value: AttributeValue
      if kind == .text {
        value = .string(input)
      } else if let integer = Int64(input) {
        value = .int64(integer)
      } else if let integer = UInt64(input) {
        value = .uint64(integer)
      } else if let number = Double(input), number.isFinite {
        value = .float64(number)
      } else {
        return .rejected("Enter a finite number.")
      }
      let action = change { $0.fields[$0.selection].value = value }
      if case .edited = action { leaveEditing() }
      return action
    case .browse, .type: return .ignored
    }
  }

  private func change(_ update: (inout Level) -> Void) -> MonitorWriteAction {
    let before = levels
    update(&levels[index])
    guard let compact = try? document.formatted(asJSON: true), compact.utf8.count <= Self.capacity else {
      levels = before
      return .rejected("Dictionary exceeds 16 KiB.")
    }
    if undo.count == 32 { undo.removeFirst() }
    undo.append(before)
    redo.removeAll()
    return .edited
  }

  private static func fields(in document: Document) -> [Field] {
    document.sorted { $0.key < $1.key }.map { Field(name: $0.key, value: $0.value) }
  }

  private static func preview(_ value: String) -> String {
    var text = ""
    for scalar in value.unicodeScalars.prefix(LayoutLimits.maximumTextScalars) {
      if scalar == "\n" || scalar == "\r" {
        text.append("↵")
      } else if scalar == "\t" {
        text.append("⇥")
      } else if scalar.properties.generalCategory == .control {
        text.append("�")
      } else {
        text.unicodeScalars.append(scalar)
      }
    }
    return text
  }

  private static func label(_ value: AttributeValue) -> String {
    switch value {
    case .string(let text): text
    case .bool(let flag): flag ? "☑ true" : "☐ false"
    case .null: "null"
    case .int64(let number): String(number)
    case .uint64(let number): String(number)
    case .float32(let number): String(number)
    case .float64(let number): String(number)
    case .bytes(let bytes): bytes.base64EncodedString()
    case .map(let fields): "\(fields.count) fields  ›"
    case .list(let items): "\(items.count) items  ›"
    }
  }
}

extension MonitorDictionaryEditor {
  func paint(in frame: inout Frame) {
    let width = frame.area.width
    let height = frame.area.height
    guard width >= 36, height >= 3 else { return }
    let base = style(.rgb(211, 221, 230), background: .rgb(11, 16, 22))
    let bar = style(.rgb(125, 145, 161), background: .rgb(20, 28, 37))
    let selected = style(.rgb(226, 245, 249), background: .rgb(28, 57, 73))
    frame.fill(frame.area, with: Cell(style: base))

    if case .type(let choice) = interaction {
      let name = choice.adding ?? level.selected?.name ?? ""
      frame.fill(Rect(width: width, height: 1), with: Cell(style: bar))
      frame.text(
        name.isEmpty ? "  Choose an item type" : "  Choose a type for \(Self.preview(name))", in: frame.area, style: bar
      )
      let count = min(height - 1, Kind.allCases.count)
      let start = min(Kind.allCases.count - count, max(0, choice.index - count + 1))
      for (row, kind) in Kind.allCases[start..<(start + count)].enumerated() {
        let active = start + row == choice.index
        let area = Rect(y: row + 1, width: width, height: 1)
        let rowStyle = active ? selected : base
        frame.fill(area, with: Cell(style: rowStyle))
        frame.text((active ? "› " : "  ") + kind.rawValue, in: area, style: rowStyle)
        frame.text(kind.hint, in: Rect(x: 14, y: row + 1, width: width - 14, height: 1), style: rowStyle)
      }
      return
    }

    let tableTop = levels.count > 1 ? 1 : 0
    if tableTop > 0 {
      let path = levels.map(\.label).joined(separator: "  ›  ")
      let navigation = "h Up one level"
      let navColumn = width - navigation.count - 1
      frame.fill(Rect(width: width, height: 1), with: Cell(style: bar))
      frame.text("  \(path)", in: Rect(width: navColumn - 1, height: 1), style: bar)
      if focused && isBrowsing {
        frame.text(navigation, in: Rect(x: navColumn, width: navigation.count, height: 1), style: bar)
      }
    }
    let nameWidth = min(32, width / 3)
    let valueColumn = 2 + nameWidth + 10
    func columns(row: Int) -> (name: Rect, type: Rect, value: Rect) {
      (
        Rect(x: 2, y: row, width: nameWidth - 1, height: 1),
        Rect(x: 2 + nameWidth, y: row, width: 9, height: 1),
        Rect(x: valueColumn, y: row, width: width - valueColumn, height: 1)
      )
    }
    let header = columns(row: tableTop)
    frame.fill(Rect(y: tableTop, width: width, height: 1), with: Cell(style: bar))
    frame.text(level.isArray ? "Item" : "Field name", in: header.name, style: bar)
    frame.text("Type", in: header.type, style: bar)
    frame.text("Value", in: header.value, style: bar)
    let visible = height - 1 - tableTop
    let last = level.fields.count
    let start = max(0, min(last + 1 - visible, level.selection - visible / 2))
    for fieldIndex in start..<min(last + 1, start + visible) {
      let y = tableTop + 1 + fieldIndex - start
      let active = focused && fieldIndex == level.selection
      let rowStyle = active ? selected : base
      let region = Rect(y: y, width: width, height: 1)
      frame.fill(region, with: Cell(style: rowStyle))
      if active { frame.text("›", in: region, style: selected) }
      let slots = columns(row: y)
      if fieldIndex == last {
        if active, case .name(let field, adding: true) = interaction {
          field.input.paint(
            in: slots.name, frame: &frame, focused: true, placeholder: "Type a name", style: rowStyle)
        } else {
          let label = level.isArray ? "+ Add item" : "+ Add field"
          frame.text(label, in: Rect(x: 2, y: y, width: width - 2, height: 1), style: rowStyle)
        }
        continue
      }
      let field = level.fields[fieldIndex]
      let name = level.isArray ? "[\(fieldIndex)]" : (field.name.isEmpty ? "\"\"" : Self.preview(field.name))
      if active, case .name(let editor, adding: false) = interaction {
        editor.input.paint(in: slots.name, frame: &frame, focused: true, placeholder: "Field name", style: rowStyle)
      } else {
        frame.text(name, in: slots.name, style: rowStyle)
      }
      frame.text(
        Kind(field.value).rawValue, in: slots.type, style: style(.rgb(105, 169, 232), background: rowStyle.background))
      if active, case .value(let editor, _, let replacing) = interaction {
        editor.input.paint(
          in: slots.value, frame: &frame, focused: true, placeholder: replacing ? "Replace text…" : "",
          style: rowStyle)
      } else {
        let value = Self.label(field.value)
        frame.text(value.isEmpty ? "\"\"" : Self.preview(value), in: slots.value, style: rowStyle)
      }
    }

  }

  private func style(_ color: Color, background: Color) -> Style {
    Style(foreground: color, background: background, bold: false, dim: false, underlined: false, inverse: false)
  }
}
