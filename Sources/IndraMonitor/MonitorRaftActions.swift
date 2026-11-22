import Foundation
import IndraRaft
import Slate

enum MonitorRaftOverlay {
  case inspection(MonitorRaftInspection)
  case write(MonitorRaftWrite)
}

struct MonitorRaftInspection {
  enum Kind {
    case log, snapshot

    var firstQuery: RaftInspection.Query {
      switch self {
      case .log: .log(before: nil)
      case .snapshot: .snapshot(offset: 0, at: nil)
      }
    }
  }

  let kind: Kind
  let group: String
  let peer: String
  let name: String
  private var earlier: [RaftInspection.Query] = []
  private var query: RaftInspection.Query
  var pending: UUID?
  var reply: RaftInspectionReply?
  var failure: String?
  private(set) var textOffset = 0

  init(kind: Kind, group: String, peer: String, name: String) {
    self.kind = kind
    self.group = group
    self.peer = peer
    self.name = name
    self.query = kind.firstQuery
  }

  var request: RaftInspection {
    RaftInspection(peer: peer, group: group, query: query)
  }

  var title: String { "\(kind == .log ? "LOG" : "SNAPSHOT") / \(name)" }

  mutating func move(_ key: InputKey) -> Bool {
    guard pending == nil else { return false }
    switch key {
    case .pageUp:
      guard let previous = earlier.popLast() else { return false }
      query = previous
    case .pageDown:
      let next: RaftInspection.Query
      switch reply {
      case .log(let page):
        guard let before = page.olderBefore else { return false }
        next = .log(before: before)
      case .snapshot(let page?):
        guard let offset = page.nextOffset else { return false }
        next = .snapshot(offset: offset, at: page.index)
      default:
        return false
      }
      earlier.append(query)
      query = next
    case .character("r"), .character("R"):
      earlier.removeAll()
      query = kind.firstQuery
    default:
      return false
    }
    textOffset = 0
    return true
  }

  mutating func receive(_ reply: RaftInspectionReply) {
    self.reply = reply
    if case .log(let page) = reply, case .log(before: nil) = query {
      query = .log(before: page.lastIndex + 1)
    } else if case .snapshot(let page?) = reply, case .snapshot(let offset, _) = query {
      query = .snapshot(offset: offset, at: page.index)
    }
  }

  mutating func pan(_ distance: Int) {
    let last = max(0, (records.map(\.count).max() ?? 0) - 1)
    textOffset = min(last, max(0, textOffset + distance))
  }

  var records: [String] {
    switch reply {
    case .log(let page):
      return page.entries.map { entry in
        let state = entry.index <= page.appliedIndex ? "applied" :
          entry.index <= page.commitIndex ? "committed" : "uncommitted"
        let operation: String
        switch entry.operation {
        case .put(let key, let value): operation = "put \(String(reflecting: key)) = \(Self.label(value))"
        case .delete(let key): operation = "delete \(String(reflecting: key))"
        case .membership(let config):
          operation = "membership · \(config.voters.count) voters · \(config.readers.count) learners"
            + (config.previousVoters.map { " · joint with \($0.count) previous voters" } ?? "")
        case .noOp: operation = "no-op"
        }
        return "#\(entry.index)  term \(entry.term)  \(state)  \(operation)"
      }
    case .snapshot(let page?):
      return page.values.map { "\(String(reflecting: $0.key)) = \(Self.label($0.value))" }
    default:
      return []
    }
  }

  private static func label(_ value: RaftValue) -> String {
    (try? value.formatted(asJSON: true)) ?? "Unreadable value"
  }
}

final class MonitorRaftWrite {
  enum Format: String {
    case text = "Text"
    case dictionary = "Dictionary"
  }

  struct Command: Sendable {
    let group: String
    let key: String
    let value: RaftValue
    let id: UUID
  }

  enum Focus: Int, CaseIterable {
    case key, format, value
  }

  let group: String
  let name: String
  let key: MonitorVimField
  let value: MonitorVimField
  let dictionary = MonitorDictionaryEditor()
  private(set) var focus = Focus.key
  private(set) var format = Format.dictionary
  private(set) var commandLine: TextInput?
  var pending: UUID? {
    didSet { updateEditorFocus() }
  }
  var committed: UInt64?
  var failure: String?
  private var previous: Command?

  init(group: String, name: String) throws {
    self.group = group
    self.name = name
    key = try MonitorVimField("", capacity: 1024, inserting: true)
    value = try MonitorVimField("", capacity: 1024, inserting: true)
  }

  var editingDictionary: Bool { focus == .value && format == .dictionary }

  var controls: String {
    if commandLine != nil { return "Enter Run command   Esc Cancel" }
    if pending != nil { return "" }
    switch focus {
    case .key:
      return key.inserting ? "Enter Continue   Tab Value type   Esc Normal mode" : "i Edit key   Enter Continue   :q Close"
    case .format:
      return "h/l Choose value type   Enter Continue   Shift+Tab Key"
    case .value where format == .dictionary:
      return dictionary.controls
    case .value:
      return value.inserting
        ? "Enter Finish editing   Esc Normal mode"
        : "i Edit text   Shift+Tab Value type   :w Write   :q Close"
    }
  }

  func selectFormat(_ format: Format) {
    guard self.format != format else { return }
    self.format = format
    edited()
    updateEditorFocus()
  }

  private func selectFocus(_ focus: Focus, insert: Bool) {
    self.focus = focus
    if insert {
      if focus == .key { key.beginInsert() }
      if focus == .value, format == .text { value.beginInsert() }
    }
    updateEditorFocus()
  }

  private func updateEditorFocus() {
    dictionary.focused = editingDictionary && pending == nil && commandLine == nil
    if !editingDictionary { dictionary.leaveEditing() }
  }

  func handle(_ input: InputKey, modifiers: KeyModifiers, copyBuffer: inout CopyBuffer) -> MonitorWriteAction {
    let modifiers = modifiers.withoutLocks
    if let commandLine {
      if input == .escape || input == .enter, modifiers.isEmpty {
        let words = commandLine.snapshot().split(separator: " ")
        self.commandLine = nil
        updateEditorFocus()
        guard input == .enter else { return .redraw }
        switch words {
        case ["w"]: return .submit
        case ["q"]: return .dismiss
        case []: return .redraw
        default: return .rejected("Use :w to write or :q to close.")
        }
      }
      if modifiers.isEmpty || modifiers == .shift {
        if case .control = input { return .ignored }
        _ = commandLine.handle(input, modifiers: modifiers, copyBuffer: &copyBuffer)
        return .redraw
      }
      return .ignored
    }
    let field = focus == .key ? key : value
    let fieldBrowsing = editingDictionary ? dictionary.isBrowsing : (focus == .format || !field.inserting)
    if input == .character(":"), (modifiers.isEmpty || modifiers == .shift), fieldBrowsing {
      commandLine = try? TextInput("", capacity: 16)
      dictionary.leaveEditing()
      updateEditorFocus()
      return .redraw
    }
    if input == .tab, (modifiers.isEmpty || modifiers == .shift) {
      guard !editingDictionary || dictionary.isBrowsing else { return .ignored }
      let next = focus.rawValue + (modifiers == .shift ? -1 : 1)
      selectFocus(Focus.allCases[(next + Focus.allCases.count) % Focus.allCases.count], insert: true)
      return .redraw
    }
    if editingDictionary { return dictionary.handle(input, modifiers: modifiers, copyBuffer: &copyBuffer) }
    if focus == .format {
      guard modifiers.isEmpty else { return .ignored }
      switch input {
      case .left, .character("h"): selectFormat(.dictionary)
      case .right, .character("l"): selectFormat(.text)
      case .enter: selectFocus(.value, insert: true)
      default: return .ignored
      }
      return .redraw
    }
    switch field.handle(input, modifiers: modifiers, copyBuffer: &copyBuffer) {
    case .ignored: return .ignored
    case .changed: return .edited
    case .rejected: return .invalidField
    case .submit:
      if focus == .key {
        guard !key.input.snapshot().isEmpty else { return .rejected("Enter a key.") }
        selectFocus(.value, insert: true)
      } else if field.inserting {
        _ = field.handle(.escape, modifiers: [], copyBuffer: &copyBuffer)
      }
      return .redraw
    }
  }

  func paste(_ text: UTF8Span) -> MonitorWriteAction {
    guard commandLine == nil else { return .ignored }
    if editingDictionary { return dictionary.paste(text) }
    guard focus != .format else { return .ignored }
    let field = focus == .key ? key : value
    guard field.inserting else { return .ignored }
    let value = String(copying: text)
    guard !value.contains(where: \.isNewline), !value.contains("\t") else { return .rejected("Paste one line.") }
    do { try field.input.insert(text) } catch { return .invalidField }
    return .edited
  }

  func command() -> Command? {
    let key = key.input.snapshot()
    guard !key.isEmpty else {
      failure = "Enter a key."
      selectFocus(.key, insert: true)
      return nil
    }
    let value: RaftValue
    switch format {
    case .text:
      value = .string(self.value.input.snapshot())
    case .dictionary:
      value = dictionary.document
    }
    if let previous, previous.key == key, previous.value == value { return previous }
    let next = Command(group: group, key: key, value: value, id: UUID())
    previous = next
    return next
  }

  func edited() {
    failure = nil
    committed = nil
  }
}

struct MonitorRaftAction: Sendable {
  enum Operation: Sendable {
    case inspect(RaftInspection)
    case put(MonitorRaftWrite.Command)
  }

  struct Result: Sendable {
    enum Outcome: Sendable {
      case inspection(RaftInspectionReply)
      case committed(UInt64)
      case failure(String)
    }

    let id: UUID
    let outcome: Outcome
  }

  let id: UUID
  let operation: Operation

  static func run(
    socket: String, requests: ScreenSource<Self>, results: ScreenSource<Result>
  ) async {
    while let action = await requests.next(), !Task.isCancelled {
      let outcome: Result.Outcome
      do {
        switch action.operation {
        case .inspect(let request):
          outcome = .inspection(try await RaftClient.inspect(request, socket: socket))
        case .put(let command):
          outcome = .committed(try await RaftClient.put(
            key: command.key, value: command.value, group: command.group, requestID: command.id, socket: socket))
        }
      } catch {
        guard !Task.isCancelled else { return }
        outcome = .failure(String(String(describing: error).prefix(512)))
      }
      results.publish(Result(id: action.id, outcome: outcome))
    }
  }
}
