import Foundation
import Slate
import TCPActorSystem
import Testing

@testable import IndraMonitor
@testable import IndraNode
@testable import IndraRaft

@MainActor struct MonitorTests {
  @Test func dictionaryFormAcceptsPrettyJSONAndKeepsTheRetryID() throws {
    let write = try MonitorRaftWrite(group: "group", name: "test")
    try write.key.input.insert("profile".utf8Span)
    try write.value.input.insert("text draft".utf8Span)
    let json = """
      {
        "name": "Zane",
        "active": true
      }
      """
    _ = write.dictionary.paste(json.utf8Span)
    let first = try #require(write.command())
    #expect(first.value == (try RaftValue(documentJSON: json)))

    var copyBuffer = CopyBuffer()
    _ = write.dictionary.handle(.character("u"), modifiers: [], copyBuffer: &copyBuffer)
    _ = write.dictionary.paste(#"{"active":true,"name":"Zane"}"#.utf8Span)
    #expect(write.command()?.id == first.id)

    write.selectFormat(.text)
    let text = try #require(write.command())
    #expect(text.value == .string("text draft"))
    #expect(text.id != first.id)
    write.selectFormat(.dictionary)
    #expect(write.dictionary.document == first.value)
  }

  @Test func dictionaryFormBuildsTypedAndNestedValuesWithoutJSONInput() throws {
    let editor = MonitorDictionaryEditor()
    var copyBuffer = CopyBuffer()
    func press(_ key: InputKey, modifiers: KeyModifiers) -> MonitorWriteAction {
      editor.handle(key, modifiers: modifiers, copyBuffer: &copyBuffer)
    }
    func add(_ name: String, typeOffset: Int) {
      _ = press(.character("a"), modifiers: [])
      if !name.isEmpty {
        _ = editor.paste(name.utf8Span)
        _ = press(.enter, modifiers: [])
      }
      for _ in 0..<typeOffset { _ = press(.character("j"), modifiers: []) }
      _ = press(.enter, modifiers: [])
    }

    add("flags", typeOffset: 5)
    _ = press(.character("l"), modifiers: [])
    add("", typeOffset: 2)
    _ = press(.character(" "), modifiers: [])
    _ = press(.character("h"), modifiers: [])

    add("profile", typeOffset: 4)
    _ = press(.character("l"), modifiers: [])
    add("visits", typeOffset: 1)
    _ = editor.paste("invalid".utf8Span)
    guard case .rejected = press(.enter, modifiers: []) else {
      Issue.record("Expected the number field to reject nonnumeric input")
      return
    }
    guard case .ignored = press(.character("a"), modifiers: [.control]) else {
      Issue.record("Expected Ctrl+A to leave the input alone")
      return
    }
    _ = press(.escape, modifiers: [])
    _ = press(.character("c"), modifiers: [])
    _ = press(.character("c"), modifiers: [])
    _ = editor.paste("3".utf8Span)
    _ = press(.enter, modifiers: [])
    add("name", typeOffset: 0)
    _ = editor.paste("Zane 👩‍💻".utf8Span)
    _ = press(.enter, modifiers: [])

    let expected = try RaftValue(documentJSON: #"{"flags":[true],"profile":{"visits":3,"name":"Zane 👩‍💻"}}"#)
    #expect(editor.document == expected)
    _ = press(.character("d"), modifiers: [])
    _ = press(.character("d"), modifiers: [])
    #expect(editor.document != expected)
    _ = press(.character("u"), modifiers: [])
    #expect(editor.document == expected)
    _ = press(.character("r"), modifiers: [.control])
    #expect(editor.document != expected)
    _ = press(.character("u"), modifiers: [])
    _ = press(.character("h"), modifiers: [])
    #expect(editor.document == expected)
  }

  @Test func pingKeyUsesSelectedAddressAndDoesNotQueueRepeatedPresses() async throws {
    let screen = MonitorScreen(socket: nil)
    let pings = MonitorPings()
    screen.pings = pings
    screen.model.apply(
      reading(status: fixtureStatus(process: UUID()), uptime: 10, inbound: 0, outbound: 0, services: [], raft: nil))
    var copyBuffer = CopyBuffer()
    #expect(screen.event(.key(.character("p"), modifiers: []), copyBuffer: &copyBuffer) == .redraw)
    let original = try #require(pings.read())
    #expect(original.isPending)
    #expect(original.target.endpoint.port == 7332)
    screen.model.move(1)
    _ = screen.event(.key(.character("P"), modifiers: [.shift]), copyBuffer: &copyBuffer)
    #expect(pings.read()?.target == original.target)
    let socket = "/tmp/indra-missing-\(UUID().uuidString).sock"
    let worker = Task { await pings.run(socket: socket) }
    do {
      try await waitForPing(pings)
      screen.model.manualPing = pings.read()
      if case .failure = screen.model.manualPing?.outcome {
      } else {
        Issue.record("Expected the daemon connection failure to be shown")
      }
      #expect(pings.read()?.target == original.target)
      _ = screen.event(.key(.character("p"), modifiers: []), copyBuffer: &copyBuffer)
      try await waitForPing(pings)
      #expect(pings.read()?.target.name == "king-dog")
      #expect(screen.model.status?.members.first?.state == .alive)
      pings.finish()
      await worker.value
    } catch {
      pings.finish()
      worker.cancel()
      await worker.value
      throw error
    }
  }

  @Test func pingResultDistinguishesProcessReplacement() throws {
    var model = MonitorModel()
    let status = fixtureStatus(process: UUID())
    model.apply(reading(status: status, uptime: 10, inbound: 0, outbound: 0, services: [], raft: nil))
    let selected = try #require(model.selected?.node)
    let replacement = NodeIdentity(
      id: selected.id, name: selected.name,
      endpoint: ActorEndpoint(host: selected.endpoint.host, port: selected.endpoint.port, systemID: UUID()), build: nil)
    model.manualPing = ManualPing(
      target: selected, outcome: .success(PingResult(node: replacement, milliseconds: 12.34)))
    let text = try render(model, size: TerminalSize(columns: 140, rows: 42))
    #expect(text.contains("P ping"))
    #expect(text.contains("PONG studio 127.0.0.1:7332: 12.34 ms (different process replied)"))
    #expect(model.status?.members.first?.node.endpoint == selected.endpoint)
  }

  @Test func ratesUseDaemonTimeAndResetAfterRestart() {
    let status = fixtureStatus(process: UUID())
    var model = MonitorModel()
    model.apply(reading(status: status, uptime: 10, inbound: 2, outbound: 3, services: [], raft: nil))
    #expect(model.inboundRate == nil)
    model.apply(reading(status: status, uptime: 12, inbound: 6, outbound: 9, services: [], raft: nil))
    #expect(model.inboundRate == 2)
    #expect(model.outboundRate == 3)
    #expect(model.history == [5])
    model.apply(
      reading(status: fixtureStatus(process: UUID()), uptime: 1, inbound: 0, outbound: 0, services: [], raft: nil))
    #expect(model.inboundRate == nil)
    #expect(model.history.isEmpty)
  }

  @Test func disconnectPreservesTheObservedView() {
    let status = fixtureStatus(process: UUID())
    let initial = reading(status: status, uptime: 10, inbound: 2, outbound: 3, services: [], raft: nil)
    var model = MonitorModel()
    model.apply(initial)
    model.move(2)
    #expect(model.selected?.node.name == "pi")
    model.apply(.failure("Disconnected"))
    #expect(model.status?.node.name == "tower")
    #expect(model.error == "Disconnected")
    #expect(model.snapshot?.uptimeSeconds == 10)
    model.apply(reading(status: status, uptime: 14, inbound: 10, outbound: 15, services: [], raft: nil))
    #expect(model.error == nil)
    #expect(model.snapshot?.uptimeSeconds == 14)
    #expect(model.selected?.node.name == "pi")
  }

  @Test(arguments: [
    TerminalSize(columns: 60, rows: 22), TerminalSize(columns: 80, rows: 24), TerminalSize(columns: 140, rows: 42)
  ])
  func rendersMembershipAndBuildsAtSupportedSizes(size: TerminalSize) throws {
    var model = MonitorModel()
    model.apply(
      reading(status: fixtureStatus(process: UUID()), uptime: 12, inbound: 6, outbound: 9, services: [], raft: nil))
    model.move(2)
    let text = try render(model, size: size)
    #expect(text.contains("INDRA / CLUSTER MONITOR"))
    #expect(text.contains("LIVE"))
    #expect(text.contains("tower"))
    #expect(text.contains("pi"))
    #expect(text.contains("dirty"))
    #expect(text.contains("RECENT ACTIVITY"))
    #expect(text.contains("Q quit"))
    #expect(!text.contains("Space pause"))
    model.showDetails = true
    let details = try render(model, size: size)
    #expect(details.contains("SELECTED PEER"))
    #expect(details.contains("Build:"))
  }

  @Test func smallTerminalShowsResizeInstructions() throws {
    let text = try render(MonitorModel(), size: TerminalSize(columns: 40, rows: 12))
    #expect(text.contains("Resize to at least 60"))
    #expect(text.contains("Q quit"))
  }

  @Test func raftProgressFollowsTheSelectedPeerProcess() throws {
    var model = MonitorModel()
    let status = fixtureStatus(process: UUID())
    let host = status.members[0].node
    let service = DiscoveredService(
      node: host,
      registration: ServiceRegistration(
        key: RaftStatus.serviceName, actorType: "IndraRaft.RaftService",
        address: ActorAddress(endpoint: host.endpoint, id: UUID())))
    let progress = RaftStatus(
      node: host.id.uuidString, term: 3, commitIndex: 42, appliedIndex: 41, voters: 1, readers: 0,
      group: host.id.uuidString, role: "leader", leader: host.id.uuidString, snapshotIndex: 0,
      configuration: nil)
    let report = ServiceReport(node: host, outcome: .responded(metrics: progress))
    let size = TerminalSize(columns: 140, rows: 42)
    model.apply(reading(status: status, uptime: 10, inbound: 0, outbound: 0, services: [service], raft: [report]))
    let text = try render(model, size: size)
    #expect(text.contains("Raft: leader / term 3 / voters 1"))
    #expect(text.contains("Commit 42 / applied 41"))
    model.move(1)
    #expect(try !render(model, size: size).contains("Raft:"))
    model.move(-1)
    model.apply(reading(status: status, uptime: 11, inbound: 0, outbound: 0, services: [service], raft: nil))
    #expect(try render(model, size: size).contains("Raft: status unavailable"))
  }

  @Test(arguments: [TerminalSize(columns: 60, rows: 22), TerminalSize(columns: 140, rows: 42)])
  func raftViewShowsProgressAndOfflineVoters(size: TerminalSize) throws {
    let snapshot = fixtureStatus(process: UUID())
    let tower = snapshot.node
    let studio = snapshot.members[0].node
    let offline = snapshot.members[1].node
    let learner = snapshot.members[2].node
    let previousStudio = NodeIdentity(
      id: studio.id, name: studio.name,
      endpoint: ActorEndpoint(host: studio.endpoint.host, port: studio.endpoint.port, systemID: UUID()), build: nil)
    let group = tower.id.uuidString
    let roster = RaftConfigurationStatus(
      voters: [group, studio.id.uuidString, offline.id.uuidString], previousVoters: nil, readers: [learner.id.uuidString])
    func report(_ node: NodeIdentity, role: String, term: UInt64, commit: UInt64, applied: UInt64, snapshot: UInt64)
      -> ServiceReport<RaftStatus>
    {
      ServiceReport(
        node: node,
        outcome: .responded(metrics: RaftStatus(
          node: node.id.uuidString, term: term, commitIndex: commit, appliedIndex: applied, voters: 3, readers: 1,
          group: group, role: role, leader: group, snapshotIndex: snapshot, configuration: roster)))
    }
    let reports = [
      report(tower, role: "leader", term: 7, commit: 200, applied: 200, snapshot: 120),
      report(studio, role: "follower", term: 7, commit: 180, applied: 140, snapshot: 60),
      report(learner, role: "follower", term: 7, commit: 130, applied: 120, snapshot: 60),
      report(previousStudio, role: "follower", term: 99, commit: 900, applied: 900, snapshot: 800),
      ServiceReport(node: offline, outcome: .unreachable),
    ]
    let screen = MonitorScreen(socket: nil)
    screen.model.apply(reading(status: snapshot, uptime: 10, inbound: 0, outbound: 0, services: [], raft: reports))
    var copyBuffer = CopyBuffer()
    #expect(screen.event(.key(.character(" "), modifiers: []), copyBuffer: &copyBuffer) == .none)
    _ = screen.event(.key(.character("L"), modifiers: [.shift]), copyBuffer: &copyBuffer)
    #expect(screen.event(.key(.character(" "), modifiers: []), copyBuffer: &copyBuffer) == .none)
    let dashboard = try render(screen.model, size: size)
    #expect(dashboard.contains("Leader tower / term 7 / commit 200"))
    #expect(dashboard.contains("voters 2/3   learners 1/1"))
    #expect(dashboard.contains("king-dog"))
    #expect(dashboard.contains("offline"))
    #expect(dashboard.contains("learner"))
    #expect(dashboard.contains("Commit 180 / applied 140"))
    #expect(dashboard.contains("Snapshot 60 / lag 60"))
    #expect(dashboard.contains("░░░") && dashboard.contains("▒▒"))
    #expect(!dashboard.contains("term 99"))
    #expect(!dashboard.contains("Space pause"))
    _ = screen.event(.key(.character("s"), modifiers: []), copyBuffer: &copyBuffer)
    guard case .inspection(let inspection) = screen.raftOverlay else {
      Issue.record("Expected the Raft snapshot view")
      return
    }
    #expect(inspection.kind == .snapshot)
  }

  @Test func raftGroupNavigationKeepsSwimSelectionAndShowsAnElection() throws {
    let snapshot = fixtureStatus(process: UUID())
    let tower = snapshot.node
    let studio = snapshot.members[0].node
    let pi = snapshot.members[2].node
    func report(_ node: NodeIdentity, group: NodeIdentity, role: String, term: UInt64)
      -> ServiceReport<RaftStatus>
    {
      ServiceReport(
        node: node,
        outcome: .responded(metrics: RaftStatus(
          node: node.id.uuidString, term: term, commitIndex: 8, appliedIndex: 8, voters: 1, readers: 0,
          group: group.id.uuidString, role: role, leader: role == "leader" ? node.id.uuidString : nil,
          snapshotIndex: 0, configuration: nil)))
    }
    let reports = [
      report(tower, group: tower, role: "leader", term: 6),
      report(studio, group: tower, role: "candidate", term: 7),
      report(pi, group: pi, role: "leader", term: 2),
    ]
    let screen = MonitorScreen(socket: nil)
    let size = TerminalSize(columns: 80, rows: 24)
    screen.model.apply(reading(status: snapshot, uptime: 10, inbound: 0, outbound: 0, services: [], raft: reports))
    var copyBuffer = CopyBuffer()
    #expect(screen.event(.key(.character("r"), modifiers: []), copyBuffer: &copyBuffer) == .none)
    #expect(screen.model.tab == .swim)
    _ = screen.event(.key(.character("L"), modifiers: [.shift]), copyBuffer: &copyBuffer)
    #expect(try render(screen.model, size: size).contains("No leader observed / term 7"))
    _ = screen.event(.key(.right, modifiers: []), copyBuffer: &copyBuffer)
    #expect(try render(screen.model, size: size).contains("Leader pi / term 2"))
    _ = screen.event(.key(.escape, modifiers: []), copyBuffer: &copyBuffer)
    #expect(screen.model.selected?.node == studio)
    #expect(try render(screen.model, size: size).contains("MEMBERSHIP"))
    _ = screen.event(.key(.character("L"), modifiers: [.shift]), copyBuffer: &copyBuffer)
    #expect(try render(screen.model, size: size).contains("Leader pi / term 2"))
  }

  @Test func tabsClickAndSwitchWithTheKeyboardWhileTheWriteFormKeepsTabForFields() throws {
    let screen = monitorWithRaftLeader()
    let size = TerminalSize(columns: 60, rows: 22)
    var surface = try Surface(maximumSize: size, layoutBudget: MonitorScreen.configuration.layoutBudget)
    try surface.render(size: size, content: { screen.render(in: size) })
    let swim = try #require(screen.tabHits.bounds(for: .swim))
    let raft = try #require(screen.tabHits.bounds(for: .raft))
    let active = try #require(surface.cell(x: swim.x + 1, y: swim.y)?.style.background)
    let inactive = try #require(surface.cell(x: raft.x + 1, y: raft.y)?.style.background)
    #expect(active != inactive)

    var copyBuffer = CopyBuffer()
    let clickRaft = MouseEvent(action: .press, button: .left, column: raft.x, row: raft.y, modifiers: [])
    #expect(screen.event(.mouse(clickRaft), copyBuffer: &copyBuffer) == .redraw)
    #expect(screen.model.tab == .raft)
    try surface.render(size: size, content: { screen.render(in: size) })
    #expect(surface.cell(x: raft.x + 1, y: raft.y)?.style.background == active)
    #expect(surface.cell(x: swim.x + 1, y: swim.y)?.style.background == inactive)
    #expect(screen.event(.key(.tab, modifiers: []), copyBuffer: &copyBuffer) == .none)
    #expect(screen.event(.key(.tab, modifiers: [.shift]), copyBuffer: &copyBuffer) == .none)
    #expect(screen.model.tab == .raft)
    _ = screen.event(.key(.character("H"), modifiers: []), copyBuffer: &copyBuffer)
    #expect(screen.model.tab == .swim)
    _ = screen.event(.key(.character("l"), modifiers: [.shift]), copyBuffer: &copyBuffer)
    #expect(screen.model.tab == .raft)
    _ = screen.event(.key(.character("l"), modifiers: []), copyBuffer: &copyBuffer)
    guard case .inspection(let inspection) = screen.raftOverlay else {
      Issue.record("Expected lowercase l to open the Raft log")
      return
    }
    #expect(inspection.kind == .log)
    _ = screen.event(.key(.character("h"), modifiers: [.shift]), copyBuffer: &copyBuffer)
    #expect(screen.model.tab == .swim)
    #expect(screen.raftOverlay == nil)
    _ = screen.event(.key(.character("L"), modifiers: []), copyBuffer: &copyBuffer)
    #expect(screen.model.tab == .raft)
    _ = screen.event(.key(.character("w"), modifiers: []), copyBuffer: &copyBuffer)
    guard case .write(let write) = screen.raftOverlay else {
      Issue.record("Expected the Raft write form")
      return
    }
    try surface.render(size: size, content: { screen.render(in: size) })
    #expect(screen.tabHits.count == 0)
    let clickSwim = MouseEvent(action: .press, button: .left, column: swim.x, row: swim.y, modifiers: [])
    #expect(screen.event(.mouse(clickSwim), copyBuffer: &copyBuffer) == .none)
    _ = screen.event(.key(.tab, modifiers: []), copyBuffer: &copyBuffer)
    #expect(write.focus == .format)
    #expect(screen.model.tab == .raft)
  }

  @Test(arguments: [false, true])
  func controlCQuitsFromEveryMonitorView(enhanced: Bool) {
    let screen = monitorWithRaftLeader()
    let key: InputKey = enhanced ? .character("c") : .control(.c)
    let modifiers: KeyModifiers = enhanced ? [.control] : []
    var copyBuffer = CopyBuffer()
    func checkQuit() {
      #expect(screen.event(.key(key, modifiers: modifiers), copyBuffer: &copyBuffer) == .quit)
    }

    checkQuit()
    screen.model.selectTab(.raft)
    checkQuit()
    _ = screen.event(.key(.character("l"), modifiers: []), copyBuffer: &copyBuffer)
    guard case .inspection = screen.raftOverlay else {
      Issue.record("Expected the Raft inspection")
      return
    }
    checkQuit()
    _ = screen.event(.key(.escape, modifiers: []), copyBuffer: &copyBuffer)
    _ = screen.event(.key(.character("w"), modifiers: []), copyBuffer: &copyBuffer)
    guard case .write(let write) = screen.raftOverlay else {
      Issue.record("Expected the Raft write form")
      return
    }
    checkQuit()
    write.pending = UUID()
    checkQuit()
  }

  @Test func dictionaryFormRendersFieldsAndQueuesOnlyExplicitWrites() throws {
    let screen = monitorWithRaftLeader()
    screen.model.selectTab(.raft)
    var copyBuffer = CopyBuffer()
    func press(_ key: InputKey, modifiers: KeyModifiers) {
      _ = screen.event(.key(key, modifiers: modifiers), copyBuffer: &copyBuffer)
    }
    press(.character("w"), modifiers: [])
    guard case .write(let write) = screen.raftOverlay else {
      Issue.record("Expected the Raft write form")
      return
    }
    _ = screen.event(.paste("profile".utf8Span), copyBuffer: &copyBuffer)
    press(.enter, modifiers: [])
    #expect(write.editingDictionary)
    let size = TerminalSize(columns: 60, rows: 22)
    var surface = try Surface(maximumSize: size, layoutBudget: MonitorScreen.configuration.layoutBudget)
    func renderedRows() throws -> [String] {
      try surface.render(size: size, content: { screen.render(in: size) })
      return (0..<size.rows).map { y in
        String((0..<size.columns).compactMap { x in surface.cell(x: x, y: y)?.glyph.scalar }.map(Character.init))
      }
    }
    let empty = try renderedRows()
    #expect(empty.contains { $0.contains("[Dictionary]") && $0.contains("Text") })
    #expect(empty.contains { $0.contains("Enter Add field") })
    let addRow = try #require(empty.firstIndex { $0.contains("+ Add field") })

    press(.enter, modifiers: [])
    _ = screen.event(.paste("active".utf8Span), copyBuffer: &copyBuffer)
    #expect(try renderedRows()[addRow].contains("active"))
    press(.enter, modifiers: [])
    press(.control(.s), modifiers: [])
    #expect(write.pending == nil)
    #expect(!write.dictionary.isBrowsing)
    #expect(try renderedRows().contains { $0.contains("Choose a type for active") })
    press(.character("j"), modifiers: [])
    press(.character("j"), modifiers: [])
    press(.enter, modifiers: [])
    press(.character(" "), modifiers: [])
    #expect(write.pending == nil)

    let rows = try renderedRows()
    #expect(rows.contains { $0.contains("Dictionary value") })
    #expect(rows.contains { $0.contains("active") && $0.contains("Boolean") && $0.contains("true") })
    press(.character(":"), modifiers: [])
    press(.character("w"), modifiers: [])
    press(.enter, modifiers: [])
    #expect(write.pending != nil)
  }

  @Test func writeFormChoosesTextAndEditsTheKeyInNormalMode() throws {
    let write = try MonitorRaftWrite(group: "group", name: "test")
    var copyBuffer = CopyBuffer()
    func press(_ input: InputKey, modifiers: KeyModifiers) -> MonitorWriteAction {
      write.handle(input, modifiers: modifiers, copyBuffer: &copyBuffer)
    }
    _ = write.paste("draft".utf8Span)
    _ = press(.escape, modifiers: [])
    _ = press(.character("0"), modifiers: [])
    _ = press(.character("x"), modifiers: [])
    _ = press(.character("i"), modifiers: [])
    _ = press(.character("c"), modifiers: [])
    #expect(write.key.input.snapshot() == "craft")
    _ = press(.tab, modifiers: [])
    _ = press(.character("l"), modifiers: [])
    _ = press(.enter, modifiers: [])
    _ = write.paste("hello".utf8Span)
    _ = press(.escape, modifiers: [])
    _ = press(.character(":"), modifiers: [])
    _ = press(.character("w"), modifiers: [])
    guard case .submit = press(.enter, modifiers: []) else {
      Issue.record("Expected :w to submit the text value")
      return
    }
    let command = try #require(write.command())
    #expect(command.key == "craft")
    #expect(command.value == .string("hello"))
  }

  private func monitorWithRaftLeader() -> MonitorScreen {
    let status = fixtureStatus(process: UUID())
    let peer = status.members[0].node
    let id = peer.id.uuidString
    let leader = RaftStatus(
      node: id, term: 1, commitIndex: 2, appliedIndex: 2, voters: 1, readers: 0,
      group: id, role: "leader", leader: id, snapshotIndex: 0, configuration: nil)
    let screen = MonitorScreen(socket: nil)
    screen.model.apply(
      reading(
        status: status, uptime: 10, inbound: 0, outbound: 0, services: [],
        raft: [ServiceReport(node: peer, outcome: .responded(metrics: leader))]))
    return screen
  }

  @Test func servicesAppearInTheSelectedPeerPanelAndFollowTheHighlightedProcess() throws {
    let status = fixtureStatus(process: UUID())
    let peer = status.members[1].node
    let earlier = NodeIdentity(
      id: peer.id, name: peer.name,
      endpoint: ActorEndpoint(host: peer.endpoint.host, port: peer.endpoint.port, systemID: UUID()), build: peer.build)
    func service(_ key: String, on node: NodeIdentity) -> DiscoveredService {
      DiscoveredService(
        node: node,
        registration: ServiceRegistration(
          key: key, actorType: "Worker", address: ActorAddress(endpoint: node.endpoint, id: UUID())))
    }
    let workers = (0..<30).map { service("worker-\($0)", on: peer) }
    let all = workers + [service("old-service", on: earlier), service("other-service", on: status.members[0].node)]
    let screen = MonitorScreen(socket: nil)
    let wide = TerminalSize(columns: 140, rows: 42)
    let compact = TerminalSize(columns: 60, rows: 22)
    screen.model.apply(reading(status: status, uptime: 10, inbound: 0, outbound: 0, services: all, raft: nil))
    var copyBuffer = CopyBuffer()
    let initial = try render(screen.model, size: wide)
    #expect(initial.split(separator: "\n").contains { $0.contains("MEMBERS") && $0.contains("SELECTED PEER") })
    #expect(initial.contains("other-service  Worker"))
    #expect(!initial.contains("S services"))
    #expect(screen.event(.key(.character("s"), modifiers: []), copyBuffer: &copyBuffer) == .none)
    _ = screen.event(.key(.down, modifiers: []), copyBuffer: &copyBuffer)
    let listing = try render(screen.model, size: wide)
    #expect(listing.contains("SELECTED PEER"))
    #expect(listing.contains("king-dog /"))
    #expect(listing.contains("worker-0  Worker"))
    #expect(!listing.contains("old-service"))
    #expect(!listing.contains("other-service"))
    #expect(screen.model.selectedNodeServices == workers.map(\.registration))
    for _ in 0..<30 { _ = screen.event(.key(.right, modifiers: []), copyBuffer: &copyBuffer) }
    #expect(try render(screen.model, size: wide).contains("› worker-29"))
    _ = screen.event(.key(.enter, modifiers: []), copyBuffer: &copyBuffer)
    let scrolled = try render(screen.model, size: compact)
    #expect(scrolled.contains("› worker-29"))
    #expect(scrolled.contains(workers[29].registration.address.id.uuidString))
    screen.model.apply(reading(status: status, uptime: 11, inbound: 0, outbound: 0, services: [workers[0]], raft: nil))
    #expect(try render(screen.model, size: compact).contains("› worker-0"))
    _ = screen.event(.key(.escape, modifiers: []), copyBuffer: &copyBuffer)
    #expect(screen.model.selected?.node == peer)
    _ = screen.event(.key(.down, modifiers: []), copyBuffer: &copyBuffer)
    #expect(screen.event(.key(.character("S"), modifiers: [.shift]), copyBuffer: &copyBuffer) == .none)
    #expect(try render(screen.model, size: wide).contains("No services discovered for this node"))
    screen.model.apply(reading(status: status, uptime: 12, inbound: 0, outbound: 0, services: nil, raft: nil))
    #expect(try render(screen.model, size: wide).contains("Service listing unavailable"))
    #expect(screen.model.error == nil)
  }

  @Test func maximumViewportKeepsScrolledSelectionAndActivityWithinTheLayoutBudget() throws {
    let original = fixtureStatus(process: UUID())
    let peers = (0..<256).map { index in
      Member(
        node: NodeIdentity(
          id: UUID(), name: "peer-\(index)",
          endpoint: ActorEndpoint(host: "127.0.0.1", port: 10_000 + index, systemID: UUID()), build: .current),
        state: .alive)
    }
    let status = NodeStatus(
      node: original.node, cluster: original.cluster, members: peers,
      seeds: [], configurationError: "Test configuration error", discoveryErrors: [:])
    let events = (0..<128).map { index in
      ProbeEvent(
        id: UInt64(index), uptimeSeconds: 1, direction: .inbound, kind: .ack,
        peer: PeerAddress(host: "127.0.0.1", port: 7332), target: nil, sequenceNumber: UInt32(index),
        gossipMembers: 3, roundTripMilliseconds: 1, error: nil)
    }
    let changes = peers.suffix(2).map {
      MembershipChange(uptimeSeconds: 1, node: $0.node, previous: .suspect, current: .alive)
    }
    let snapshot = MonitorSnapshot(
      status: status, uptimeSeconds: 10, protocolPeriod: 1, localHealthMultiplier: 0,
      probeIntervalMilliseconds: 3000, requestTimeoutMilliseconds: 2000,
      outbound: ProbeTotals(), inbound: ProbeTotals(), inFlight: 0, events: events, changes: changes)
    var model = MonitorModel()
    model.apply(.snapshot(snapshot, services: [], raft: nil, receivedAt: .now))
    model.move(255)
    model.manualPing = ManualPing(target: peers[255].node, outcome: .pending)
    let text = try render(model, size: MonitorScreen.configuration.maximumSize)
    #expect(text.contains("peer-255"))
    #expect(!text.contains("peer-0 "))
    #expect(text.contains("SELECTED PEER"))
    #expect(text.contains("PING peer-255"))
    #expect(text.contains("Test configuration error"))
    #expect(text.contains("gossip=3"))
    #expect(text.contains("Q quit"))
  }

  @Test func longDaemonErrorsAreClippedWithoutRejectingTheDSLLayout() throws {
    var model = MonitorModel()
    model.apply(
      reading(status: fixtureStatus(process: UUID()), uptime: 10, inbound: 0, outbound: 0, services: [], raft: nil))
    model.apply(.failure(String(repeating: "e\u{301}", count: 5000)))
    let text = try render(model, size: TerminalSize(columns: 140, rows: 42))
    #expect(text.contains("DISCONNECTED"))
    #expect(text.contains("STALE VIEW:"))
    #expect(text.contains("Q quit"))
  }
}

@MainActor private func waitForPing(_ pings: MonitorPings) async throws {
  let deadline = ContinuousClock.now.advanced(by: .seconds(3))
  while pings.read()?.isPending == true {
    guard ContinuousClock.now < deadline else { throw IndraError("Timed out waiting for ping result") }
    try await Task.sleep(for: .milliseconds(10))
  }
}

@MainActor private func render(_ model: MonitorModel, size: TerminalSize) throws -> String {
  var surface = try Surface(maximumSize: size, layoutBudget: MonitorScreen.configuration.layoutBudget)
  let tabHits = HitMap<MonitorTab>(capacity: 2)
  try surface.render(size: size, content: { MonitorView(model: model, size: size, raftOverlay: nil, tabHits: tabHits) })
  var text = ""
  for y in 0..<size.rows {
    for x in 0..<size.columns {
      if let cell = surface.cell(x: x, y: y), !cell.isContinuation { text.unicodeScalars.append(cell.glyph.scalar) }
    }
    text += "\n"
  }
  return text
}

private func fixtureStatus(process: UUID) -> NodeStatus {
  let build = BuildIdentity(commit: String(repeating: "a", count: 40), isDirty: true)
  let node = NodeIdentity(
    id: UUID(), name: "tower", endpoint: ActorEndpoint(host: "127.0.0.1", port: 7331, systemID: process), build: build)
  let peers = ["studio", "king-dog", "pi"].enumerated().map { index, name in
    Member(
      node: NodeIdentity(
        id: UUID(), name: name,
        endpoint: ActorEndpoint(host: "127.0.0.1", port: 7332 + index, systemID: UUID()), build: build),
      state: index == 2 ? .suspect : .alive)
  }
  return NodeStatus(
    node: node, cluster: "test", members: peers, seeds: [], configurationError: nil, discoveryErrors: [:])
}

private func reading(
  status: NodeStatus, uptime: Double, inbound: Int, outbound: Int, services: [DiscoveredService]?,
  raft: [ServiceReport<RaftStatus>]?
) -> MonitorReading {
  var incoming = ProbeTotals()
  var outgoing = ProbeTotals()
  for _ in 0..<inbound { incoming.record(.ack) }
  for _ in 0..<outbound { outgoing.record(.ping) }
  let snapshot = MonitorSnapshot(
    status: status, uptimeSeconds: uptime, protocolPeriod: 1, localHealthMultiplier: 0,
    probeIntervalMilliseconds: 3000, requestTimeoutMilliseconds: 2000, outbound: outgoing, inbound: incoming,
    inFlight: 0, events: [], changes: [])
  return .snapshot(snapshot, services: services, raft: raft, receivedAt: .now)
}
