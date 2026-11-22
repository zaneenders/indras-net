import Foundation
import IndraNode
import IndraRaft
import Observation
import Slate
import TCPActorSystem

@MainActor public func runMonitor(socket: String) async throws {
  var screen = MonitorScreen(socket: socket)
  let session = ScreenSession(maximumOutputBytes: 4 * 1_048_576, taskCapacity: 6)
  do {
    try await session.run(
      application: &screen, inputFileDescriptor: 0, outputFileDescriptor: 1, signalHandling: .managed)
  } catch ScreenApplicationError.cancelled { return }
}

enum MonitorReading: Sendable {
  case snapshot(
    MonitorSnapshot, services: [DiscoveredService]?, raft: [ServiceReport<RaftStatus>]?,
    receivedAt: ContinuousClock.Instant)
  case failure(String)
}

private func poll(socket: String, readings: ScreenSource<MonitorReading>) async {
  while !Task.isCancelled {
    let reading: MonitorReading
    do {
      async let services = discoveredServices(socket: socket)
      async let raft = raftReports(socket: socket)
      guard case .monitor(let snapshot) = try await NodeControl.request(.monitor, socket: socket, timeout: .seconds(3))
      else { throw IndraError("Expected a monitor snapshot. Check that the daemon supports monitor.") }
      let receivedAt = ContinuousClock.now
      reading = .snapshot(snapshot, services: await services, raft: await raft, receivedAt: receivedAt)
    } catch {
      guard !Task.isCancelled else { return }
      reading = .failure(String(describing: error))
    }
    readings.publish(reading)
    do { try await Task.sleep(for: .seconds(1)) } catch { return }
  }
}

private func discoveredServices(socket: String) async -> [DiscoveredService]? {
  guard case .services(let services) = try? await NodeControl.request(.services, socket: socket, timeout: .seconds(3))
  else { return nil }
  return services
}

private func raftReports(socket: String) async -> [ServiceReport<RaftStatus>]? {
  try? await NodeControl.query(RaftStatus.serviceName, returning: RaftStatus.self, socket: socket, timeout: .seconds(1))
}

enum MonitorTab: String, Sendable {
  case swim = "SWIM"
  case raft = "Raft"
}

struct MonitorModel: Sendable {
  var status: NodeStatus? { snapshot?.status }
  var snapshot: MonitorSnapshot?
  var error: String?
  var receivedAt: ContinuousClock.Instant?
  var selectedProcess: UUID?
  var inboundRate: Double?
  var outboundRate: Double?
  var history: [Double] = []
  var showDetails = false
  private(set) var selectedServiceIndex = 0
  private(set) var tab: MonitorTab = .swim
  var isRaftClient: Bool {
    guard let node = status?.node.id, let services else { return false }
    return !services.contains { $0.node.id == node && $0.registration.key == RaftStatus.serviceName }
  }
  private var raftGroupID: String?
  private var raftReplicaID: String?
  private var lastSwimSelection: UUID?
  private(set) var raftGroups: [MonitorRaftGroup]?
  private var services: [DiscoveredService]?
  private var raft: [ServiceReport<RaftStatus>]?
  var manualPing: ManualPing?

  mutating func apply(_ reading: MonitorReading) {
    switch reading {
    case .failure(let message): error = message
    case .snapshot(let next, let services, let raft, let receivedAt):
      let previousProcess = selectedProcess
      self.services = services
      self.raft = raft
      raftGroups = raft.map { MonitorRaftGroup.collect(reports: $0, status: next.status) }
      apply(next, receivedAt: receivedAt)
      selectedServiceIndex = previousProcess == selectedProcess ? min(selectedServiceIndex, lastServiceIndex) : 0
    }
  }

  private mutating func apply(_ nextSnapshot: MonitorSnapshot, receivedAt: ContinuousClock.Instant) {
    error = nil
    let next = nextSnapshot.status
    let sameProcess = status?.node.endpoint.systemID == next.node.endpoint.systemID
    inboundRate = nil
    outboundRate = nil
    if sameProcess, let before = snapshot {
      let elapsed = nextSnapshot.uptimeSeconds - before.uptimeSeconds
      if elapsed > 0, nextSnapshot.inbound.messages >= before.inbound.messages,
        nextSnapshot.outbound.messages >= before.outbound.messages
      {
        inboundRate = Double(nextSnapshot.inbound.messages - before.inbound.messages) / elapsed
        outboundRate = Double(nextSnapshot.outbound.messages - before.outbound.messages) / elapsed
      }
    }
    if !sameProcess { history.removeAll(keepingCapacity: true) }
    if let inboundRate, let outboundRate {
      if history.count == 60 { history.removeFirst() }
      history.append(inboundRate + outboundRate)
    }
    snapshot = nextSnapshot
    self.receivedAt = receivedAt
    if !next.members.contains(where: { $0.node.endpoint.systemID == selectedProcess }) {
      selectedProcess = next.members.first?.node.endpoint.systemID
    }
  }

  var selectedIndex: Int? { status?.members.firstIndex { $0.node.endpoint.systemID == selectedProcess } }
  var selected: Member? {
    guard let selectedIndex else { return nil }
    return status?.members[selectedIndex]
  }

  var serviceNode: NodeIdentity? { selected?.node ?? status?.node }

  var selectedNodeServices: [ServiceRegistration]? {
    guard let node = serviceNode else { return nil }
    return services?.compactMap {
      $0.node.id == node.id && $0.node.endpoint == node.endpoint ? $0.registration : nil
    }
  }

  var selectedRaftReport: ServiceReport<RaftStatus>.Outcome? {
    guard let node = serviceNode else { return nil }
    return raft?.first { $0.node.id == node.id && $0.node.endpoint == node.endpoint }?.outcome
  }

  private var lastServiceIndex: Int { max(0, (selectedNodeServices?.count ?? 0) - 1) }

  mutating func moveService(_ offset: Int) {
    selectedServiceIndex = max(0, min(lastServiceIndex, selectedServiceIndex + offset))
  }

  var raftGroup: MonitorRaftGroup? {
    raftGroups?.first { $0.id == raftGroupID } ?? raftGroups?.first
  }

  var selectedRaftReplicaIndex: Int {
    raftGroup?.replicas.firstIndex { $0.id == raftReplicaID } ?? 0
  }

  mutating func selectTab(_ next: MonitorTab) {
    guard tab != next else { return }
    tab = next
    guard next == .raft else { return }
    if (lastSwimSelection != selectedProcess || (raftGroupID == nil && raftReplicaID == nil)),
      case .responded(let status) = selectedRaftReport
    {
      raftGroupID = status.group
      raftReplicaID = status.node
    }
    lastSwimSelection = selectedProcess
  }

  mutating func moveRaftGroup(_ offset: Int) {
    guard let groups = raftGroups, let current = raftGroup,
      let index = groups.firstIndex(where: { $0.id == current.id })
    else { return }
    raftGroupID = groups[(index + offset % groups.count + groups.count) % groups.count].id
    raftReplicaID = nil
  }

  mutating func move(_ offset: Int) {
    if tab == .raft {
      guard let replicas = raftGroup?.replicas, !replicas.isEmpty else { return }
      raftReplicaID = replicas[max(0, min(replicas.count - 1, selectedRaftReplicaIndex + offset))].id
      return
    }
    guard let members = status?.members, !members.isEmpty else { return }
    let next = members[max(0, min(members.count - 1, (selectedIndex ?? 0) + offset))].node.endpoint.systemID
    if selectedProcess != next { selectedServiceIndex = 0 }
    selectedProcess = next
  }
}

@MainActor @Observable final class MonitorScreen: ScreenApplication {
  private let socket: String?
  let tabHits = HitMap<MonitorTab>(capacity: 2)
  var pings: MonitorPings?
  var model = MonitorModel()
  private(set) var raftOverlay: MonitorRaftOverlay?
  private let raftRequests = ScreenSource<MonitorRaftAction>(policy: .ordered(capacity: 1))
  private let raftResults = ScreenSource<MonitorRaftAction.Result>(policy: .latest)

  init(socket: String?) { self.socket = socket }

  func start(in session: ScreenSession) {
    guard let socket else { return }
    let readings = ScreenSource<MonitorReading>(policy: .latest)
    let pings = MonitorPings()
    self.pings = pings
    precondition(session.consume(readings, at: 0) { [self] reading in model.apply(reading) })
    precondition(session.startTask(at: 1) { await poll(socket: socket, readings: readings) })
    precondition(session.startTask(at: 2) { await pings.run(socket: socket) })
    precondition(session.consume(pings.updates, at: 3) { [self] result in model.manualPing = result })
    let requests = raftRequests
    let results = raftResults
    precondition(session.startTask(at: 4) { await MonitorRaftAction.run(socket: socket, requests: requests, results: results) })
    precondition(session.consume(results, at: 5) { [self] result in receiveRaftAction(result) })
  }

  func stop() async {
    pings?.finish()
    raftRequests.finish()
    raftResults.finish()
  }

  static var configuration: ScreenApplicationConfiguration {
    ScreenApplicationConfiguration(
      maximumSize: TerminalSize(columns: 320, rows: 120), viewportOverflow: .clip,
      mouseTracking: true, enhancedKeyboard: true, kittyGraphicsQueryID: nil,
      capabilities: TerminalCapabilities(color: .trueColor, unicode: .grapheme),
      maximumPasteBytes: MonitorDictionaryEditor.capacity,
      layoutBudget: MonitorView.layoutBudget)
  }

  var tickInterval: Duration? { .seconds(1) }

  func event(_ event: borrowing ScreenEvent, copyBuffer: inout CopyBuffer) -> ScreenUpdate {
    switch event {
    case .key(let key, let modifiers) where key == .control(.c)
      || (key == .character("c") && modifiers.contains(.control)):
      return .quit
    default: break
    }
    if let raftOverlay {
      return raftEvent(event, overlay: raftOverlay, copyBuffer: &copyBuffer)
    }
    switch event {
    case .tick:
      return .redraw
    case .mouse(let mouse):
      guard mouse.action == .press, mouse.button == .left, let hit = tabHits.hitTest(mouse) else { return .none }
      model.selectTab(hit.id)
      return .redraw
    case .key(let key, let modifiers):
      if Self.isTabSwitch(key, modifiers: modifiers) {
        switchTab()
        return .redraw
      }
      switch key {
      case .character("q"): return .quit
      case .escape where model.tab == .raft: model.selectTab(.swim)
      case .escape:
        if model.showDetails { model.showDetails = false } else { return .quit }
      case .up, .character("k"): model.move(-1)
      case .down, .character("j"): model.move(1)
      case .left where model.tab == .raft: model.moveRaftGroup(-1)
      case .right where model.tab == .raft: model.moveRaftGroup(1)
      case .left: model.moveService(-1)
      case .right: model.moveService(1)
      case .character("l"):
        guard model.tab == .raft else { return .none }
        openRaftInspection(.log)
      case .character("s") where model.tab == .raft, .character("S") where model.tab == .raft:
        openRaftInspection(.snapshot)
      case .character("w"), .character("W"):
        guard model.tab == .raft, let group = model.raftGroup,
          let write = try? MonitorRaftWrite(group: group.id, name: group.name)
        else { return .none }
        raftOverlay = .write(write)
      case .enter:
        guard model.tab == .swim else { return .none }
        model.showDetails.toggle()
      case .character("p"), .character("P"):
        guard model.tab == .swim, let node = model.selected?.node, let pings else {
          return .none
        }
        pings.ping(node)
        model.manualPing = pings.read()
      default: return .none
      }
      return .redraw
    default: return .none
    }
  }

  private static func isTabSwitch(_ key: InputKey, modifiers: KeyModifiers) -> Bool {
    let modifiers = modifiers.withoutLocks
    switch key {
    case .character("H"), .character("L"): return modifiers.isEmpty || modifiers == .shift
    case .character("h"), .character("l"): return modifiers == .shift
    default: return false
    }
  }

  private func switchTab() {
    raftOverlay = nil
    model.selectTab(model.tab == .swim ? .raft : .swim)
  }

  private func openRaftInspection(_ kind: MonitorRaftInspection.Kind) {
    guard let group = model.raftGroup else { return }
    let replica = group.replicas[model.selectedRaftReplicaIndex]
    var inspection = MonitorRaftInspection(kind: kind, group: group.id, peer: replica.id, name: replica.name)
    inspect(&inspection)
    raftOverlay = .inspection(inspection)
  }

  private func inspect(_ inspection: inout MonitorRaftInspection) {
    let action = MonitorRaftAction(id: UUID(), operation: .inspect(inspection.request))
    inspection.reply = nil
    inspection.failure = nil
    if raftRequests.publish(action) == .accepted {
      inspection.pending = action.id
    } else {
      inspection.failure = "Inspection is busy. Press R to retry."
    }
  }

  private func raftEvent(
    _ event: borrowing ScreenEvent, overlay: MonitorRaftOverlay, copyBuffer: inout CopyBuffer
  ) -> ScreenUpdate {
    switch event {
    case .tick: return .redraw
    default: break
    }
    switch overlay {
    case .inspection(var inspection):
      let key: InputKey
      switch event {
      case .key(let input, let modifiers):
        if Self.isTabSwitch(input, modifiers: modifiers) {
          switchTab()
          return .redraw
        }
        key = input
      default: return .none
      }
      switch key {
      case .escape:
        raftOverlay = nil
        return .redraw
      case .character("q"): return .quit
      case .character("l"), .character("s"), .character("S"):
        let kind: MonitorRaftInspection.Kind = key == .character("l") ? .log : .snapshot
        guard inspection.kind != kind, inspection.pending == nil else { return .none }
        inspection = MonitorRaftInspection(kind: kind, group: inspection.group, peer: inspection.peer, name: inspection.name)
        inspect(&inspection)
      case .left: inspection.pan(-16)
      case .right: inspection.pan(16)
      default:
        guard inspection.move(key) else { return .none }
        inspect(&inspection)
      }
      raftOverlay = .inspection(inspection)
    case .write(let write):
      return raftWriteEvent(event, write: write, copyBuffer: &copyBuffer)
    }
    return .redraw
  }

  private func raftWriteEvent(
    _ event: borrowing ScreenEvent, write: MonitorRaftWrite, copyBuffer: inout CopyBuffer
  ) -> ScreenUpdate {
    guard write.pending == nil else { return .none }
    let action: MonitorWriteAction
    switch event {
    case .key(let key, let modifiers):
      action = write.handle(key, modifiers: modifiers, copyBuffer: &copyBuffer)
    case .paste(let text):
      action = write.paste(text)
    case .pasteRejected(let reason):
      action = .rejected(reason == .tooLarge ? "Paste exceeds 16 KiB." : "Unable to paste this text.")
    default: return .none
    }

    switch action {
    case .ignored: return .none
    case .redraw: break
    case .edited: write.edited()
    case .dismiss: raftOverlay = nil
    case .rejected(let message): write.failure = message
    case .submit: submit(write)
    }
    return .redraw
  }

  private func submit(_ write: MonitorRaftWrite) {
    guard let command = write.command() else { return }
    let action = MonitorRaftAction(id: UUID(), operation: .put(command))
    if raftRequests.publish(action) == .accepted {
      write.edited()
      write.pending = action.id
    } else {
      write.failure = "Raft is busy. Retry with :w."
    }
  }

  private func receiveRaftAction(_ result: MonitorRaftAction.Result) {
    switch raftOverlay {
    case .inspection(var inspection) where inspection.pending == result.id:
      inspection.pending = nil
      switch result.outcome {
      case .inspection(let reply): inspection.receive(reply)
      case .failure(let message): inspection.failure = message
      case .committed: break
      }
      raftOverlay = .inspection(inspection)
    case .write(let write) where write.pending == result.id:
      write.pending = nil
      switch result.outcome {
      case .committed(let index): write.committed = index
      case .failure(let message): write.failure = message
      case .inspection: break
      }
    default: break
    }
  }

  borrowing func render(in size: TerminalSize) -> some Block {
    MonitorView(model: model, size: size, raftOverlay: raftOverlay, tabHits: tabHits)
  }
}
