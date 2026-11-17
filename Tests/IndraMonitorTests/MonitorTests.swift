import Foundation
import Slate
import TCPActorSystem
import Testing

@testable import IndraMonitor
@testable import IndraNode

@MainActor struct MonitorTests {
  @Test func pingKeyUsesSelectedAddressWhilePausedAndDoesNotQueueRepeatedPresses() async throws {
    let screen = MonitorScreen(socket: nil)
    let pings = MonitorPings()
    screen.pings = pings
    screen.model.apply(
      reading(status: fixtureStatus(process: UUID()), uptime: 10, inbound: 0, outbound: 0, generation: 1))
    screen.model.paused = true
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
      #expect(screen.model.paused)
      #expect(screen.model.generation == 1)
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

  @Test func pingResultIsVisibleWithOlderDaemonsAndDistinguishesProcessReplacement() throws {
    var model = MonitorModel()
    let status = fixtureStatus(process: UUID())
    model.apply(MonitorReading(generation: 1, status: status, snapshot: nil, error: nil, receivedAt: .now))
    let selected = try #require(model.selected?.node)
    let replacement = NodeIdentity(
      id: selected.id, name: selected.name,
      endpoint: ActorEndpoint(host: selected.endpoint.host, port: selected.endpoint.port, node: UUID()), build: nil)
    model.manualPing = ManualPing(
      target: selected, outcome: .success(PingResult(node: replacement, milliseconds: 12.34)))
    let text = try render(model, size: TerminalSize(columns: 140, rows: 42))
    #expect(text.contains("P ping"))
    #expect(text.contains("PONG studio 127.0.0.1:7332: 12.34 ms (different process replied)"))
    #expect(text.contains("Update daemon to enable probe telemetry."))
    #expect(model.status?.members.first?.node.endpoint == selected.endpoint)
  }

  @Test func ratesUseDaemonTimeAndResetAfterRestart() {
    let status = fixtureStatus(process: UUID())
    var model = MonitorModel()
    model.apply(reading(status: status, uptime: 10, inbound: 2, outbound: 3, generation: 1))
    #expect(model.inboundRate == nil)
    model.apply(reading(status: status, uptime: 12, inbound: 6, outbound: 9, generation: 2))
    #expect(model.inboundRate == 2)
    #expect(model.outboundRate == 3)
    #expect(model.history == [5])
    model.apply(reading(status: fixtureStatus(process: UUID()), uptime: 1, inbound: 0, outbound: 0, generation: 3))
    #expect(model.inboundRate == nil)
    #expect(model.history.isEmpty)
  }

  @Test func pauseAndDisconnectPreserveTheObservedView() {
    let status = fixtureStatus(process: UUID())
    let initial = reading(status: status, uptime: 10, inbound: 2, outbound: 3, generation: 1)
    var model = MonitorModel()
    model.apply(initial)
    model.move(2)
    #expect(model.selected?.node.name == "pi")
    model.paused = true
    model.apply(reading(status: status, uptime: 12, inbound: 6, outbound: 9, generation: 2))
    #expect(model.generation == 1)
    model.paused = false
    model.apply(MonitorReading(generation: 3, status: nil, snapshot: nil, error: "Disconnected", receivedAt: .now))
    #expect(model.status?.node.name == "tower")
    #expect(model.error == "Disconnected")
    #expect(model.snapshot?.uptimeSeconds == 10)
    model.apply(MonitorReading(generation: 4, status: status, snapshot: nil, error: nil, receivedAt: .now))
    #expect(model.error == nil)
    #expect(model.snapshot == nil)
    #expect(model.selected?.node.name == "pi")
  }

  @Test(arguments: [
    TerminalSize(columns: 60, rows: 22), TerminalSize(columns: 80, rows: 24), TerminalSize(columns: 140, rows: 42)
  ])
  func rendersMembershipAndBuildsAtSupportedSizes(size: TerminalSize) throws {
    var model = MonitorModel()
    model.apply(
      reading(status: fixtureStatus(process: UUID()), uptime: 12, inbound: 6, outbound: 9, generation: 1))
    model.move(2)
    let text = try render(model, size: size)
    #expect(text.contains("INDRA / CLUSTER MONITOR"))
    #expect(text.contains("LIVE"))
    #expect(text.contains("tower"))
    #expect(text.contains("pi"))
    #expect(text.contains("dirty"))
    #expect(text.contains("RECENT ACTIVITY"))
    #expect(text.contains("Q quit"))
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

  @Test func maximumViewportKeepsScrolledSelectionAndActivityWithinTheLayoutBudget() throws {
    let original = fixtureStatus(process: UUID())
    let peers = (0..<256).map { index in
      Member(
        node: NodeIdentity(
          id: UUID(), name: "peer-\(index)",
          endpoint: ActorEndpoint(host: "127.0.0.1", port: 10_000 + index, node: UUID()), build: .current),
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
    model.apply(MonitorReading(generation: 1, status: status, snapshot: snapshot, error: nil, receivedAt: .now))
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
    model.apply(reading(status: fixtureStatus(process: UUID()), uptime: 10, inbound: 0, outbound: 0, generation: 1))
    model.apply(
      MonitorReading(
        generation: 2, status: nil, snapshot: nil,
        error: String(repeating: "e\u{301}", count: 5000), receivedAt: .now))
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
  try surface.render(size: size, content: { MonitorView(model: model, size: size) })
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
    id: UUID(), name: "tower", endpoint: ActorEndpoint(host: "127.0.0.1", port: 7331, node: process), build: build)
  let peers = ["studio", "king-dog", "pi"].enumerated().map { index, name in
    Member(
      node: NodeIdentity(
        id: UUID(), name: name,
        endpoint: ActorEndpoint(host: "127.0.0.1", port: 7332 + index, node: UUID()), build: build),
      state: index == 2 ? .suspect : .alive)
  }
  return NodeStatus(
    node: node, cluster: "test", members: peers, seeds: [], configurationError: nil, discoveryErrors: [:])
}

private func reading(status: NodeStatus, uptime: Double, inbound: Int, outbound: Int, generation: UInt64)
  -> MonitorReading
{
  var incoming = ProbeTotals()
  var outgoing = ProbeTotals()
  for _ in 0..<inbound { incoming.record(.ack) }
  for _ in 0..<outbound { outgoing.record(.ping) }
  let snapshot = MonitorSnapshot(
    status: status, uptimeSeconds: uptime, protocolPeriod: 1, localHealthMultiplier: 0,
    probeIntervalMilliseconds: 3000, requestTimeoutMilliseconds: 2000, outbound: outgoing, inbound: incoming,
    inFlight: 0, events: [], changes: [])
  return MonitorReading(generation: generation, status: status, snapshot: snapshot, error: nil, receivedAt: .now)
}
