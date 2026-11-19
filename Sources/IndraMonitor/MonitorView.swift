import Foundation
import IndraNode
import Slate
import TCPActorSystem

struct MonitorView: Block, Sendable {
  static let layoutBudget = LayoutBudget(nodeCapacity: 4096, maximumBlockVisits: 16_384)

  let model: MonitorModel
  let size: TerminalSize

  var body: some Block {
    content
      .frame(width: .fill, height: .fill, alignment: .topLeading)
      .background(MonitorPalette.background)
  }

  @BlockBuilder private var content: some Block {
    if size.columns < 60 || size.rows < 22 {
      Column(gap: 1, alignment: .topLeading) {
        MonitorLine(text: "INDRA / CLUSTER MONITOR", style: MonitorPalette.accent)
        MonitorLine(text: "Resize to at least 60 columns x 22 rows", style: MonitorPalette.normal)
        MonitorLine(text: "Q quit", style: MonitorPalette.muted)
      }.padding(horizontal: 1, vertical: 1)
    } else if let index = model.selectedServiceIndex {
      Column(gap: 0, alignment: .topLeading) {
        blankLine
        header
        blankLine
        serviceList(index: index)
        MonitorLine(text: "↑↓ select  Esc back  Space pause  Q quit", style: MonitorPalette.muted)
      }.padding(horizontal: 2, vertical: 0)
    } else {
      Column(gap: 0, alignment: .topLeading) {
        blankLine
        header
        blankLine
        summary.frame(width: .fill, height: .fixed(4), alignment: .topLeading)
        memberPanels.frame(width: .fill, height: .fill, alignment: .topLeading)
        blankLine
        MonitorPanel(title: "RECENT ACTIVITY / newest first") {
          lines(activityLines)
        }.frame(width: .fill, height: .fixed(activityHeight), alignment: .topLeading)
        MonitorLine(text: "↑↓ node  S services  P ping  ↵ info  Space pause  Q quit", style: MonitorPalette.muted)
      }.padding(horizontal: 2, vertical: 0)
    }
  }

  private var blankLine: some Block { MonitorLine(text: "", style: MonitorPalette.normal) }
  private var activityHeight: Int { size.rows >= 32 ? 10 : 6 }
  // These capacities bound list construction to the space left by the fixed sections.
  private var memberRows: Int { max(0, size.rows - activityHeight - 14) }
  private var detailRows: Int { memberRows + 1 }
  private var memberPanelWidth: Int {
    size.columns >= 110 ? ((size.columns - 4) * 3) / 5 - 1 : size.columns - 4
  }

  private var header: some Block {
    let mode =
      model.paused ? "PAUSED" : model.error != nil ? "DISCONNECTED" : model.status == nil ? "CONNECTING" : "LIVE"
    let age = model.receivedAt.map { "  \(max(0, $0.duration(to: .now).components.seconds))s ago" } ?? ""
    return Column(gap: 0, alignment: .topLeading) {
      Row(gap: 1, alignment: .topLeading) {
        MonitorLine(text: "INDRA / CLUSTER MONITOR", style: MonitorPalette.accent)
        MonitorLine(
          text: mode + age,
          style: model.error != nil ? MonitorPalette.bad : model.paused ? MonitorPalette.warning : MonitorPalette.good
        )
        .frame(width: .fixed(24), height: .fixed(1), alignment: .topLeading)
      }.frame(width: .fill, height: .fixed(1), alignment: .topLeading)
      if let status = model.status {
        MonitorLine(
          text: "\(status.cluster) / \(status.node.name) / \(address(status.node)) / local SWIM view",
          style: MonitorPalette.muted)
        MonitorLine(
          text: "Daemon \(status.node.build?.summary ?? "unknown build")   Client \(BuildIdentity.current.summary)",
          style: MonitorPalette.muted)
      } else {
        blankLine
        blankLine
      }
    }
  }

  private var summary: some Block {
    let peers = model.status?.members ?? []
    let rates =
      model.error != nil ? "stale / no live rates" : "↓ \(rate(model.inboundRate))  ↑ \(rate(model.outboundRate)) /s"
    let engine =
      model.snapshot.map { "LHM \($0.localHealthMultiplier) / period \($0.protocolPeriod)" } ?? "Awaiting telemetry"
    let trend = sparkline(model.history, width: max(0, (size.columns - 6) / 3 - 4))
    return Row(gap: 1, alignment: .topLeading) {
      MonitorPanel(title: "MEMBERSHIP") {
        Column(gap: 0, alignment: .topLeading) {
          MonitorLine(
            text: "\(peers.filter { $0.state == .alive }.count) alive / \(peers.count) peers",
            style: MonitorPalette.normal)
          MonitorLine(
            text:
              "\(peers.filter { $0.state == .suspect }.count) suspect / \(peers.filter { $0.state == .unreachable || $0.state == .dead }.count) down",
            style: MonitorPalette.normal)
        }.padding(horizontal: 1, vertical: 0)
      }
      MonitorPanel(title: "PROBE TRAFFIC") {
        Column(gap: 0, alignment: .topLeading) {
          MonitorLine(text: rates, style: MonitorPalette.normal)
          MonitorLine(
            text: "\(model.snapshot?.inFlight ?? 0) in flight / \(model.snapshot?.outbound.failures ?? 0) failed",
            style: MonitorPalette.normal)
        }.padding(horizontal: 1, vertical: 0)
      }
      MonitorPanel(title: "SWIM / TRAFFIC TREND") {
        Column(gap: 0, alignment: .topLeading) {
          MonitorLine(text: engine, style: MonitorPalette.normal)
          MonitorLine(text: trend.isEmpty ? "collecting samples…" : trend, style: MonitorPalette.normal)
        }.padding(horizontal: 1, vertical: 0)
      }
    }
  }

  @BlockBuilder private var memberPanels: some Block {
    if model.showDetails {
      details
    } else if size.columns >= 110 {
      Row(gap: 1, alignment: .topLeading) {
        members.frame(width: .fixed(memberPanelWidth), height: .fill, alignment: .topLeading)
        details
      }
    } else {
      members
    }
  }

  private var members: some Block {
    let peers = model.status?.members ?? []
    let first = max(0, (model.selectedIndex ?? 0) - memberRows + 1)
    let wide = memberPanelWidth >= 68
    let rows = peers.dropFirst(first).prefix(memberRows).map {
      MonitorMemberRow(member: $0, selected: $0.node.endpoint.systemID == model.selectedProcess, wide: wide)
    }
    return MonitorPanel(title: "MEMBERS") {
      Column(gap: 0, alignment: .topLeading) {
        if peers.isEmpty {
          blankLine
          MonitorLine(
            text: model.status == nil ? "Connecting to daemon…" : "No peers discovered", style: MonitorPalette.muted
          )
          .padding(horizontal: 1, vertical: 0)
        } else {
          MonitorMemberColumns(
            marker: "", name: "NAME", state: "STATE", address: wide ? "ADDRESS" : nil,
            build: "BUILD", style: MonitorPalette.muted)
          BlockList(rows[...])
        }
      }
    }
  }

  private var details: some Block {
    MonitorPanel(title: "SELECTED PEER") { lines(detailLines) }
  }

  private func serviceList(index: Int) -> some Block {
    let node = model.serviceNode
    let registrations = model.selectedNodeServices
    let capacity = size.rows - 9  // Header, panel borders, actor ID and footer.
    let first = max(0, index - capacity + 1)
    return MonitorPanel(title: "SERVICES / \(node?.name ?? "…")") {
      Column(gap: 0, alignment: .topLeading) {
        if let registrations, !registrations.isEmpty {
          let rows = registrations.indices.dropFirst(first).prefix(capacity).map { row in
            let registration = registrations[row]
            return MonitorLine(
              text: "\(row == index ? "›" : " ") \(registration.key)  \(registration.actorType)",
              style: row == index ? MonitorPalette.selected : MonitorPalette.normal)
          }
          BlockList(rows[...])
          MonitorLine(text: "Actor  \(registrations[index].address.id.uuidString)", style: MonitorPalette.muted)
        } else {
          MonitorLine(
            text: registrations == nil ? "Service listing unavailable" : "No services discovered for this node",
            style: MonitorPalette.muted)
        }
      }.padding(horizontal: 1, vertical: 0)
    }.frame(width: .fill, height: .fill, alignment: .topLeading)
  }

  private var detailLines: [MonitorLine] {
    guard let member = model.selected else {
      return [MonitorLine(text: "Select a peer to inspect", style: MonitorPalette.muted)]
    }
    let node = member.node
    var result = [
      MonitorLine(text: "\(node.name) / \(member.state.rawValue)", style: MonitorPalette.state(member.state))
    ]
    var text = [
      address(node), "Build: \(node.build?.summary ?? "unknown")",
      "Node: \(node.id.uuidString)", "Process: \(node.endpoint.systemID.uuidString)"
    ]
    if let build = node.build { text.append("Commit: \(build.commit)") }
    if let status = model.status, status.seeds.contains(PeerAddress(host: node.endpoint.host, port: node.endpoint.port))
    {
      text.append("Configured seed")
    }
    if let snapshot = model.snapshot {
      text.append("Observer up \(Int(snapshot.uptimeSeconds))s / LHM \(snapshot.localHealthMultiplier)")
      text.append(
        "Probe \(snapshot.probeIntervalMilliseconds)ms / timeout \(snapshot.requestTimeoutMilliseconds)ms (base)")
      text.append("Observer OUT ping \(snapshot.outbound.ping) / indirect \(snapshot.outbound.pingRequest)")
      text.append("Observer IN ack \(snapshot.inbound.ack) / nack \(snapshot.inbound.nack)")
    }
    result += text.prefix(max(0, detailRows - 1)).map { MonitorLine(text: $0, style: MonitorPalette.normal) }
    return result
  }

  private var activityLines: [MonitorLine] {
    let capacity = activityHeight - 2
    var result: [MonitorLine] = []
    if let ping = model.manualPing {
      let style: Style
      switch ping.outcome {
      case .pending: style = MonitorPalette.warning
      case .success: style = MonitorPalette.good
      case .failure: style = MonitorPalette.bad
      }
      result.append(MonitorLine(text: ping.summary, style: style))
    }
    if let error = model.error {
      result.append(MonitorLine(text: "STALE VIEW: \(error)", style: MonitorPalette.bad))
      return result
    }
    guard let snapshot = model.snapshot else {
      result.append(MonitorLine(text: "Waiting for daemon…", style: MonitorPalette.warning))
      return result
    }
    if let error = snapshot.status.configurationError {
      result.append(MonitorLine(text: "CONFIG: \(error)", style: MonitorPalette.bad))
    }
    for change in snapshot.changes.suffix(2).reversed().prefix(max(0, capacity - result.count)) {
      result.append(
        MonitorLine(
          text:
            "+\(Int(change.uptimeSeconds))s SWIM \(change.node.name): \(change.previous?.rawValue ?? "new") → \(change.current.rawValue)",
          style: MonitorPalette.state(change.current)))
    }
    for event in snapshot.events.reversed().prefix(max(0, capacity - result.count)) {
      let kind = event.kind == .pingRequest ? "PING_REQ" : event.kind.rawValue.uppercased()
      var text =
        "+\(Int(event.uptimeSeconds))s \(event.direction == .inbound ? "IN " : "OUT") \(kind) \(event.peer) #\(event.sequenceNumber)"
      if let target = event.target, target != event.peer { text += " → \(target)" }
      if let milliseconds = event.roundTripMilliseconds { text += " \(rate(milliseconds))ms" }
      text += event.error.map { " \($0)" } ?? " gossip=\(event.gossipMembers)"
      result.append(
        MonitorLine(
          text: text,
          style: event.kind == .failure || event.kind == .nack ? MonitorPalette.warning : MonitorPalette.muted))
    }
    return result
  }

  private func lines(_ content: [MonitorLine]) -> some Block {
    Column(gap: 0, alignment: .topLeading) { BlockList(content[...]) }
      .padding(horizontal: 1, vertical: 0)
  }

  private func address(_ node: NodeIdentity) -> String {
    PeerAddress(host: node.endpoint.host, port: node.endpoint.port).description
  }
  private func rate(_ value: Double?) -> String {
    value.map { $0.formatted(.number.precision(.fractionLength(1))) } ?? "—"
  }
  private func sparkline(_ values: [Double], width: Int) -> String {
    let levels = Array("▁▂▃▄▅▆▇█")
    let peak = max(1, values.max() ?? 1)
    return String(values.suffix(width).map { levels[min(7, max(0, Int($0 / peak * 7)))] })
  }
}

private struct MonitorPanel<Content: Block>: Block, ~Sendable {
  let title: String
  let content: Content

  init(title: String, @BlockBuilder content: () -> Content) {
    self.title = title
    self.content = content()
  }

  var body: some Block {
    content.frame(width: .fill, height: .fill, alignment: .topLeading)
      .border(" \(title) ", style: MonitorPalette.border)
  }
}

private struct MonitorLine: Block, Sendable {
  let text: String
  let style: Style

  var body: some Block {
    Text(String(text.unicodeScalars.prefix(LayoutLimits.maximumTextScalars)), style: style)
      .frame(width: .fill, height: .fixed(1), alignment: .topLeading)
  }
}

private struct MonitorMemberRow: Block, Sendable {
  let member: Member
  let selected: Bool
  let wide: Bool

  var body: some Block {
    MonitorMemberColumns(
      marker: selected ? "›" : " ", name: member.node.name, state: member.state.rawValue,
      address: wide ? PeerAddress(host: member.node.endpoint.host, port: member.node.endpoint.port).description : nil,
      build: member.node.build?.summary ?? "unknown",
      style: selected ? MonitorPalette.selected : MonitorPalette.state(member.state)
    )
    .background(selected ? MonitorPalette.selectionBackground : MonitorPalette.background)
  }
}

private struct MonitorMemberColumns: Block, Sendable {
  let marker: String
  let name: String
  let state: String
  let address: String?
  let build: String
  let style: Style

  var body: some Block {
    Row(gap: 0, alignment: .topLeading) {
      MonitorLine(text: marker, style: style).frame(width: .fixed(1), height: .fixed(1), alignment: .topLeading)
      Row(gap: 1, alignment: .topLeading) {
        MonitorLine(text: name, style: style).frame(width: .fixed(12), height: .fixed(1), alignment: .topLeading)
        MonitorLine(text: state, style: style).frame(width: .fixed(12), height: .fixed(1), alignment: .topLeading)
        if let address {
          MonitorLine(text: address, style: style).frame(width: .fixed(21), height: .fixed(1), alignment: .topLeading)
        }
        MonitorLine(text: build, style: style)
      }.frame(width: .fill, height: .fixed(1), alignment: .topLeading)
    }.frame(width: .fill, height: .fixed(1), alignment: .topLeading)
  }
}

private enum MonitorPalette: Sendable {
  static let background = Color.rgb(15, 20, 27)
  static let selectionBackground = Color.rgb(28, 57, 73)
  static let normal = style(.rgb(211, 221, 230), bold: false)
  static let muted = style(.rgb(125, 145, 161), bold: false)
  static let border = style(.rgb(62, 84, 104), bold: false)
  static let accent = style(.rgb(98, 213, 229), bold: true)
  static let good = style(.rgb(124, 211, 155), bold: false)
  static let warning = style(.rgb(240, 191, 105), bold: false)
  static let bad = style(.rgb(240, 128, 128), bold: false)
  static let selected = style(.rgb(226, 245, 249), bold: true)

  static func state(_ state: Member.State) -> Style {
    switch state {
    case .alive: good
    case .suspect: warning
    case .unreachable, .dead: bad
    }
  }

  private static func style(_ color: Color, bold: Bool) -> Style {
    Style(foreground: color, background: .default, bold: bold, dim: false, underlined: false, inverse: false)
  }
}
