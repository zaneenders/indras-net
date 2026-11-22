import Foundation
import IndraNode
import IndraRaft
import Slate
import TCPActorSystem

struct MonitorView: Block {
  static let layoutBudget = LayoutBudget(nodeCapacity: 4096, maximumBlockVisits: 16_384)

  let model: MonitorModel
  let size: TerminalSize
  let raftOverlay: MonitorRaftOverlay?
  let tabHits: HitMap<MonitorTab>

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
    } else {
      Column(gap: 0, alignment: .topLeading) {
        blankLine
        header
        tabs
        if let raftOverlay {
          switch raftOverlay {
          case .inspection(let inspection):
            raftInspection(inspection)
            MonitorLine(text: "PgUp/PgDn page  ←→ pan  l/S view  R reload  Esc back", style: MonitorPalette.muted)
          case .write(let write):
            raftWrite(write)
          }
        } else if model.tab == .raft {
          raftDashboard
          MonitorLine(text: raftControls, style: MonitorPalette.muted)
        } else if model.showDetails {
          details.frame(width: .fill, height: .fill, alignment: .topLeading)
          MonitorLine(text: "↑↓ node  ←→ services  Esc back  Q quit", style: MonitorPalette.muted)
        } else {
          summary.frame(width: .fill, height: .fixed(4), alignment: .topLeading)
          memberPanels.frame(width: .fill, height: .fill, alignment: .topLeading)
          blankLine
          MonitorPanel(title: "RECENT ACTIVITY / newest first") {
            lines(activityLines)
          }.frame(width: .fill, height: .fixed(activityHeight), alignment: .topLeading)
          MonitorLine(
            text: "↑↓ node  \(size.columns >= 110 ? "←→ services  " : "")P ping  ↵ info  Q quit",
            style: MonitorPalette.muted)
        }
      }.padding(horizontal: 2, vertical: 0)
    }
  }

  private var blankLine: some Block { MonitorLine(text: "", style: MonitorPalette.normal) }

  private var tabs: some Block {
    Row(gap: 1, alignment: .topLeading) {
      tab(.swim)
      tab(.raft)
      if raftOverlay == nil {
        Text("H/L tabs", style: MonitorPalette.muted)
      }
    }
    .frame(width: .fill, height: .fixed(1), alignment: .topLeading)
    .background(MonitorPalette.tabBarBackground)
  }

  @BlockBuilder private func tab(_ tab: MonitorTab) -> some Block {
    let active = model.tab == tab
    let style = Style(
      foreground: active ? MonitorPalette.accent.foreground : MonitorPalette.muted.foreground,
      background: active ? MonitorPalette.selectionBackground : MonitorPalette.tabBarBackground,
      bold: active, dim: false, underlined: false, inverse: false)
    let label = Text(" \(tab.rawValue) ", style: style)
    if raftOverlay == nil {
      label.hitRegion(tab, in: tabHits)
    } else {
      label
    }
  }

  private var raftControls: String {
    guard model.raftGroup != nil else { return "Q quit" }
    let group = (model.raftGroups?.count ?? 0) > 1 ? "←→ group  " : ""
    return "↑↓ replica  \(group)l log  S snapshot  W write  Q quit"
  }
  private var activityHeight: Int { size.rows >= 32 ? 10 : 6 }
  // These capacities bound list construction to the space left by the fixed sections.
  private var memberRows: Int { max(0, size.rows - activityHeight - 14) }
  private var detailRows: Int { model.showDetails ? size.rows - 8 : memberRows + 1 }
  private var memberPanelWidth: Int {
    size.columns >= 110 ? ((size.columns - 4) * 3) / 5 - 1 : size.columns - 4
  }

  private var header: some Block {
    let mode = model.error != nil ? "DISCONNECTED" : model.status == nil ? "CONNECTING" : "LIVE"
    let age = model.receivedAt.map { "  \(max(0, $0.duration(to: .now).components.seconds))s ago" } ?? ""
    return Column(gap: 0, alignment: .topLeading) {
      Row(gap: 1, alignment: .topLeading) {
        MonitorLine(text: "INDRA / CLUSTER MONITOR", style: MonitorPalette.accent)
        MonitorLine(text: mode + age, style: model.error != nil ? MonitorPalette.bad : MonitorPalette.good)
          .frame(width: .fixed(24), height: .fixed(1), alignment: .topLeading)
      }.frame(width: .fill, height: .fixed(1), alignment: .topLeading)
      if let status = model.status {
        let role = model.tab == .raft && model.isRaftClient ? " / Raft client" : ""
        MonitorLine(
          text:
            "\(status.cluster) / \(status.node.name) / \(address(status.node))\(role)",
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
    if size.columns >= 110 {
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
    MonitorPanel(title: model.selected == nil && model.serviceNode != nil ? "LOCAL NODE" : "SELECTED PEER") {
      lines(detailLines)
    }
  }

  private var detailLines: [MonitorLine] {
    guard let node = model.serviceNode else {
      return [MonitorLine(text: "Select a peer to inspect", style: MonitorPalette.muted)]
    }
    let state = model.selected?.state ?? .alive
    var result = [
      MonitorLine(text: "\(node.name) / \(state.rawValue)", style: MonitorPalette.state(state))
    ]
    let registrations = model.selectedNodeServices
    var summary = [address(node), "Build: \(node.build?.summary ?? "unknown")"]
    if case .responded(let status) = model.selectedRaftReport {
      summary.append("Raft: \(status.role) / term \(status.term) / voters \(status.voters)")
      summary.append("Commit \(status.commitIndex) / applied \(status.appliedIndex)")
    } else if registrations?.contains(where: { $0.key == RaftStatus.serviceName }) == true {
      summary.append("Raft: status unavailable")
    }
    let serviceRows = registrations?.isEmpty == false ? 2 : 1
    result += summary.prefix(max(0, detailRows - 1 - serviceRows)).map {
      MonitorLine(text: $0, style: MonitorPalette.normal)
    }
    result += serviceLines(registrations, capacity: detailRows - result.count)

    var metadata = [
      "Node: \(node.id.uuidString)", "Process: \(node.endpoint.systemID.uuidString)"
    ]
    if let build = node.build { metadata.append("Commit: \(build.commit)") }
    if let status = model.status, status.seeds.contains(PeerAddress(host: node.endpoint.host, port: node.endpoint.port))
    {
      metadata.append("Configured seed")
    }
    if let snapshot = model.snapshot {
      metadata.append("Observer up \(Int(snapshot.uptimeSeconds))s / LHM \(snapshot.localHealthMultiplier)")
      metadata.append(
        "Probe \(snapshot.probeIntervalMilliseconds)ms / timeout \(snapshot.requestTimeoutMilliseconds)ms (base)")
      metadata.append("Observer OUT ping \(snapshot.outbound.ping) / indirect \(snapshot.outbound.pingRequest)")
      metadata.append("Observer IN ack \(snapshot.inbound.ack) / nack \(snapshot.inbound.nack)")
    }
    result += metadata.prefix(detailRows - result.count).map { MonitorLine(text: $0, style: MonitorPalette.normal) }
    return result
  }

  private func serviceLines(_ registrations: [ServiceRegistration]?, capacity: Int) -> [MonitorLine] {
    guard let registrations, !registrations.isEmpty else {
      return [MonitorLine(
        text: registrations == nil ? "Service listing unavailable" : "No services discovered for this node",
        style: MonitorPalette.muted)]
    }
    let selected = min(model.selectedServiceIndex, registrations.count - 1)
    let showActor = model.showDetails && capacity >= 3
    let visible = capacity - (showActor ? 2 : 1)
    let scrolls = registrations.count > visible
    let first = max(0, selected - visible + 1)
    let end = min(registrations.count, first + visible)
    let range = scrolls ? "  \(first + 1)–\(end) of \(registrations.count)" : ""
    var rows = [MonitorLine(text: "SERVICES\(range)", style: MonitorPalette.muted)]
    rows += (first..<end).map { index in
      let registration = registrations[index]
      let highlighted = index == selected
      return MonitorLine(
        text: "\(highlighted ? "› " : "")\(registration.key)  \(registration.actorType)",
        style: highlighted ? MonitorPalette.selected : MonitorPalette.normal)
    }
    if showActor {
      rows.append(MonitorLine(text: "Actor  \(registrations[selected].address.id.uuidString)", style: MonitorPalette.muted))
    }
    return rows
  }

  @BlockBuilder private var raftDashboard: some Block {
    if let group = model.raftGroup {
      raftSummary(group)
        .frame(width: .fill, height: .fixed(group.configuration?.previousVoters == nil ? 4 : 5), alignment: .topLeading)
      raftReplicas(group).frame(width: .fill, height: .fill, alignment: .topLeading)
      raftDetail(group.replicas[model.selectedRaftReplicaIndex], in: group)
        .frame(width: .fill, height: .fixed(6), alignment: .topLeading)
    } else {
      MonitorPanel(title: "RAFT") {
        lines([MonitorLine(
          text: model.snapshot == nil ? "Waiting for daemon…" :
            model.raftGroups == nil ? "Raft status unavailable" : "No Raft replicas responding",
          style: MonitorPalette.muted)])
      }.frame(width: .fill, height: .fill, alignment: .topLeading)
    }
  }

  private func raftSummary(_ group: MonitorRaftGroup) -> some Block {
    let response = group.responding
    let leader = group.conflictingLeaders ? "Conflicting leaders" :
      group.leader.map { "Leader \($0)" } ?? "No leader observed"
    var summary = [
      MonitorLine(text: "\(leader) / term \(group.term) / commit \(group.commit)",
                  style: group.leader == nil ? MonitorPalette.warning : MonitorPalette.good)
    ]
    if let configuration = group.configuration {
      let voterCount = configuration.voters.filter { response.contains($0) }.count
      let learnerCount = configuration.readers.filter { response.contains($0) }.count
      summary.append(MonitorLine(
        text: "Reporting   voters \(voterCount)/\(configuration.voters.count)   learners \(learnerCount)/\(configuration.readers.count)",
        style: MonitorPalette.normal))
      if let previous = configuration.previousVoters {
        let previousCount = previous.filter { response.contains($0) }.count
        summary.append(MonitorLine(
          text: "Joint config   previous voters \(previousCount)/\(previous.count) reporting",
          style: MonitorPalette.warning))
      }
    } else {
      summary.append(MonitorLine(
        text: "\(group.voters) voters / \(group.learners) learners / \(response.count) reporting",
        style: MonitorPalette.normal))
    }
    return MonitorPanel(title: "RAFT / \(group.name)") { lines(summary) }
  }

  private func raftReplicas(_ group: MonitorRaftGroup) -> some Block {
    let wide = size.columns >= 100
    let selected = model.selectedRaftReplicaIndex
    let capacity = max(1, size.rows - (group.configuration?.previousVoters == nil ? 19 : 20))
    let first = max(0, selected - capacity + 1)
    let rows = group.replicas.indices.dropFirst(first).prefix(capacity).map { index in
      let peer = group.replicas[index]
      let status = peer.status
      let style = index == selected ? MonitorPalette.selected :
        status == nil ? MonitorPalette.muted : status?.role == "leader" ? MonitorPalette.good : MonitorPalette.normal
      return MonitorRaftColumns(
        marker: index == selected ? "›" : " ", name: peer.name, role: peer.role,
        term: indexLabel(status?.term, digits: 5), commit: indexLabel(status?.commitIndex, digits: 7),
        applied: indexLabel(status?.appliedIndex, digits: 7), snapshot: indexLabel(status?.snapshotIndex, digits: 9),
        lag: indexLabel(peer.lag(behind: group.commit), digits: 5),
        progress: wide ? peer.progress(to: group.commit, width: 12) ?? "—" : "",
        wide: wide, style: style)
    }
    return MonitorPanel(title: "REPLICAS") {
      Column(gap: 0, alignment: .topLeading) {
        MonitorRaftColumns(
          marker: " ", name: "NAME", role: "ROLE", term: "TERM", commit: "COMMIT", applied: "APPLIED",
          snapshot: "SNAPSHOT", lag: "LAG", progress: "PROGRESS", wide: wide, style: MonitorPalette.muted)
        BlockList(rows[...])
      }.padding(horizontal: 1, vertical: 0)
    }
  }

  private func raftDetail(_ replica: MonitorRaftGroup.Replica, in group: MonitorRaftGroup) -> some Block {
    var content = [MonitorLine(
      text: [replica.membership?.rawValue, replica.status?.role ?? replica.role].compactMap { $0 }
        .joined(separator: " / "),
      style: replica.status == nil ? MonitorPalette.warning : MonitorPalette.normal)]
    if let status = replica.status {
      let lag = group.commit - min(group.commit, status.appliedIndex)
      content.append(MonitorLine(
        text: "Commit \(status.commitIndex) / applied \(status.appliedIndex)", style: MonitorPalette.normal))
      content.append(MonitorLine(
        text: "Snapshot \(status.snapshotIndex) / lag \(lag) / group commit \(group.commit)", style: MonitorPalette.normal))
      if let bar = replica.progress(to: group.commit, width: size.columns >= 100 ? 24 : 10) {
        content.append(MonitorLine(text: "\(bar)  ░ snap  █ applied  ▒ to apply  · behind", style: MonitorPalette.accent))
      }
    }
    return MonitorPanel(title: replica.name) { lines(content) }
  }

  private func raftInspection(_ inspection: MonitorRaftInspection) -> some Block {
    var rows: [MonitorLine] = []
    func add(_ text: String, style: Style) { rows.append(MonitorLine(text: text, style: style)) }
    if inspection.pending != nil {
      add("Loading…", style: MonitorPalette.muted)
    } else if let failure = inspection.failure {
      add(failure, style: MonitorPalette.bad)
    } else {
      switch inspection.reply {
      case .log(let page):
        add("Last \(page.lastIndex) / commit \(page.commitIndex) / applied \(page.appliedIndex)", style: MonitorPalette.normal)
        if page.compactedThrough > 0 {
          add("Compacted through \(page.compactedThrough) · open S snapshot", style: MonitorPalette.muted)
        }
        if page.entries.isEmpty { add("No retained log entries on this page.", style: MonitorPalette.muted) }
      case .snapshot(let page?):
        add("Index \(page.index) / term \(page.term) / \(page.receiptCount) write receipts", style: MonitorPalette.normal)
        if let configuration = page.configuration {
          let joint = configuration.previousVoters.map { " / \($0.count) previous voters" } ?? ""
          add("\(configuration.voters.count) voters / \(configuration.readers.count) learners\(joint)", style: MonitorPalette.muted)
        }
        if page.values.isEmpty {
          add("No keys in the snapshot.", style: MonitorPalette.muted)
        } else {
          add("Keys \(page.offset + 1)–\(page.offset + page.values.count) of \(page.valueCount)", style: MonitorPalette.muted)
        }
      case .snapshot(nil):
        add("No snapshot saved on this replica yet.", style: MonitorPalette.muted)
      case nil: break
      }
      for record in inspection.records {
        let prefix = inspection.textOffset == 0 ? "" : "‹"
        add(prefix + record.dropFirst(inspection.textOffset), style: MonitorPalette.normal)
      }
    }
    return MonitorPanel(title: inspection.title) { lines(rows) }
      .frame(width: .fill, height: .fill, alignment: .topLeading)
  }

  private func raftWrite(_ write: MonitorRaftWrite) -> some Block {
    MonitorPanel(title: "WRITE / \(write.name)") {
      Column(gap: 0, alignment: .topLeading) {
        InputBox(
          write.key.input, focused: write.focus == .key && write.commandLine == nil && write.pending == nil,
          placeholder: "Type the key for this value", style: MonitorPalette.normal)
          .frame(width: .fill, height: .fixed(1), alignment: .topLeading)
          .border(" Raft key ", style: write.focus == .key ? MonitorPalette.accent : MonitorPalette.border)
        Row(gap: 1, alignment: .topLeading) {
          Text(" Value type", style: write.focus == .format ? MonitorPalette.accent : MonitorPalette.muted)
          writeFormat(.dictionary, form: write)
          writeFormat(.text, form: write)
          if write.focus == .value && write.dictionary.isBrowsing && write.commandLine == nil && write.pending == nil {
            Text("Shift+Tab to change", style: MonitorPalette.muted)
          }
        }.frame(width: .fill, height: .fixed(1), alignment: .topLeading)
        let border = write.focus == .value ? MonitorPalette.accent : MonitorPalette.border
        if write.format == .dictionary {
          Canvas(write.dictionary)
            .frame(width: .fill, height: .fixed(min(size.rows - 15, write.dictionary.preferredHeight)), alignment: .topLeading)
            .border(" Dictionary value ", style: border)
        } else {
          InputBox(
            write.value.input, focused: write.focus == .value && write.commandLine == nil && write.pending == nil,
            placeholder: "Type the text value", style: MonitorPalette.normal)
            .frame(width: .fill, height: .fixed(1), alignment: .topLeading)
            .border(" Text value ", style: border)
        }
        if let command = write.commandLine {
          Row(gap: 0, alignment: .topLeading) {
            Text(" :", style: MonitorPalette.accent)
            InputBox(command, focused: true, placeholder: "", style: MonitorPalette.normal)
              .frame(width: .fill, height: .fixed(1), alignment: .topLeading)
          }.frame(width: .fill, height: .fixed(1), alignment: .topLeading)
        } else {
          MonitorLine(text: "", style: MonitorPalette.muted)
        }
        MonitorLine(text: write.controls, style: MonitorPalette.muted)
        if write.pending != nil {
          MonitorLine(text: "Waiting for the leader to commit…", style: MonitorPalette.muted)
        } else if let index = write.committed {
          MonitorLine(text: "Committed at index \(index)", style: MonitorPalette.good)
        } else if let failure = write.failure {
          MonitorLine(text: failure, style: MonitorPalette.bad)
        } else if write.focus == .key {
          MonitorLine(text: "A write replaces the value at an existing key.", style: MonitorPalette.muted)
        }
      }.frame(width: .fill, height: .fill, alignment: .topLeading)
    }.frame(width: .fill, height: .fill, alignment: .topLeading)
  }

  private func writeFormat(_ format: MonitorRaftWrite.Format, form: MonitorRaftWrite) -> some Block {
    let selected = form.format == format
    let style = Style(
      foreground: selected ? MonitorPalette.accent.foreground : MonitorPalette.muted.foreground,
      background: selected ? MonitorPalette.selectionBackground : MonitorPalette.tabBarBackground,
      bold: selected, dim: false, underlined: false, inverse: false)
    return Text(selected ? "[\(format.rawValue)]" : " \(format.rawValue) ", style: style)
  }

  private func indexLabel(_ index: UInt64?, digits: Int) -> String {
    guard let index else { return "—" }
    let text = String(index)
    return text.count <= digits ? text : index.formatted(.number.notation(.compactName))
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

private struct MonitorRaftColumns: Block, Sendable {
  let marker: String
  let name: String
  let role: String
  let term: String
  let commit: String
  let applied: String
  let snapshot: String
  let lag: String
  let progress: String
  let wide: Bool
  let style: Style

  var body: some Block {
    Row(gap: 1, alignment: .topLeading) {
      cell(marker, width: 1)
      cell(name, width: wide ? 17 : 11)
      cell(role, width: wide ? 10 : 9)
      cell(term, width: wide ? 6 : 5)
      cell(commit, width: wide ? 9 : 7)
      cell(applied, width: wide ? 9 : 7)
      if wide { cell(snapshot, width: 9) }
      cell(lag, width: wide ? 7 : 5)
      if wide { cell(progress, width: 12) }
    }.frame(width: .fill, height: .fixed(1), alignment: .topLeading)
      .background(marker == "›" ? MonitorPalette.selectionBackground : MonitorPalette.background)
  }

  private func cell(_ text: String, width: Int) -> some Block {
    MonitorLine(text: text, style: style).frame(width: .fixed(width), height: .fixed(1), alignment: .topLeading)
  }
}

private enum MonitorPalette: Sendable {
  static let background = Color.rgb(15, 20, 27)
  static let tabBarBackground = Color.rgb(20, 28, 37)
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
