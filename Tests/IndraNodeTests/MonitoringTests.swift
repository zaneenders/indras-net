import Foundation
import NIOCore
import TCPActorSystem
import Testing

@testable import IndraNode

@Suite(.timeLimit(.minutes(1)))
struct MonitoringTests {
  @Test func snapshotsObserveTrafficWithoutGeneratingProbes() async throws {
    try await withMonitorRuntime(name: "observer") { first in
      try await withMonitorRuntime(name: "peer") { second in
        let peer = await second.status().node
        let before = await first.monitor()
        #expect(before.outbound.messages == 0)
        _ = try await first.ping("127.0.0.1:\(peer.endpoint.port)")
        let after = await first.monitor()
        #expect(after.outbound.ping == 1)
        #expect(after.inbound.ack == 1)
        #expect(after.inFlight == 0)
        #expect(after.events.count == 2)
        #expect(after.events.last?.roundTripMilliseconds != nil)
        #expect(after.changes.contains { $0.node.id == peer.id && $0.current == .alive })
        #expect(after.status.node.build == .current)
        #expect(after.status.members.first?.node.build == .current)
        for _ in 0..<100 {
          let reading = await first.monitor()
          #expect(reading.outbound.messages == after.outbound.messages)
          #expect(reading.inbound.messages == after.inbound.messages)
          #expect(reading.protocolPeriod == after.protocolPeriod)
          #expect(reading.status.members.first?.state == .alive)
        }
        let receiver = await second.monitor()
        #expect(receiver.inbound.ping == 1)
        #expect(receiver.outbound.ack == 1)
        let decoded = try JSONDecoder().decode(MonitorSnapshot.self, from: JSONEncoder().encode(after))
        #expect(decoded.events.last?.sequenceNumber == after.events.last?.sequenceNumber)
        #expect(decoded.status.node.build == .current)
        let older = NodeIdentity(
          id: UUID(), name: "older",
          endpoint: ActorEndpoint(host: "127.0.0.1", port: 7333, systemID: UUID()), build: nil)
        let stripped = NodeIdentity(id: peer.id, name: peer.name, endpoint: peer.endpoint, build: nil)
        _ = try await first.receive(
          MembershipRequest(
            version: 1, cluster: "test", node: older,
            probe: .ping(
              sequenceNumber: 42, payload: [GossipMember(node: stripped, status: .alive(0), protocolPeriod: 0)])))
        #expect(await first.status().members.first { $0.node.id == peer.id }?.node.build == .current)
      }
    }
  }

  @Test func telemetryHistoryIsBoundedWhileCountersKeepGrowing() {
    var telemetry = MembershipTelemetry(capacity: 128)
    let peer = PeerAddress(host: "127.0.0.1", port: 7331)
    let node = NodeIdentity(
      id: UUID(), name: "peer",
      endpoint: ActorEndpoint(host: peer.host, port: peer.port, systemID: UUID()), build: nil)
    for number in 0..<10_000 {
      telemetry.record(
        direction: .outbound, kind: .ping, peer: peer, target: nil,
        sequenceNumber: UInt32(number), gossipMembers: 1, roundTripMilliseconds: nil, error: nil)
      telemetry.recordChange(node: node, previous: .alive, current: .suspect)
    }
    #expect(telemetry.events.count == 128)
    #expect(telemetry.events.first?.sequenceNumber == 9872)
    #expect(telemetry.events.last?.sequenceNumber == 9999)
    #expect(telemetry.outbound.ping == 10_000)
    #expect(telemetry.changes.count == 128)
  }

  @Test func buildMetadataIsOptionalForOlderNodesAndValidated() throws {
    let old = NodeIdentity(
      id: UUID(), name: "old", endpoint: ActorEndpoint(host: "127.0.0.1", port: 7331, systemID: UUID()), build: nil)
    let data = try JSONEncoder().encode(old)
    #expect(!String(decoding: data, as: UTF8.self).contains("build"))
    #expect(try JSONDecoder().decode(NodeIdentity.self, from: data).build == nil)
    try BuildIdentity.current.validate()
    try BuildIdentity(commit: "unknown", isDirty: nil).validate()
    #expect(BuildIdentity(commit: String(repeating: "a", count: 40), isDirty: true).summary == "aaaaaaaaaaaa dirty")
    #expect(throws: IndraError.self) { try BuildIdentity(commit: "not a commit", isDirty: false).validate() }
  }
}

private func withMonitorRuntime(name: String, body: @Sendable (NodeRuntime) async throws -> Void) async throws {
  let configuration = makeConfiguration(name: name, seeds: [])
  let system = try await TCPActorSystem(
    host: "127.0.0.1", advertisedHost: "127.0.0.1", port: 0,
    callTimeout: .seconds(2), connectTimeout: .seconds(2))
  let runtime = NodeRuntime(
    configuration: configuration, configurationPath: "/unused", peerHistoryPath: nil, system: system)
  do {
    try await runtime.start(services: [])
    try await body(runtime)
    await runtime.shutdown()
  } catch {
    await runtime.shutdown()
    throw error
  }
}
