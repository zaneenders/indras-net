import Foundation
import NIOCore
import Networking
import TCPActorSystem
import Testing

@testable import IndraNode
@testable import TCPActorSystem

@Suite(.timeLimit(.minutes(1)))
struct SWIMMembershipTests {
  @Test func indirectProbeKeepsMemberAliveWhenDirectTCPPathFails() async throws {
    try await withProbeTarget { target, probes in
      try await withSWIMRuntime(name: "origin") { origin in
        try await withSWIMRuntime(name: "intermediary") { intermediary in
          _ = try await intermediary.ping(target.address.description)
          let intermediaryIdentity = await intermediary.status().node
          _ = try await origin.ping(intermediaryIdentity.address.description)
          _ = try await origin.ping(target.address.description)
          let originIdentity = await origin.status().node
          await probes.block(originIdentity.id)
          await #expect(throws: (any Error).self) { try await origin.ping(target.name) }
          let before = try #require(await origin.status().members.first { $0.node.id == target.id })
          #expect(before.state == .alive)
          #expect(before.reachability == .unreachable)
          let intermediaryProbes = await probes.count(intermediaryIdentity.id)
          let membership = Task { try await origin.runMembership() }
          do {
            try await waitForMembership {
              let member = await origin.status().members.first { $0.node.id == target.id }
              let indirectProbes = await probes.count(intermediaryIdentity.id)
              return indirectProbes > intermediaryProbes && member?.state == .alive
                && member?.reachability == .unreachable
            }
            membership.cancel()
            do { try await membership.value } catch is CancellationError {}
          } catch {
            membership.cancel()
            _ = try? await membership.value
            throw error
          }
        }
      }
    }
  }

  @Test func lifeguardExtendsProbeDeadlineAndDiagnosticPingKeepsItsOwnDeadline() async throws {
    try await withProbeTarget { target, probes in
      try await withSWIMRuntime(name: "origin") { origin in
        _ = try await origin.ping(target.address.description)
        let node = await origin.status().node
        await probes.block(node.id)
        let membership = Task { try await origin.runMembership() }
        do {
          try await waitForMembership { await origin.status().members.first?.state == .suspect }
          await probes.allow(node.id, delay: .milliseconds(250))
          try await waitForMembership {
            let member = await origin.status().members.first
            return member?.state == .alive && member?.reachability == .reachable
          }
          await #expect(throws: IndraError.self) { try await origin.ping(target.name) }
          membership.cancel()
          do { try await membership.value } catch is CancellationError {}
        } catch {
          membership.cancel()
          _ = try? await membership.value
          throw error
        }
      }
    }
  }

  @Test func previousLocalProcessInGossipCannotReplaceCurrentProcess() async throws {
    try await withSWIMRuntime(name: "current") { runtime in
      let current = await runtime.status().node
      let previous = NodeIdentity(
        id: current.id, name: current.name,
        endpoint: ActorEndpoint(
          host: current.endpoint.host, port: current.endpoint.port, node: UUID()))
      let peer = NodeIdentity(
        id: UUID(), name: "peer",
        endpoint: ActorEndpoint(
          host: "127.0.0.1", port: 7332, node: UUID()))
      let response = try await runtime.receive(
        MembershipRequest(
          version: 1, cluster: "test", node: peer,
          probe: .ping(
            sequenceNumber: 1,
            payload: [
              GossipMember(node: previous, status: .alive(0), protocolPeriod: 0),
              GossipMember(node: peer, status: .alive(0), protocolPeriod: 0)
            ])))
      #expect(response.node == current)
      let status = await runtime.status()
      #expect(status.members.count == 1)
      #expect(status.members.first?.node == peer)
      await #expect(throws: IndraError.self) {
        try await runtime.receive(
          MembershipRequest(
            version: 1, cluster: "test", node: peer,
            probe: .pingRequest(target: previous, sequenceNumber: 2, payload: [])))
      }
      #expect(await runtime.status().node == current)
    }
  }

  @Test func indirectProbeReturnsNackWhenTargetIsUnavailable() async throws {
    let reservation = try await TCP.listen(host: "127.0.0.1", port: 0)
    let target = NodeIdentity(
      id: UUID(), name: "offline",
      endpoint: ActorEndpoint(
        host: "127.0.0.1", port: try #require(reservation.channel.localAddress?.port), node: UUID()))
    try await reservation.channel.close()
    try await withSWIMRuntime(name: "intermediary") { runtime in
      let origin = NodeIdentity(
        id: UUID(), name: "origin",
        endpoint: ActorEndpoint(
          host: "127.0.0.1", port: 7332, node: UUID()))
      let reply = try await runtime.receive(
        MembershipRequest(
          version: 1, cluster: "test", node: origin,
          probe: .pingRequest(target: target, sequenceNumber: 42, payload: [])))
      guard case .nack(let node, let sequenceNumber) = reply.response else {
        Issue.record("Expected a negative acknowledgement")
        return
      }
      #expect(node == target)
      #expect(sequenceNumber == 42)
    }
  }
}

private actor ProbeHistory {
  private var blocked: UUID?
  private var counts: [UUID: Int] = [:]
  private var incarnation: UInt64 = 0
  private var delays: [UUID: Duration] = [:]

  func block(_ id: UUID) { blocked = id }
  func count(_ id: UUID) -> Int { counts[id] ?? 0 }

  func allow(_ id: UUID, delay: Duration) {
    if blocked == id { blocked = nil }
    delays[id] = delay
  }

  func record(_ id: UUID) async throws -> UInt64 {
    counts[id, default: 0] += 1
    guard id != blocked else { throw RPCError.timedOut }
    try await Task.sleep(for: delays[id] ?? .zero)
    incarnation += 1
    return incarnation
  }
}

private func withProbeTarget(body: @Sendable (NodeIdentity, ProbeHistory) async throws -> Void) async throws {
  let listener = try await TCP.listen(host: "127.0.0.1", port: 0)
  let target = NodeIdentity(
    id: UUID(), name: "target",
    endpoint: ActorEndpoint(
      host: "127.0.0.1", port: try #require(listener.channel.localAddress?.port), node: UUID()))
  let probes = ProbeHistory()
  let server = Task { try await serveProbeTarget(listener: listener, target: target, history: probes) }
  do {
    try await body(target, probes)
    server.cancel()
    try? await listener.channel.close()
    _ = try? await server.value
  } catch {
    server.cancel()
    try? await listener.channel.close()
    _ = try? await server.value
    throw error
  }
}

private func serveProbeTarget(listener: Listener, target: NodeIdentity, history: ProbeHistory) async throws {
  try await withTaskCancellationHandler {
    try await listener.executeThenClose { connections in
      try await withThrowingDiscardingTaskGroup { clients in
        for try await connection in connections {
          clients.addTask {
            try await connection.executeThenClose { input, output in
              try await output.write(WireMessage.hello(target.endpoint).frame())
              let address = ActorAddress(endpoint: target.endpoint, id: UUID())
              for try await frame in input {
                switch try WireMessage(frame: frame) {
                case .hello: break
                case .lookup(let id, _, _):
                  try await output.write(WireMessage.reply(id, .value(try JSONEncoder().encode(address))).frame())
                case .call(let invocation):
                  let request = try JSONDecoder().decode(
                    MembershipRequest.self, from: #require(invocation.arguments.first))
                  let result: CallResult
                  do {
                    let incarnation = try await history.record(request.node.id)
                    let reply = MembershipReply(
                      version: 1, cluster: "test", node: target,
                      response: .ack(
                        target: target, incarnation: incarnation,
                        payload: [GossipMember(node: target, status: .alive(incarnation), protocolPeriod: 0)],
                        sequenceNumber: request.probe.sequenceNumber))
                    result = .value(try JSONEncoder().encode(reply))
                  } catch {
                    result = .systemError(.timedOut)
                  }
                  try await output.write(WireMessage.reply(invocation.requestID, result).frame())
                case .reply: throw IndraError("Unexpected reply")
                }
              }
            }
          }
        }
      }
    }
  } onCancel: {
    listener.channel.close(promise: nil)
  }
}

private func withSWIMRuntime(name: String, body: @Sendable (NodeRuntime) async throws -> Void) async throws {
  let configuration = makeConfiguration(name: name, seeds: [])
  let system = try await TCPActorSystem(
    host: configuration.listenHost, advertisedHost: configuration.advertisedHost,
    port: configuration.port,
    callTimeout: .milliseconds(NodeRuntime.transportTimeoutMilliseconds(configuration: configuration)),
    connectTimeout: .milliseconds(NodeRuntime.transportTimeoutMilliseconds(configuration: configuration)))
  let runtime = NodeRuntime(configuration: configuration, configurationPath: "/unused", system: system)
  do {
    try await runtime.start()
    try await body(runtime)
    await runtime.shutdown()
  } catch {
    await runtime.shutdown()
    throw error
  }
}

private func waitForMembership(_ condition: @Sendable () async throws -> Bool) async throws {
  let deadline = ContinuousClock.now.advanced(by: .seconds(10))
  while try await !condition() {
    guard ContinuousClock.now < deadline else { throw IndraError("Membership did not converge") }
    try await Task.sleep(for: .milliseconds(25))
  }
}
