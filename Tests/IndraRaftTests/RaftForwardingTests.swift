import Foundation
import IndraNode
import TCPActorSystem
import Testing
import _NIOFileSystem

@testable import IndraRaft

struct RaftForwardingTests {
  @Test func snapshotJoinElectsAnotherLeaderAndPreservesReceipts() async throws {
    try await FileSystem.shared.withTemporaryDirectory(
      prefix: FilePath("/tmp"), options: .init(followSymbolicLinks: false, closeOnExec: true)
    ) { _, root in
      for name in ["tower-store", "alpha-store", "beta-store", "gamma-store"] {
        try await FileSystem.shared.createDirectory(
          at: root.appending(name), withIntermediateDirectories: false, permissions: .ownerReadWriteExecute)
      }
      let tower = try await Daemon.start(
        name: "tower", root: root, seeds: [], directory: root.appending("tower-store").string, mode: .bootstrap)
      defer { await tower.stop() }
      let document = try RaftValue(documentJSON: #"{"details":{"active":true},"items":[1,null,"one"]}"#)
      let original = RaftRequest.put(key: "initial", value: document, requestID: UUID())
      let receipt = try await tower.forward(original, to: nil)
      for index in 1..<RaftHost.snapshotInterval {
        _ = try await tower.forward(.put(key: "key-\(index)", value: .string("value"), requestID: UUID()), to: nil)
      }
      #expect(try await tower.raftStatus().snapshotIndex == RaftHost.snapshotInterval)
      for index in 0...RaftHost.inspectionPageSize {
        _ = try await tower.forward(
          .put(key: "new-\(index)", value: .string("after snapshot"), requestID: UUID()), to: nil)
      }

      let alpha = try await Daemon.start(
        name: "alpha", root: root, seeds: [tower.address], directory: root.appending("alpha-store").string,
        mode: .discover)
      defer { await alpha.stop() }
      let beta = try await Daemon.start(
        name: "beta", root: root, seeds: [tower.address, alpha.address], directory: root.appending("beta-store").string,
        mode: .discover)
      defer { await beta.stop() }
      for replica in [tower, alpha, beta] {
        let status = try await replica.waitForStatus { $0.voters == 3 }
        #expect(status.snapshotIndex >= RaftHost.snapshotInterval)
      }
      let gamma = try await Daemon.start(
        name: "gamma", root: root, seeds: [beta.address], directory: root.appending("gamma-store").string, mode: .discover)
      defer { await gamma.stop() }
      for replica in [tower, alpha, beta, gamma] {
        _ = try await replica.waitForStatus { $0.voters == 4 && $0.configuration?.previousVoters == nil }
      }
      let peer = beta.node.id.uuidString
      let group = tower.node.id.uuidString
      let recent = try await RaftClient.inspect(
        RaftInspection(peer: peer, group: group, query: .log(before: nil)), socket: alpha.socket)
      guard case .log(let page) = recent else { Issue.record("Expected beta’s log"); return }
      #expect(page.compactedThrough == RaftHost.snapshotInterval)
      #expect(page.entries.count == RaftHost.inspectionPageSize)
      let cursor = try #require(page.olderBefore)
      let older = try await RaftClient.inspect(
        RaftInspection(peer: peer, group: group, query: .log(before: cursor)), socket: alpha.socket)
      guard case .log(let previous) = older else { Issue.record("Expected older log entries"); return }
      #expect(previous.entries.first?.index == cursor - 1)
      #expect((page.entries + previous.entries).contains {
        $0.operation == .put(key: "new-\(RaftHost.inspectionPageSize)", value: .string("after snapshot"))
      })
      let saved = try await RaftClient.inspect(
        RaftInspection(peer: peer, group: group, query: .snapshot(offset: 0, at: nil)), socket: alpha.socket)
      guard case .snapshot(let snapshot?) = saved else { Issue.record("Expected beta’s snapshot"); return }
      #expect(snapshot.index == RaftHost.snapshotInterval)
      #expect(snapshot.valueCount == Int(RaftHost.snapshotInterval))
      #expect(snapshot.values.contains { $0.key == "initial" && $0.value == document })
      let offset = try #require(snapshot.nextOffset)
      let later = try await RaftClient.inspect(
        RaftInspection(peer: peer, group: group, query: .snapshot(offset: offset, at: snapshot.index)), socket: alpha.socket)
      guard case .snapshot(let more?) = later else { Issue.record("Expected more snapshot values"); return }
      #expect(more.offset == offset)
      #expect(Set(snapshot.values.map(\.key)).isDisjoint(with: more.values.map(\.key)))
      await tower.stop()
      _ = try await alpha.waitForStatus { $0.leader != nil && $0.leader != tower.node.id.uuidString }

      #expect(try await beta.forward(.get(key: "initial"), to: nil) == .value(document))
      #expect(try await beta.forward(original, to: nil) == receipt)
      _ = try await beta.forward(.put(key: "initial", value: .string("after failover"), requestID: UUID()), to: nil)
      #expect(try await alpha.forward(.get(key: "initial"), to: nil) == .value(.string("after failover")))
    }
  }

  @Test func clientDaemonForwardsWithoutHostingRaft() async throws {
    try await FileSystem.shared.withTemporaryDirectory(
      prefix: FilePath("/tmp"), options: .init(followSymbolicLinks: false, closeOnExec: true)
    ) { _, root in
      let store = root.appending("store")
      try await FileSystem.shared.createDirectory(
        at: store, withIntermediateDirectories: false, permissions: .ownerReadWriteExecute)
      let host = try await Daemon.start(name: "host", root: root, seeds: [], directory: store.string, mode: .bootstrap)
      defer { await host.stop() }
      let client = try await Daemon.start(name: "client", root: root, seeds: [host.address], directory: nil, mode: .bootstrap)
      defer { await client.stop() }
      try await client.waitForRaft(on: host.node)

      let id = UUID()
      let put = RaftRequest.put(key: "example", value: .string("first"), requestID: id)
      let committed = RaftReply.applied(RaftWriteResult(requestID: id, index: 1, removed: nil))
      #expect(try await RaftClient.put(
        key: "example", value: .string("first"), group: host.node.id.uuidString, requestID: id, socket: client.socket) == 1)
      #expect(try await client.forward(put, to: "host") == committed)
      #expect(try await client.forward(put, to: host.node.id.uuidString.lowercased()) == committed)
      #expect(try await client.forward(.get(key: "example"), to: "host") == .value(.string("first")))
      #expect(try await RaftClient.inspect(
        RaftInspection(peer: host.node.id.uuidString, group: host.node.id.uuidString, query: .snapshot(offset: 0, at: nil)),
        socket: client.socket) == .snapshot(nil))
      #expect(try await NodeControl.command(
        RaftStatus.serviceName, input: RaftRequest.status, returning: RaftReply.self, socket: client.socket,
        timeout: .seconds(3)) == .client(client.node.id.uuidString))

      let reports = try await NodeControl.query(
        RaftStatus.serviceName, returning: RaftStatus.self, socket: client.socket, timeout: .seconds(3))
      let report = try #require(reports.first { $0.node.id == host.node.id })
      guard case .responded(let status) = report.outcome else {
        Issue.record("Expected the host to report Raft progress")
        return
      }
      #expect(status.node == host.node.id.uuidString)
      #expect(status.commitIndex == 1)
      #expect(status.appliedIndex == 1)
      #expect(status.voters == 1)
      let local = try #require(reports.first { $0.node.id == client.node.id })
      if case .unavailable = local.outcome {} else { Issue.record("The client advertised a Raft replica") }
    }
  }

  private struct Daemon: Sendable {
    let node: NodeIdentity
    let socket: String
    let task: Task<Void, any Error>

    var address: PeerAddress { PeerAddress(host: node.endpoint.host, port: node.endpoint.port) }

    static func start(
      name: String, root: FilePath, seeds: [PeerAddress], directory: String?, mode: RaftStartMode
    ) async throws -> Self {
      let config = root.appending("\(name).json").string
      let socket = root.appending("run").appending("\(name).sock").string
      try await NodeConfiguration(
        version: 1, id: UUID(), name: name, cluster: "raft-forward-test", listenHost: "127.0.0.1",
        advertisedHost: "127.0.0.1", port: 0, seeds: seeds, probeIntervalMilliseconds: 200,
        requestTimeoutMilliseconds: 1000
      ).save(path: config, overwrite: false)
      let (events, ready) = AsyncThrowingStream<NodeStatus, any Error>.makeStream(bufferingPolicy: .bufferingNewest(1))
      let task = Task {
        do {
          try await runNode(
            configurationPath: config, socket: socket,
            services: [RaftService.installer(directory: directory, mode: mode)]
          ) {
            ready.yield($0)
            ready.finish()
          }
        } catch {
          ready.finish(throwing: error)
          throw error
        }
      }
      do {
        var iterator = events.makeAsyncIterator()
        let status = try #require(try await iterator.next())
        return Self(node: status.node, socket: socket, task: task)
      } catch {
        task.cancel()
        _ = await task.result
        throw error
      }
    }

    func forward(_ request: RaftRequest, to peer: String?) async throws -> RaftReply {
      try await NodeControl.command(
        RaftForward.commandName, input: RaftForward(peer: peer, group: nil, request: request), returning: RaftReply.self,
        socket: socket, timeout: .seconds(3))
    }

    func raftStatus() async throws -> RaftStatus {
      let reply = try await NodeControl.command(
        RaftStatus.serviceName, input: RaftRequest.status, returning: RaftReply.self, socket: socket,
        timeout: .seconds(3))
      guard case .status(let status) = reply else { throw IndraError("Expected a Raft status") }
      return status
    }

    func waitForStatus(_ condition: (RaftStatus) -> Bool) async throws -> RaftStatus {
      let deadline = ContinuousClock.now.advanced(by: .seconds(15))
      repeat {
        if let status = try? await raftStatus(), condition(status) { return status }
        try await Task.sleep(for: .milliseconds(50))
      } while ContinuousClock.now < deadline
      throw IndraError("Raft did not reach the expected state")
    }

    func waitForRaft(on target: NodeIdentity) async throws {
      let deadline = ContinuousClock.now.advanced(by: .seconds(5))
      while ContinuousClock.now < deadline {
        if case .services(let services) = try await NodeControl.request(.services, socket: socket, timeout: .seconds(3)),
          services.contains(where: { $0.node.id == target.id && $0.registration.key == RaftStatus.serviceName })
        {
          return
        }
        try await Task.sleep(for: .milliseconds(25))
      }
      throw IndraError("Raft discovery did not converge")
    }

    func stop() async {
      task.cancel()
      _ = await task.result
    }
  }
}
