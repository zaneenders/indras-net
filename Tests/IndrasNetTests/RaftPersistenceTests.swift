import Foundation
import TestUtils
import Testing

@testable import IndrasNet

@Suite struct RaftStoreTests {
  @Test func inMemoryStoreRoundTripsState() async throws {
    let store = InMemoryRaftStore()
    let command = Data("set x=1".utf8)
    let state = PersistentRaftState(
      currentTerm: 4,
      votedFor: "127.0.0.1:9001",
      log: .sentinel + [LogEntry(term: 4, command: command)]
    )

    try await store.save(state)
    let loaded = try #require(try await store.load())

    #expect(loaded == state)
  }

  @Test func fileStoreRoundTripsState() async throws {
    let directory = FileManager.default.temporaryDirectory
      .appendingPathComponent(UUID().uuidString, isDirectory: true)
    defer { try? FileManager.default.removeItem(at: directory) }

    let store = FileRaftStore(directory: directory)
    let command = Data("set y=2".utf8)
    let state = PersistentRaftState(
      currentTerm: 7,
      votedFor: "127.0.0.1:9002",
      log: .sentinel + [LogEntry(term: 7, command: command)]
    )

    try await store.save(state)
    let loaded = try #require(try await store.load())

    #expect(loaded == state)
  }

  @Test func fileStoreReturnsNilWhenMissing() async throws {
    let directory = FileManager.default.temporaryDirectory
      .appendingPathComponent(UUID().uuidString, isDirectory: true)
    defer { try? FileManager.default.removeItem(at: directory) }

    let store = FileRaftStore(directory: directory)
    #expect(try await store.load() == nil)
  }
}

@Suite struct RaftPersistenceTests {
  private let command = Data("set persisted=1".utf8)

  @Test func shellRestoresPersistedLogOnRestart() async throws {
    let root = FileManager.default.temporaryDirectory
      .appendingPathComponent(UUID().uuidString, isDirectory: true)
    defer { try? FileManager.default.removeItem(at: root) }

    let mesh = SimulatedTransport.Mesh()
    let addresses = (0..<3).map { NodeAddress(host: "sim", port: 700 + $0) }

    func makeShell(_ address: NodeAddress) -> Shell<SimulatedTransport> {
      let store = FileRaftStore(
        directory: root.appendingPathComponent(address.addressKey, isDirectory: true))
      return Shell(
        address,
        transport: SimulatedTransport(peer: address, mesh: mesh),
        store: store,
        logger: TestHelpers.quietLogger
      )
    }

    var shells = addresses.map(makeShell)
    for index in shells.indices {
      let peers = addresses.enumerated().filter { $0.offset != index }.map(\.element)
      _ = try await shells[index].start(with: peers)
    }

    let leader = try await waitForLeader(in: shells)
    let reply = await leader.submit(command: command)
    #expect(reply.status == .ok)
    try await waitForReplicated(command: command, in: shells, atIndex: 1)

    let followerIndex = try await followerIndex(in: shells, excludingLeader: leader)
    let followerAddress = addresses[followerIndex]
    let followerStore = FileRaftStore(
      directory: root.appendingPathComponent(followerAddress.addressKey, isDirectory: true))

    await shells[followerIndex].stop()
    shells[followerIndex] = makeShell(followerAddress)
    let peers = addresses.enumerated().filter { $0.offset != followerIndex }.map(\.element)
    _ = try await shells[followerIndex].start(with: peers)

    let restoredLog = await shells[followerIndex].instance.log
    #expect(restoredLog.count >= 2)
    #expect(restoredLog[1].command == command)

    let persisted = try #require(try await followerStore.load())
    #expect(persisted.log.count >= 2)
    #expect(persisted.log[1].command == command)

    for shell in shells {
      try await shell.shutdown()
    }
  }

  private func waitForLeader(
    in shells: [Shell<SimulatedTransport>],
    timeout: Duration = .seconds(5)
  ) async throws -> Shell<SimulatedTransport> {
    await TestHelpers.waitUntil(timeout: timeout) {
      for shell in shells where await shell.instance.role == .leader {
        return true
      }
      return false
    }

    for shell in shells where await shell.instance.role == .leader {
      return shell
    }
    throw MissingLeader()
  }

  private struct MissingLeader: Error {}

  private func followerIndex(
    in shells: [Shell<SimulatedTransport>],
    excludingLeader leader: Shell<SimulatedTransport>
  ) async throws -> Int {
    let leaderID = await leader.instance.id
    for (index, shell) in shells.enumerated() where await shell.instance.id != leaderID {
      return index
    }
    throw MissingLeader()
  }

  private func waitForReplicated(
    command: Data,
    in shells: [Shell<SimulatedTransport>],
    atIndex index: LogIndex,
    timeout: Duration = .seconds(10)
  ) async throws {
    await TestHelpers.waitUntil(timeout: timeout) {
      for shell in shells {
        let log = await shell.instance.log
        guard log.count > Int(index), log[Int(index)].command == command else {
          return false
        }
      }
      return true
    }
  }
}
