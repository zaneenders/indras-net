import Foundation
import TestUtils
import Testing

@testable import IndrasNet

@Suite struct PersistFailureSafetyTests {

  /// A `RaftStore` whose `save` always fails.
  struct FailingRaftStore: RaftStore {
    private struct PersistFailed: Error {}

    func load() async throws -> PersistentRaftState? { nil }
    func save(_ state: PersistentRaftState) async throws { throw PersistFailed() }
  }

  /// A `NodeTransport` that records outbound sends instead of delivering them.
  actor RecordingTransport: NodeTransport {
    private(set) var sent: [(message: RaftMessage, to: PeerId)] = []

    func start(onMessage: @escaping IndrasNetInboundHandler) async throws {}
    func shutdown() async throws {}
    func listenPort() async -> Int? { 1 }
    func connectedPeers() async -> Set<PeerId> { [] }
    func isConnected(to peer: PeerId) async -> Bool { true }
    func waitForConnection(to peer: PeerId, timeout: Duration) async -> Bool { true }
    func connect(to peer: NodeAddress) async {}
    func send(_ message: RaftMessage, to peer: PeerId) async throws {
      sent.append((message, peer))
    }
  }

  @Test func followerDoesNotGrantVoteItFailedToPersist() async throws {
    let transport = RecordingTransport()
    let follower = Shell(
      NodeAddress(host: "sim", port: 1),
      transport: transport,
      store: FailingRaftStore(),
      rng: SeededRandomNumberGenerator(seed: 1),
      timerSleep: { _ in try? await Task.sleep(for: .seconds(3600)) },
      logger: TestHelpers.quietLogger
    )
    _ = try await follower.start(with: [])

    // Candidate "a" requests a vote in term 1 from a follower whose store fails.
    let request = RequestVote.Args(term: 1, candidateId: "a", lastLogIndex: 0, lastLogTerm: 0)
    await follower.receiveMessage(message: .requestVote(request), from: "a")

    // Collect any vote reply the follower emitted (or time out if it stayed silent).
    let reply = await TestHelpers.pollForValue(timeout: .milliseconds(500)) {
      await transport.sent.first { message, _ in
        if case .requestVoteReply = message { return true }
        return false
      }?.message
    }

    try await follower.shutdown()

    // A grant here is a safety violation: the vote was never durably recorded,
    // so a crash/restart could re-grant the same term's vote to a different
    // candidate and elect two leaders in one term.
    if case .requestVoteReply(let payload)? = reply {
      #expect(!payload.granted, "follower granted a vote it failed to persist")
    }
  }
}
