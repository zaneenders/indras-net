import ArgumentParser
import Foundation
import SetuNetworking
import Testing

@testable import Setu
@testable import SetuCLI

@Suite(.timeLimit(.minutes(1)))
struct BatchTests {
  @Test(arguments: [1, 3])
  func statusFailuresNeverReleaseAnUnfinishedJobsSlot(pollFailures: Int) async throws(any Error) {
    let control = UnreliableControl(pollFailures: pollFailures, loseSubmissionReply: false)
    do {
      try await sendBatch(
        file: "/tmp/source", targets: batchTargets, destination: nil, createDestination: false, parallel: 1, wait: true,
        request: { try await control.request($0) }, format: OutputOptions.parse(["--json"]))
      #expect(pollFailures == 1)
    } catch let error as ExitCode {
      #expect(pollFailures == 3 && error == .failure)
    }
    #expect(await control.firstPolls == (pollFailures == 1 ? 2 : 3))
    #expect(await control.submitted == (pollFailures == 1 ? ["tower", "pi"] : ["tower"]))
    #expect(!(await control.overlapped))
  }

  @Test func lostSubmissionRepliesStopTheQueueWithoutReplayingTheRequest() async throws(any Error) {
    let control = UnreliableControl(pollFailures: 0, loseSubmissionReply: true)
    do {
      try await sendBatch(
        file: "/tmp/source", targets: batchTargets, destination: nil, createDestination: false, parallel: 1, wait: true,
        request: { try await control.request($0) }, format: OutputOptions.parse(["--json"]))
      Issue.record("Expected the lost submission reply to fail the batch")
    } catch let error as ExitCode { #expect(error == .failure) }
    #expect(await control.submitted == ["tower"])
    #expect(await control.firstPolls == 0)
    #expect(!(await control.overlapped))
  }

  private var batchTargets: [PeerTarget] {
    [
      PeerTarget(host: "tower", port: 8443, connections: 1, chunkSize: defaultChunkSize),
      PeerTarget(host: "pi", port: 8443, connections: 1, chunkSize: defaultChunkSize)
    ]
  }

  @Test(arguments: [false, true])
  func aFailedRecipientDoesNotPreventQueuedRecipientsFromReceivingTheFile(customDestination: Bool)
    async throws(any Error)
  {
    // Keep the control socket below macOS's Unix-domain socket path limit.
    let root = URL(fileURLWithPath: "/tmp").appendingPathComponent("setu-batch-\(UUID())")
    defer { try? FileManager.default.removeItem(at: root) }
    let first = try await TransferServer.start(
      host: "127.0.0.1", port: 0, directory: root.appendingPathComponent("first/shared").path,
      temporaryDirectory: root.appendingPathComponent("first/temporary").path,
      maxActiveUploads: 128, uploadIdleTimeout: .seconds(900), onRestart: nil)
    let second = try await TransferServer.start(
      host: "127.0.0.1", port: 0, directory: root.appendingPathComponent("second/shared").path,
      temporaryDirectory: root.appendingPathComponent("second/temporary").path,
      maxActiveUploads: 128, uploadIdleTimeout: .seconds(900), onRestart: nil)
    let socket = root.appendingPathComponent("sender/run/control.sock").path
    let node = Task {
      try await runNode(
        host: "127.0.0.1", port: 0, directory: root.appendingPathComponent("sender/shared").path,
        socket: socket, temporaryDirectory: root.appendingPathComponent("sender/temporary").path,
        maxActiveUploads: 128, uploadIdleTimeout: .seconds(900),
        downloadRecoveryBytes: defaultDownloadRecoveryBytes, onRestart: nil)
    }
    let control = try NodeControlClient(socket: socket)
    do {
      let deadline = ContinuousClock.now + .seconds(10)
      while !FileManager.default.fileExists(atPath: socket), ContinuousClock.now < deadline {
        try await Task.sleep(for: .milliseconds(10))
      }
      let source = root.appendingPathComponent("source.txt")
      let data = Data("one command, several receivers".utf8)
      try data.write(to: source)
      let destination = customDestination ? root.appendingPathComponent("remote Inbox/nested").path : nil
      let format = try OutputOptions.parse(["--json-stream"])
      let targets = [
        PeerTarget(host: "127.0.0.1", port: first.port, connections: 1, chunkSize: defaultChunkSize),
        PeerTarget(host: "::1", port: 1, connections: 1, chunkSize: defaultChunkSize),
        PeerTarget(host: "127.0.0.1", port: second.port, connections: 1, chunkSize: defaultChunkSize)
      ]
      let started = ContinuousClock.now
      do {
        try await sendBatch(
          file: source.path, targets: targets, destination: destination, createDestination: customDestination,
          parallel: 1, wait: true,
          request: { try await control.request($0) }, format: format)
        Issue.record("Expected an aggregate failure for the unreachable recipient")
      } catch let error as ExitCode { #expect(error == .failure) }
      // A refused peer must use Setu's bounded retries so queued recipients can proceed promptly.
      #expect(started.duration(to: .now) < .seconds(10))
      for directory in ["first", "second"] {
        let received =
          destination.map { URL(fileURLWithPath: $0).appendingPathComponent("source.txt") }
          ?? root.appendingPathComponent(directory + "/shared/source.txt")
        #expect(try Data(contentsOf: received) == data)
        if customDestination {
          #expect(try FileManager.default.contentsOfDirectory(atPath: root.path + "/" + directory + "/shared").isEmpty)
        }
      }
      guard case .jobs(let jobs) = try await control.request(.jobs) else {
        throw ValidationError("Missing batch jobs")
      }
      #expect(jobs.count == 3)
      #expect(jobs.filter { $0.state == .completed }.count == 2)
      #expect(jobs.filter { $0.state == .failed }.count == 1)
      #expect(jobs.allSatisfy { $0.destination == destination })
      await control.close()
      node.cancel()
      try await node.value
      await first.shutdown()
      await second.shutdown()
    } catch {
      await control.close()
      node.cancel()
      _ = try? await node.value
      await first.shutdown()
      await second.shutdown()
      throw error
    }
  }

  @Test func tailscaleCandidatesExcludeSelfAndPreserveOfflineDevices() throws(any Error) {
    let data = Data(
      #"{"Self":{"HostName":"local"},"Peer":{"one":{"HostName":"tower","DNSName":"tower.example.ts.net.","OS":"linux","Online":true},"two":{"HostName":"phone","DNSName":"","OS":"iOS","Online":false}}}"#
        .utf8)
    let candidates = try JSONDecoder().decode(TailnetStatus.self, from: data).candidates
    #expect(candidates.count == 2)
    #expect(candidates[0].host == "phone" && candidates[0].online == false)
    #expect(candidates[1].host == "tower.example.ts.net" && candidates[1].operatingSystem == "linux")
    _ = try SetuCommand.parseAsRoot(["peers", "--tailscale", "--json"])
    _ = try SetuCommand.parseAsRoot(["peers", "--host", "tower", "--json"])
  }
}

private actor UnreliableControl {
  private let pollFailures: Int
  private let loseSubmissionReply: Bool
  private var firstCompleted = false
  private(set) var submitted: [String] = []
  private(set) var firstPolls = 0
  private(set) var overlapped = false

  init(pollFailures: Int, loseSubmissionReply: Bool) {
    self.pollFailures = pollFailures
    self.loseSubmissionReply = loseSubmissionReply
  }

  func request(_ request: NodeRequest) throws(any Error) -> NodeReply {
    switch request {
    case .send(let peer, _):
      if !submitted.isEmpty && !firstCompleted { overlapped = true }
      submitted.append(peer.host)
      if loseSubmissionReply { throw ValidationError("Lost submission reply") }
      return .job(job(peer.host, state: .running))
    case .job(let id):
      if id == "tower" {
        firstPolls += 1
        if firstPolls <= pollFailures { throw ValidationError("Lost status reply") }
        firstCompleted = true
      }
      return .job(job(id, state: .completed))
    default:
      throw ValidationError("Unexpected control request")
    }
  }

  private func job(_ id: String, state: NodeJob.State) -> NodeJob {
    NodeJob(
      id: id, operation: "send /tmp/source", createdAt: 0, state: state, result: nil, error: nil, progress: nil,
      peer: nil, destination: nil, recovery: nil)
  }
}
