import Foundation
import Subprocess
import Testing

@testable import IndrasNet

#if canImport(System)
import System
#else
import SystemPackage
#endif

public enum E2ETestSupport {
  public static func packageRoot() throws -> FilePath {
    var url = URL(fileURLWithPath: #filePath)
    let fileManager = FileManager.default
    while url.path != "/" {
      if fileManager.fileExists(atPath: url.appendingPathComponent("Package.swift").path) {
        return FilePath(url.path)
      }
      url.deleteLastPathComponent()
    }
    struct MissingPackageRoot: Error {}
    throw MissingPackageRoot()
  }

  public static func locateExecutable(named name: String) throws -> FilePath? {
    let root = try packageRoot()
    let fileManager = FileManager.default

    let flat = root.appending(".build/debug/\(name)")
    if fileManager.isExecutableFile(atPath: flat.string) {
      return flat
    }

    let buildDir = root.appending(".build")
    guard let entries = try? fileManager.contentsOfDirectory(atPath: buildDir.string) else {
      return nil
    }
    for entry in entries {
      let candidate = buildDir.appending("\(entry)/debug/\(name)")
      if fileManager.isExecutableFile(atPath: candidate.string) {
        return candidate
      }
    }
    return nil
  }

  public static func buildProduct(named binary: String) async throws -> FilePath {
    if let path = try locateExecutable(named: binary) {
      return path
    }

    let root = try packageRoot()
    let buildResult = try await run(
      .name("swift"),
      arguments: ["build", "--product", binary],
      workingDirectory: root,
      input: .none,
      output: .discarded,
      error: .string(limit: .max)
    )
    if buildResult.terminationStatus.isSuccess == false {
      let stderr = buildResult.standardError ?? ""
      Issue.record("swift build --product \(binary) failed: \(stderr)")
    }

    guard let path = try locateExecutable(named: binary) else {
      Issue.record("\(binary) binary not found after swift build")
      struct MissingExecutable: Error {}
      throw MissingExecutable()
    }
    return path
  }

  public static func processPlatformOptions() -> PlatformOptions {
    var options = PlatformOptions()
    options.teardownSequence = [
      .send(signal: .interrupt, allowedDurationToNextStep: .seconds(2)),
      .gracefulShutDown(allowedDurationToNextStep: .seconds(2)),
    ]
    return options
  }

  @discardableResult
  public static func runNode(
    binary: FilePath,
    arguments: [String],
    log: NodeLog,
    workingDirectory: FilePath,
    platformOptions: PlatformOptions = PlatformOptions()
  ) async throws -> TerminationStatus? {
    do {
      let result = try await run(
        .path(binary),
        arguments: Arguments(arguments),
        workingDirectory: workingDirectory,
        platformOptions: platformOptions,
        input: .none,
        output: .discarded,
        error: .sequence
      ) { execution in
        for try await line in execution.standardError.strings() {
          await log.record(line)
        }
      }
      return result.terminationStatus
    } catch is CancellationError {
      return nil
    }
  }

  public static func waitForRunning(log: NodeLog, timeout: Duration) async throws {
    let clock = ContinuousClock()
    let deadline = clock.now + timeout
    while clock.now < deadline {
      if await log.hasRunning() {
        return
      }
      try await Task.sleep(for: .milliseconds(25))
    }
    Issue.record("node did not report running in time; log: \(await log.allLines())")
    struct Timeout: Error {}
    throw Timeout()
  }

  public static func waitForAllMinRequestVoteSent(
    logs: [NodeLog],
    nodes: [String],
    baselines: [Int],
    minimum: Int,
    timeout: Duration
  ) async throws {
    let clock = ContinuousClock()
    let deadline = clock.now + timeout
    while clock.now < deadline {
      var allMet = true
      for index in logs.indices {
        let count = await logs[index].requestVoteSentCount(
          node: nodes[index],
          since: baselines[index]
        )
        if count < minimum {
          allMet = false
          break
        }
      }
      if allMet {
        return
      }
      try await Task.sleep(for: .milliseconds(25))
    }
    var summaries: [String] = []
    for index in logs.indices {
      let count = await logs[index].requestVoteSentCount(
        node: nodes[index],
        since: baselines[index]
      )
      summaries.append("\(nodes[index]): \(count)")
    }
    Issue.record(
      "not all nodes sent \(minimum) request votes in time; counts since baseline: \(summaries.joined(separator: ", "))"
    )
    struct Timeout: Error {}
    throw Timeout()
  }

  public static func waitForAllMinRequestVoteReceived(
    logs: [NodeLog],
    nodes: [String],
    baselines: [Int],
    minimum: Int,
    timeout: Duration
  ) async throws {
    let clock = ContinuousClock()
    let deadline = clock.now + timeout
    while clock.now < deadline {
      var allMet = true
      for index in logs.indices {
        let count = await logs[index].requestVoteReceivedCount(
          node: nodes[index],
          since: baselines[index]
        )
        if count < minimum {
          allMet = false
          break
        }
      }
      if allMet {
        return
      }
      try await Task.sleep(for: .milliseconds(25))
    }
    var summaries: [String] = []
    for index in logs.indices {
      let count = await logs[index].requestVoteReceivedCount(
        node: nodes[index],
        since: baselines[index]
      )
      summaries.append("\(nodes[index]): \(count)")
    }
    Issue.record(
      "not all nodes received \(minimum) request votes in time; counts since baseline: \(summaries.joined(separator: ", "))"
    )
    struct Timeout: Error {}
    throw Timeout()
  }

  public static func waitForMinAppendEntriesSent(
    logs: [NodeLog],
    nodes: [String],
    baselines: [Int],
    minimum: Int,
    timeout: Duration
  ) async throws {
    let clock = ContinuousClock()
    let deadline = clock.now + timeout
    while clock.now < deadline {
      var total = 0
      for index in logs.indices {
        total += await logs[index].appendEntriesSentCount(
          node: nodes[index],
          since: baselines[index]
        )
      }
      if total >= minimum {
        return
      }
      try await Task.sleep(for: .milliseconds(25))
    }
    Issue.record("cluster did not send \(minimum) appendEntries heartbeats in time")
    struct Timeout: Error {}
    throw Timeout()
  }

  public static func waitForMinClusterRequestVoteReceived(
    logs: [NodeLog],
    nodes: [String],
    minimum: Int,
    timeout: Duration
  ) async throws {
    let clock = ContinuousClock()
    let deadline = clock.now + timeout
    while clock.now < deadline {
      var total = 0
      for (log, node) in zip(logs, nodes) {
        total += await log.requestVoteReceivedCount(node: node)
      }
      if total >= minimum {
        return
      }
      try await Task.sleep(for: .milliseconds(25))
    }
    Issue.record("cluster did not receive \(minimum) request votes in time")
    struct Timeout: Error {}
    throw Timeout()
  }

  public static func waitForMinRequestVoteReceived(
    log: NodeLog,
    node: String,
    minimum: Int,
    timeout: Duration
  ) async throws {
    let clock = ContinuousClock()
    let deadline = clock.now + timeout
    while clock.now < deadline {
      if await log.requestVoteReceivedCount(node: node) >= minimum {
        return
      }
      try await Task.sleep(for: .milliseconds(25))
    }
    Issue.record(
      "node \(node) did not receive \(minimum) request votes in time; log: \(await log.allLines())"
    )
    struct Timeout: Error {}
    throw Timeout()
  }

}

public actor NodeLog {
  private var lines: [String] = []

  public init() {}

  public func record(_ line: String) {
    lines.append(line)
  }

  public func allLines() -> [String] {
    lines
  }

  public func hasRunning() -> Bool {
    lines.contains { $0.contains("running (Ctrl+C to stop)") }
  }

  public func lineCount() -> Int {
    lines.count
  }

  public func meshEventCounts(node: String, since startIndex: Int = 0) -> MeshEventCounts {
    MeshEventCounts(
      requestVoteSent: requestVoteSentCount(node: node, since: startIndex),
      requestVoteReceived: requestVoteReceivedCount(node: node, since: startIndex),
      requestVoteResponseSent: requestVoteResponseSentCount(node: node, since: startIndex),
      requestVoteResponseReceived: requestVoteResponseReceivedCount(
        node: node, since: startIndex),
      appendEntriesSent: appendEntriesSentCount(node: node, since: startIndex)
    )
  }

  private func slice(since startIndex: Int) -> ArraySlice<String> {
    lines.dropFirst(startIndex)
  }

  public func requestVoteSentCount(node: String, since startIndex: Int = 0) -> Int {
    slice(since: startIndex).count { $0.contains("[\(node)] requestVote ->") }
  }

  public func requestVoteReceivedCount(node: String, since startIndex: Int = 0) -> Int {
    slice(since: startIndex).count { $0.contains("[\(node)] requestVote <-") }
  }

  public func requestVoteResponseSentCount(node: String, since startIndex: Int = 0) -> Int {
    slice(since: startIndex).count { $0.contains("[\(node)] requestVoteResponse ->") }
  }

  public func requestVoteResponseReceivedCount(node: String, since startIndex: Int = 0) -> Int {
    slice(since: startIndex).count { $0.contains("[\(node)] requestVoteResponse <-") }
  }

  public func appendEntriesSentCount(node: String, since startIndex: Int = 0) -> Int {
    slice(since: startIndex).count { $0.contains("[\(node)] appendEntries ->") }
  }
}

public struct MeshEventCounts: Equatable, Sendable {
  public var requestVoteSent: Int
  public var requestVoteReceived: Int
  public var requestVoteResponseSent: Int
  public var requestVoteResponseReceived: Int
  public var appendEntriesSent: Int

  public init(
    requestVoteSent: Int,
    requestVoteReceived: Int,
    requestVoteResponseSent: Int,
    requestVoteResponseReceived: Int,
    appendEntriesSent: Int
  ) {
    self.requestVoteSent = requestVoteSent
    self.requestVoteReceived = requestVoteReceived
    self.requestVoteResponseSent = requestVoteResponseSent
    self.requestVoteResponseReceived = requestVoteResponseReceived
    self.appendEntriesSent = appendEntriesSent
  }
}
