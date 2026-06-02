import Foundation
import Subprocess
import System
import Testing

@testable import IndrasNet

enum E2ETestSupport {
  static func packageRoot() throws -> FilePath {
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

  static func locateExecutable(named name: String) throws -> FilePath? {
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

  static func buildProduct(named binary: String) async throws -> FilePath {
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

  static func processPlatformOptions() -> PlatformOptions {
    var options = PlatformOptions()
    options.teardownSequence = [
      .send(signal: .interrupt, allowedDurationToNextStep: .seconds(2)),
      .gracefulShutDown(allowedDurationToNextStep: .seconds(2)),
    ]
    return options
  }

  @discardableResult
  static func runNode(
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

  static func waitForRunning(log: NodeLog, timeout: Duration) async throws {
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

  static func waitForAllMinPingSent(
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
        let count = await logs[index].pingSentCount(
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
      let count = await logs[index].pingSentCount(
        node: nodes[index],
        since: baselines[index]
      )
      summaries.append("\(nodes[index]): \(count)")
    }
    Issue.record(
      "not all nodes sent \(minimum) pings in time; counts since baseline: \(summaries.joined(separator: ", "))"
    )
    struct Timeout: Error {}
    throw Timeout()
  }

  static func waitForAllMinPingReceived(
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
        let count = await logs[index].pingReceivedCount(
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
      let count = await logs[index].pingReceivedCount(
        node: nodes[index],
        since: baselines[index]
      )
      summaries.append("\(nodes[index]): \(count)")
    }
    Issue.record(
      "not all nodes received \(minimum) pings in time; counts since baseline: \(summaries.joined(separator: ", "))"
    )
    struct Timeout: Error {}
    throw Timeout()
  }

  static func waitForMinPingReceived(
    log: NodeLog,
    node: String,
    minimum: Int,
    timeout: Duration
  ) async throws {
    let clock = ContinuousClock()
    let deadline = clock.now + timeout
    while clock.now < deadline {
      if await log.pingReceivedCount(node: node) >= minimum {
        return
      }
      try await Task.sleep(for: .milliseconds(25))
    }
    Issue.record(
      "node \(node) did not receive \(minimum) pings in time; log: \(await log.allLines())"
    )
    struct Timeout: Error {}
    throw Timeout()
  }

}

actor NodeLog {
  private var lines: [String] = []

  func record(_ line: String) {
    lines.append(line)
  }

  func allLines() -> [String] {
    lines
  }

  func hasRunning() -> Bool {
    lines.contains { $0.contains("running (Ctrl+C to stop)") }
  }

  func lineCount() -> Int {
    lines.count
  }

  func meshEventCounts(node: String, since startIndex: Int = 0) -> MeshEventCounts {
    MeshEventCounts(
      pingSent: pingSentCount(node: node, since: startIndex),
      pingReceived: pingReceivedCount(node: node, since: startIndex),
      pongSent: pongSentCount(node: node, since: startIndex),
      pongReceived: pongReceivedCount(node: node, since: startIndex)
    )
  }

  private func slice(since startIndex: Int) -> ArraySlice<String> {
    lines.dropFirst(startIndex)
  }

  func pingSentCount(node: String, since startIndex: Int = 0) -> Int {
    slice(since: startIndex).count { $0.contains("[\(node)] ping ->") }
  }

  func pingReceivedCount(node: String, since startIndex: Int = 0) -> Int {
    slice(since: startIndex).count { $0.contains("[\(node)] ping <-") }
  }

  func pongSentCount(node: String, since startIndex: Int = 0) -> Int {
    slice(since: startIndex).count { $0.contains("[\(node)] pong ->") }
  }

  func pongReceivedCount(node: String, since startIndex: Int = 0) -> Int {
    slice(since: startIndex).count { $0.contains("[\(node)] pong <-") }
  }
}

struct MeshEventCounts: Equatable {
  var pingSent: Int
  var pingReceived: Int
  var pongSent: Int
  var pongReceived: Int
}
