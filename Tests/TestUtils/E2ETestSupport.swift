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

  struct Timeout: Error {}

  public static func waitForRunning(log: NodeLog, timeout: Duration) async throws {
    let running = await TestHelpers.poll(timeout: timeout) { await log.hasRunning() }
    if !running {
      Issue.record("node did not report running in time; log: \(await log.allLines())")
      throw Timeout()
    }
  }

  /// Waits until some node reports that it became leader. This is the single,
  /// stable smoke signal that the cluster booted and completed an election over
  /// real TCP — replacing the previous brittle per-message log-count scraping.
  public static func waitForLeaderElected(logs: [NodeLog], timeout: Duration) async throws {
    let elected = await TestHelpers.poll(timeout: timeout) {
      for log in logs where await log.hasLeaderElected() {
        return true
      }
      return false
    }
    if !elected {
      Issue.record("no node reported becoming leader within \(timeout)")
      throw Timeout()
    }
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

  public func hasLeaderElected() -> Bool {
    lines.contains { $0.contains("became leader in term") }
  }
}
