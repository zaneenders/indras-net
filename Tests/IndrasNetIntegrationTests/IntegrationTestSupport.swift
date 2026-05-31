import Foundation
import IndrasNet
import Subprocess
import System
import Testing

enum IntegrationTestSupport {
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

  static func nodeExecutable() async throws -> FilePath {
    let binary = "indras-net"
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
    eventLog: IndrasNetEventLog,
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
        output: .sequence,
        error: .discarded
      ) { execution in
        let decoder = JSONDecoder()
        for try await line in execution.standardOutput.strings() {
          guard let event = try? decoder.decode(IndrasNetEvent.self, from: Data(line.utf8)) else {
            continue
          }
          await eventLog.record(event)
        }
      }
      return result.terminationStatus
    } catch is CancellationError {
      return nil
    }
  }

  static func waitForListeningPort(
    eventLog: IndrasNetEventLog,
    timeout: Duration
  ) async throws -> Int {
    let clock = ContinuousClock()
    let deadline = clock.now + timeout
    while clock.now < deadline {
      if let port = await eventLog.listeningPort() {
        return port
      }
      try await Task.sleep(for: .milliseconds(25))
    }
    Issue.record("server did not report a listening port in time; events: \(await eventLog.allEvents())")
    struct Timeout: Error {}
    throw Timeout()
  }
}

actor IndrasNetEventLog {
  private var events: [IndrasNetEvent] = []

  func record(_ event: IndrasNetEvent) {
    self.events.append(event)
  }

  func allEvents() -> [IndrasNetEvent] {
    self.events
  }

  func count(where predicate: (IndrasNetEvent) -> Bool) -> Int {
    self.events.lazy.filter(predicate).count
  }

  func contains(where predicate: (IndrasNetEvent) -> Bool) -> Bool {
    self.events.contains(where: predicate)
  }

  func listeningPort() -> Int? {
    for event in self.events {
      if case .listening(_, let port) = event { return port }
    }
    return nil
  }

  func payloads(_ direction: IndrasNetEvent.Direction, type: String) -> [String] {
    self.events.compactMap { event in
      if case .message(direction, type, let payload) = event { return payload }
      return nil
    }
  }
}
