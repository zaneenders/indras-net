import Foundation
import NIOCore
import Networking
import Subprocess
import TCPActorSystem
import Testing

@testable import IndraNode

#if canImport(System)
import System
#else
import SystemPackage
#endif

@Suite(.timeLimit(.minutes(1)))
final class DaemonRestartTests {
  @Test func serviceDiscoveryRecoversAfterDaemonCrash() async throws {
    #if os(macOS)
    let products = Bundle(for: DaemonRestartTests.self).bundleURL.deletingLastPathComponent()
    #else
    let products = URL(filePath: CommandLine.arguments[0]).deletingLastPathComponent()
    #endif
    let executable = products.appending(path: "indra", directoryHint: .notDirectory)
    try #require(
      FileManager.default.isExecutableFile(atPath: executable.path), "Missing built CLI at \(executable.path)")

    let root = URL(filePath: "/tmp/indra-restart-\(UUID().uuidString)", directoryHint: .isDirectory)
    try FileManager.default.createDirectory(
      at: root, withIntermediateDirectories: false, attributes: [.posixPermissions: 0o700])
    do {
      var configuration = makeConfiguration(name: "alpha", seeds: [])
      configuration.cluster = UUID().uuidString
      let cluster = configuration.cluster
      let alpha = try DaemonProcess(executable: executable, root: root, configuration: configuration)
      try await alpha.run(logName: "daemon.log", stopWith: .terminate) { alphaIdentity in
        let reservation = try await TCP.listen(host: "127.0.0.1", port: 0)
        let reservedPort = reservation.channel.localAddress?.port
        try await reservation.channel.close()
        var configuration = makeConfiguration(name: "beta", seeds: [alphaIdentity.address])
        configuration.cluster = cluster
        configuration.port = try #require(reservedPort)
        let beta = try DaemonProcess(executable: executable, root: root, configuration: configuration)

        let original = try await beta.run(logName: "before-crash.log", stopWith: .kill) { identity in
          try await alpha.waitForServices(from: identity)
          let pong = try await alpha.ping(node: identity.id)
          try #require(pong.node.endpoint == identity.endpoint)
          return identity
        }

        let staleSocket = try FileManager.default.attributesOfItem(atPath: beta.socket)
        try #require(staleSocket[.type] as? FileAttributeType == .typeSocket)
        _ = try await alpha.waitFor(
          "Beta's services to disappear", arguments: ["services"], as: [DiscoveredService].self
        ) { services in
          services.contains { $0.node.id == alphaIdentity.id } && services.allSatisfy { $0.node.id != original.id }
        }

        try await beta.run(logName: "after-crash.log", stopWith: .terminate) { replacement in
          try #require(replacement.id == original.id)
          try #require(replacement.address == original.address)
          try #require(replacement.endpoint.node != original.endpoint.node)
          try await alpha.waitForServices(from: replacement)
          let pong = try await alpha.ping(node: replacement.id)
          try #require(pong.node.id == replacement.id)
          try #require(pong.node.endpoint == replacement.endpoint)
        }
      }
    } catch {
      throw IndraError("\(error)\nDaemon logs retained at \(root.path)")
    }
    try FileManager.default.removeItem(at: root)
  }
}

private struct DaemonProcess: Sendable {
  let executable: URL
  let directory: URL
  let id: UUID

  var config: String { directory.appending(path: "config.json", directoryHint: .notDirectory).path }
  var socket: String { directory.appending(path: "control.sock", directoryHint: .notDirectory).path }

  init(executable: URL, root: URL, configuration: NodeConfiguration) throws {
    self.executable = executable
    self.directory = root.appending(path: configuration.name, directoryHint: .isDirectory)
    self.id = configuration.id
    try configuration.save(path: config, overwrite: false)
  }

  func run<Value: Sendable>(
    logName: String, stopWith signal: Signal, _ body: @Sendable (NodeIdentity) async throws -> Value
  ) async throws -> Value {
    let logURL = directory.appending(path: logName, directoryHint: .notDirectory)
    let log = try FileDescriptor.open(
      FilePath(logURL.path), .writeOnly, options: [.create, .exclusiveCreate, .closeOnExec],
      permissions: .ownerReadWrite, retryOnInterrupt: true)
    defer { try? log.close() }

    var options = PlatformOptions()
    options.teardownSequence = [.gracefulShutDown(toProcessGroup: false, allowedDurationToNextStep: .seconds(2))]
    let result = try await Subprocess.run(
      .path(.init(executable.path)), arguments: ["daemon", "--config", config, "--socket", socket],
      environment: .inherit, workingDirectory: nil, platformOptions: options, input: .none,
      output: .fileDescriptor(log, closeAfterSpawningProcess: false),
      error: .combinedWithOutput
    ) { process in
      let status = try await waitFor("daemon readiness", arguments: ["status"], as: NodeStatus.self) {
        $0.node.id == id
      }
      let value = try await body(status.node)
      await process.teardown(using: [
        .send(signal: signal, toProcessGroup: false, allowedDurationToNextStep: .seconds(2))
      ])
      return value
    }
    let expected: TerminationStatus = signal == .kill ? .signaled(signal.rawValue) : .exited(0)
    try #require(result.terminationStatus == expected, "See \(logURL.path)")
    return result.closureResult
  }

  func waitForServices(from peer: NodeIdentity) async throws {
    _ = try await waitFor(
      "services from \(peer.name) at \(peer.endpoint)", arguments: ["services"], as: [DiscoveredService].self
    ) { services in
      let registrations = services.filter { $0.node.id == peer.id }.map(\.registration)
      return registrations.contains { $0.key == "indra.receptionist.v1" }
        && registrations.allSatisfy { $0.address.endpoint == peer.endpoint }
    }
  }

  func ping(node: UUID) async throws -> PingResult {
    let data = try await request(["ping", node.uuidString], timeout: .seconds(2))
    return try JSONDecoder().decode(PingResult.self, from: data)
  }

  func waitFor<Value: Decodable>(
    _ phase: String, arguments: [String], as type: Value.Type, matching: (Value) -> Bool
  ) async throws -> Value {
    let deadline = ContinuousClock.now.advanced(by: .seconds(8))
    var lastResponse = "None"
    var lastError = "None"
    while ContinuousClock.now < deadline {
      try Task.checkCancellation()
      do {
        let timeout = min(.seconds(2), ContinuousClock.now.duration(to: deadline))
        let data = try await request(arguments, timeout: timeout)
        lastResponse = String(decoding: data, as: UTF8.self)
        let sample = try JSONDecoder().decode(type, from: data)
        if matching(sample) { return sample }
      } catch {
        lastError = String(describing: error)
      }
      try await Task.sleep(for: .milliseconds(50))
    }
    throw IndraError(
      """
      Timed out waiting for \(phase) at \(socket)
      Last JSON response: \(lastResponse)
      Last command error: \(lastError)
      """)
  }

  private func request(_ arguments: [String], timeout: Duration) async throws -> Data {
    let command = arguments.joined(separator: " ")
    return try await withDeadline(
      deadline: .now.advanced(by: timeout), timeoutError: IndraError("indra \(command) timed out")
    ) {
      let result = try await Subprocess.run(
        .path(.init(executable.path)), arguments: Arguments(arguments + ["--socket", socket, "--json"]),
        environment: .inherit, workingDirectory: nil, platformOptions: PlatformOptions(), input: .none,
        output: .data(limit: 65_536), error: .string(limit: 16_384))
      guard result.terminationStatus.isSuccess else {
        throw IndraError("indra \(command) \(result.terminationStatus)\n\(result.standardError)")
      }
      return result.standardOutput
    }
  }
}
