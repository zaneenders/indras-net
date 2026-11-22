import Foundation
import Testing
import _NIOFileSystem

@testable import IndraNode

struct ConfigurationTests {
  @Test func addressesSupportIPv6AndRejectWildcards() throws {
    #expect(try PeerAddress("[::1]:7331") == PeerAddress(host: "::1", port: 7331))
    #expect(try PeerAddress("host.example:7331").description == "host.example:7331")
    for invalid in ["host", "host:0", "host:65536", "0.0.0.0:7331", "[::]:7331", "bad host:7331"] {
      #expect(throws: IndraError.self) { try PeerAddress(invalid) }
    }
  }

  @Test func configurationRoundTripsWithoutOverwritingIdentity() async throws {
    let files = FileSystem.shared
    let root = try await files.createTemporaryDirectory(template: files.temporaryDirectory.appending("XXXXXXXX"))
    defer { _ = try? await files.removeItem(at: root, strategy: .sequential, recursively: true) }
    let path = root.appending("node/config.json").string
    let configuration = makeConfiguration(name: "first", seeds: [])
    try await configuration.save(path: path, overwrite: false)
    #expect(try await NodeConfiguration.load(path: path) == configuration)
    await #expect(throws: (any Error).self) {
      try await makeConfiguration(name: "second", seeds: []).save(path: path, overwrite: false)
    }
    #expect(try await NodeConfiguration.load(path: path).id == configuration.id)
    var updated = configuration
    updated.name = "renamed"
    updated.seeds = [PeerAddress(host: "localhost", port: 7332)]
    #expect(configuration.canReload(updated))
    updated.port = 7333
    #expect(!configuration.canReload(updated))
  }

  @Test func stagingUsesTheFilesystemResolvedConfigurationParent() async throws {
    let files = FileSystem.shared
    let root = try await files.createTemporaryDirectory(template: files.temporaryDirectory.appending("XXXXXXXX"))
    defer { _ = try? await files.removeItem(at: root, strategy: .sequential, recursively: true) }
    let nested = root.appending("actual/child")
    try await files.createDirectory(at: nested, withIntermediateDirectories: true, permissions: .ownerReadWriteExecute)
    try await files.createSymbolicLink(at: root.appending("link"), withDestination: nested)
    let path = root.appending("link/../config.json").string
    var configuration = makeConfiguration(name: "before", seeds: [])
    try await configuration.save(path: path, overwrite: false)
    configuration.name = "after"
    try await configuration.save(path: path, overwrite: true)
    #expect(try await NodeConfiguration.load(path: root.appending("actual/config.json").string) == configuration)
    #expect(try await directoryNames(at: root).sorted() == ["actual", "link"])
  }

  @Test func concurrentExclusiveSavesPublishExactlyOneConfiguration() async throws {
    let files = FileSystem.shared
    let root = try await files.createTemporaryDirectory(template: files.temporaryDirectory.appending("XXXXXXXX"))
    defer { _ = try? await files.removeItem(at: root, strategy: .sequential, recursively: true) }
    let path = root.appending("node/config.json").string
    let configurations = [makeConfiguration(name: "first", seeds: []), makeConfiguration(name: "second", seeds: [])]
    let saved = await withTaskGroup(of: NodeConfiguration?.self) { tasks in
      for configuration in configurations {
        tasks.addTask {
          do {
            try await configuration.save(path: path, overwrite: false)
            return configuration
          } catch {
            return nil
          }
        }
      }
      var saved: [NodeConfiguration] = []
      for await configuration in tasks {
        if let configuration { saved.append(configuration) }
      }
      return saved
    }
    #expect(try await [NodeConfiguration.load(path: path)] == saved)
  }
}

func makeConfiguration(name: String, seeds: [PeerAddress]) -> NodeConfiguration {
  NodeConfiguration(
    version: 1, id: UUID(), name: name, cluster: "test", listenHost: "127.0.0.1", advertisedHost: "127.0.0.1",
    port: 0, seeds: seeds, probeIntervalMilliseconds: 100, requestTimeoutMilliseconds: 200)
}
