import Foundation
import Testing

@testable import IndraNode

struct ConfigurationTests {
  @Test func addressesSupportIPv6AndRejectWildcards() throws {
    #expect(try PeerAddress("[::1]:7331") == PeerAddress(host: "::1", port: 7331))
    #expect(try PeerAddress("host.example:7331").description == "host.example:7331")
    for invalid in ["host", "host:0", "host:65536", "0.0.0.0:7331", "[::]:7331", "bad host:7331"] {
      #expect(throws: IndraError.self) { try PeerAddress(invalid) }
    }
  }

  @Test func configurationRoundTripsWithoutOverwritingIdentity() throws {
    let root = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
    defer { try? FileManager.default.removeItem(at: root) }
    let path = root.appendingPathComponent("config.json").path
    let configuration = makeConfiguration(name: "first", seeds: [])
    try configuration.save(path: path, overwrite: false)
    #expect(try NodeConfiguration.load(path: path) == configuration)
    #expect(throws: (any Error).self) {
      try makeConfiguration(name: "second", seeds: []).save(path: path, overwrite: false)
    }
    #expect(try NodeConfiguration.load(path: path).id == configuration.id)
    var updated = configuration
    updated.name = "renamed"
    updated.seeds = [PeerAddress(host: "localhost", port: 7332)]
    #expect(configuration.canReload(updated))
    updated.port = 7333
    #expect(!configuration.canReload(updated))
  }
}

func makeConfiguration(name: String, seeds: [PeerAddress]) -> NodeConfiguration {
  NodeConfiguration(
    version: 1, id: UUID(), name: name, cluster: "test", listenHost: "127.0.0.1", advertisedHost: "127.0.0.1",
    port: 0, seeds: seeds, probeIntervalMilliseconds: 100, requestTimeoutMilliseconds: 200)
}
