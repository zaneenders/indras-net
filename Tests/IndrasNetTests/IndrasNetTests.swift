import IndrasNet
import Testing

@Test func example() async throws {
  _ = IndrasNet(config: Config(host: "127.0.0.1", port: 0))
  #expect(Bool(true))
}
