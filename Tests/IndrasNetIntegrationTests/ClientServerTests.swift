import Testing

@Suite(.serialized) struct ClientServerTests {

  @Test func clientReceivesPongAndHelloFromServer() async throws {
    let nodeBinary = try await IntegrationTestSupport.nodeExecutable()
    let root = try IntegrationTestSupport.packageRoot()
    let host = "127.0.0.1"
    let serverLog = IndrasNetEventLog()
    let clientLog = IndrasNetEventLog()

    try await withThrowingTaskGroup(of: Void.self) { group in
      group.addTask {
        try await IntegrationTestSupport.runNode(
          binary: nodeBinary,
          arguments: ["--json-event-log", host, "0"],
          eventLog: serverLog,
          workingDirectory: root,
          platformOptions: IntegrationTestSupport.processPlatformOptions()
        )
      }

      let port = try await IntegrationTestSupport.waitForListeningPort(
        eventLog: serverLog,
        timeout: .seconds(10)
      )

      group.addTask {
        let status = try await IntegrationTestSupport.runNode(
          binary: nodeBinary,
          arguments: ["--json-event-log", "--connect", host, String(port)],
          eventLog: clientLog,
          workingDirectory: root
        )
        if status?.isSuccess != true {
          Issue.record("client exited with \(String(describing: status)); events: \(await clientLog.allEvents())")
        }
      }

      try await group.next()
      group.cancelAll()
      while (try? await group.next()) != nil {}
    }

    #expect(await serverLog.payloads(.received, type: "ping").count == 1)
    #expect(await serverLog.payloads(.received, type: "hello").count == 1)
    #expect(await clientLog.contains { if case .sessionComplete = $0 { true } else { false } })
    #expect(await clientLog.payloads(.received, type: "pong") == [""])
    #expect(await clientLog.payloads(.received, type: "hello") == ["ok"])
  }
}
