import Foundation
import NIOPosix
import Testing

@testable import IndrasNet

public struct InProcessMesh {
  let shells: [TCPShell]
  public let recorder: ShellActionRecorder

  public static func start(
    basePort: Int,
    eventLoopGroup: MultiThreadedEventLoopGroup,
    recordActions: Bool = false
  ) async throws -> InProcessMesh {
    let host = "127.0.0.1"
    let nodes = (0..<3).map { offset in
      NodeAddress(host: host, port: basePort + offset)
    }

    let recorder = ShellActionRecorder()
    let shells = nodes.map { node in
      Shell(
        node,
        transport: TCPTransport(
          configuration: node.tcpConfiguration(),
          eventLoopGroup: eventLoopGroup,
          logger: TestHelpers.quietLogger
        ),
        logger: recordActions
          ? TestHelpers.shellLogger(node: node, recorder: recorder)
          : TestHelpers.quietLogger
      )
    }

    _ = try await shells[2].start(with: [nodes[1], nodes[0]])
    _ = try await shells[1].start(with: [nodes[2], nodes[0]])
    _ = try await shells[0].start(with: [nodes[1], nodes[2]])

    return InProcessMesh(shells: shells, recorder: recorder)
  }

  public func waitForLeader(timeout: Duration = .seconds(5)) async throws -> TCPShell {
    await TestHelpers.waitUntil(timeout: timeout) {
      for shell in shells where await shell.instance.role == .leader {
        return true
      }
      return false
    }

    for shell in shells where await shell.instance.role == .leader {
      return shell
    }

    Issue.record("Expected a leader")
    struct MissingLeader: Error {}
    throw MissingLeader()
  }

  public func waitForFollower(knownLeader leaderID: PeerId, timeout: Duration = .seconds(5)) async throws -> TCPShell {
    await TestHelpers.waitUntil(timeout: timeout) {
      for shell in shells where await shell.instance.role == .follower {
        if await shell.instance.leaderId == leaderID {
          return true
        }
      }
      return false
    }

    for shell in shells where await shell.instance.role == .follower {
      if await shell.instance.leaderId == leaderID {
        return shell
      }
    }

    Issue.record("Expected a follower that knows the leader")
    struct MissingFollower: Error {}
    throw MissingFollower()
  }

  public func waitForReplicated(command: Data, atIndex index: LogIndex, timeout: Duration = .seconds(5))
    async throws
  {
    await TestHelpers.waitUntil(timeout: timeout) {
      for shell in shells {
        let nodeLog = await shell.instance.log
        guard nodeLog.count > Int(index), nodeLog[Int(index)].command == command else {
          return false
        }
      }
      return true
    }

    for shell in shells {
      let nodeLog = await shell.instance.log
      #expect(nodeLog[Int(index)].command == command)
    }
  }

  public func shutdown() async throws {
    for shell in shells {
      try await shell.shutdown()
    }
  }
}
