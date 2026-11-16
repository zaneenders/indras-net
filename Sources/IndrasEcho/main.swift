import Networking
import NIOCore

/// A finite, public-API loopback demonstration. The server sends an unsolicited
/// event, then echoes arbitrary binary payloads without interpreting them.
@main
struct IndrasEcho {
  static func main() async throws {
    let payloads = [ByteBuffer(), ByteBuffer(bytes: [0, 1, 127, 128, 255]), ByteBuffer(string: "hello")]
    let listener = try await TCP.listen(host: "127.0.0.1", port: 0)
    guard let port = listener.channel.localAddress?.port else {
      throw DemoError.missingAddress
    }
    print("Listening on 127.0.0.1:\(port)")

    try await withThrowingTaskGroup(of: Void.self) { tasks in
      tasks.addTask {
        try await listener.executeThenClose { connections in
          for try await connection in connections {
            try await connection.executeThenClose { inbound, outbound in
              try await outbound.write(Frame(target: 1, opcode: 0, payload: ByteBuffer()))
              for try await frame in inbound {
                try await outbound.write(frame)
              }
            }
            // This demonstration deliberately handles one client, then exits.
            break
          }
        }
      }

      do {
        let connection = try await TCP.connect(
          host: "127.0.0.1", port: port,
          connectTimeout: .seconds(10))
        try await connection.executeThenClose { inbound, outbound in
          var replies = inbound.makeAsyncIterator()
          guard try await replies.next() == Frame(target: 1, opcode: 0, payload: ByteBuffer()) else {
            throw DemoError.unexpectedFrame
          }
          for payload in payloads {
            let sent = Frame(target: 7, opcode: 42, payload: payload)
            try await outbound.write(sent)
            guard try await replies.next() == sent else {
              throw DemoError.unexpectedFrame
            }
            print("Echoed target=\(sent.target) opcode=\(sent.opcode) bytes=\(payload.readableBytes)")
          }
        }
        try await tasks.waitForAll()
      } catch {
        tasks.cancelAll()
        try? await listener.channel.close()
        throw error
      }
    }
    print("Binary echo passed; client and listener closed.")
  }
}

private enum DemoError: Error {
  case missingAddress
  case unexpectedFrame
}
