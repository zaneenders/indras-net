import NIOCore
import NIOPosix
import Networking
import Synchronization
import Testing

@Suite(.timeLimit(.minutes(1)))
struct TCPTests {
  @Test func duplexBinaryTrafficAndScopedShutdown() async throws {
    let frames = [
      Frame(target: 0, opcode: 0, payload: ByteBuffer()),
      Frame(target: 7, opcode: 42, payload: ByteBuffer(bytes: [0, 0xFF, 0x80, 1])),
      Frame(target: .max, opcode: .max, payload: ByteBuffer(repeating: 0xAB, count: 65_536)),
      Frame(target: 8, opcode: 1, payload: ByteBuffer(repeating: 0xCD, count: 1_048_577))
    ]
    try await withLoopbackPair { server, client in
      try await withThrowingTaskGroup(of: Void.self) { tasks in
        tasks.addTask {
          try await server.executeThenClose { inbound, outbound in
            // Send an event before the client has sent any request.
            try await outbound.write(Frame(target: 1, opcode: 99, payload: ByteBuffer()))
            var requests = inbound.makeAsyncIterator()
            for expected in frames {
              let actual = try await requests.next()
              #expect(actual == expected)
              try await outbound.write(expected)
            }
            // Keep the socket open until the client has read the final reply.
            #expect(try await requests.next() == nil)
          }
        }
        try await client.executeThenClose { inbound, outbound in
          var replies = inbound.makeAsyncIterator()
          #expect(try await replies.next() == Frame(target: 1, opcode: 99, payload: ByteBuffer()))
          for frame in frames {
            try await outbound.write(frame)
            #expect(try await replies.next() == frame)
          }
          outbound.finish()
          #expect(try await replies.next() == nil)
        }
        try await tasks.waitForAll()
      }
      #expect(!client.channel.isActive)
      #expect(!server.channel.isActive)
    }
  }

  @Test func applicationFailureClosesTheConnectionAndNotifiesItsPeer() async throws {
    try await withLoopbackPair { server, client in
      await #expect(throws: TestFailure.application) {
        try await client.executeThenClose { _, _ in
          throw TestFailure.application
        }
      }
      try await server.executeThenClose { inbound, _ in
        var iterator = inbound.makeAsyncIterator()
        let next = try await iterator.next()
        #expect(next == nil)
      }
      #expect(!client.channel.isActive)
    }
  }

  @Test(arguments: [false, true])
  func closingDrainsABufferedFrame(halfClose: Bool) async throws {
    try await withLoopbackPair { server, client in
      try await client.channel.setSendBuffer(bytes: 4096)
      try await server.channel.setAutoRead(false)
      let frame = Frame(target: 1, opcode: 2, payload: ByteBuffer(repeating: 7, count: 2 * 1024 * 1024))
      let (enqueued, signal) = AsyncStream<Void>.makeStream(bufferingPolicy: .bufferingNewest(1))
      async let send: Void = client.executeThenClose { inbound, outbound in
        defer { signal.finish() }
        try await outbound.write(frame)
        let bufferedBytes = try await client.channel.bufferedWriteBytes()
        #expect(bufferedBytes > 0)
        signal.yield(())
        if halfClose {
          outbound.finish()
          var frames = inbound.makeAsyncIterator()
          let end = try await frames.next()
          #expect(end == nil)
        }
      }

      for await _ in enqueued { break }
      try await server.channel.setAutoRead(true)
      try await server.executeThenClose { inbound, _ in
        var frames = inbound.makeAsyncIterator()
        let received = try await frames.next()
        #expect(received == frame)
        let end = try await frames.next()
        #expect(end == nil)
      }
      try await send
    }
  }

  @Test func cancellingAnInboundWaitReleasesItsSocket() async throws {
    try await withLoopbackPair { _, client in
      let waiting = Task {
        try await client.executeThenClose { inbound, _ in
          for try await _ in inbound {}
        }
      }
      waiting.cancel()
      // NIO permits cancellation to terminate a stream cleanly or throw.
      _ = try? await waiting.value
      #expect(!client.channel.isActive)
    }
  }

  @Test func finishingOutputStillAllowsAResponseAfterThePeerReceivesEof() async throws {
    try await withLoopbackPair { server, client in
      try await withThrowingTaskGroup(of: Void.self) { tasks in
        tasks.addTask {
          try await server.executeThenClose { inbound, outbound in
            var iterator = inbound.makeAsyncIterator()
            #expect(try await iterator.next() == Frame(target: 0, opcode: 1, payload: ByteBuffer()))
            #expect(try await iterator.next() == nil)
            // The client's FIN must not prevent the server from replying.
            try await outbound.write(Frame(target: 0, opcode: 2, payload: ByteBuffer()))
          }
        }
        try await client.executeThenClose { inbound, outbound in
          try await outbound.write(Frame(target: 0, opcode: 1, payload: ByteBuffer()))
          outbound.finish()
          var iterator = inbound.makeAsyncIterator()
          #expect(try await iterator.next() == Frame(target: 0, opcode: 2, payload: ByteBuffer()))
          #expect(try await iterator.next() == nil)
        }
        try await tasks.waitForAll()
      }
    }
  }

  @Test func inputCloseHandlerCanBeRemovedAndRegisteredAfterEOF() async throws {
    try await withLoopbackPair { server, client in
      let calls = Mutex(0)
      try await server.executeThenClose { inbound, _ in
        try await server.setInputCloseHandler { calls.withLock { $0 += 1 } }
        try await server.setInputCloseHandler(nil)
        try await client.channel.close(mode: .output)
        var frames = inbound.makeAsyncIterator()
        #expect(try await frames.next() == nil)
        #expect(server.channel.isActive)
        #expect(calls.withLock { $0 } == 0)

        try await server.setInputCloseHandler { calls.withLock { $0 += 1 } }
        #expect(calls.withLock { $0 } == 1)
      }
      #expect(calls.withLock { $0 } == 1)
    }
  }

  @Test func droppingAnUnusedConnectionSchedulesSocketClosure() async throws {
    let listener = try await TCP.listen(
      host: "127.0.0.1", port: 0)
    let port = try #require(listener.channel.localAddress?.port)
    do {
      var unused: Connection? = try await TCP.connect(
        host: "127.0.0.1", port: port,
        connectTimeout: .seconds(10))
      let channel = try #require(unused?.channel)
      try await listener.executeThenClose { connections in
        var iterator = connections.makeAsyncIterator()
        let server = try #require(try await iterator.next())
        unused = nil
        try await channel.closeFuture.get()
        #expect(!channel.isActive)
        try await server.executeThenClose { inbound, _ in
          var frames = inbound.makeAsyncIterator()
          let next = try await frames.next()
          #expect(next == nil)
        }
      }
    } catch {
      try? await listener.channel.close()
      throw error
    }
  }

  @Test func closingTheListenerLeavesAcceptedConnectionsUnderCallerOwnership() async throws {
    let listener = try await TCP.listen(
      host: "127.0.0.1", port: 0)
    let port = try #require(listener.channel.localAddress?.port)
    let client = try await TCP.connect(
      host: "127.0.0.1", port: port, connectTimeout: .seconds(10)
    )
    do {
      let server = try await listener.executeThenClose { connections in
        var iterator = connections.makeAsyncIterator()
        return try #require(try await iterator.next())
      }
      do {
        #expect(!listener.channel.isActive)
        #expect(client.channel.isActive)
        try await client.executeThenClose { _, outbound in
          try await outbound.write(Frame(target: 0, opcode: 3, payload: ByteBuffer()))
        }
        try await server.executeThenClose { inbound, _ in
          var iterator = inbound.makeAsyncIterator()
          let next = try await iterator.next()
          #expect(next == Frame(target: 0, opcode: 3, payload: ByteBuffer()))
        }
      } catch {
        try? await server.channel.close()
        throw error
      }
    } catch {
      try? await client.channel.close()
      try? await listener.channel.close()
      throw error
    }
  }

  @Test func truncatedPayloadClosesTheSocketWithoutAnApplicationConsumer() async throws {
    let listener = try await TCP.listen(host: "127.0.0.1", port: 0)
    let port = try #require(listener.channel.localAddress?.port)
    let raw = try await ClientBootstrap(group: .singletonMultiThreadedEventLoopGroup)
      .connect(host: "127.0.0.1", port: port).get()
    do {
      try await listener.executeThenClose { connections in
        var iterator = connections.makeAsyncIterator()
        let server = try #require(try await iterator.next())
        var header = ByteBuffer()
        header.writeInteger(UInt32(4), endianness: .big)
        header.writeInteger(UInt32(1), endianness: .big)
        header.writeInteger(UInt32(2), endianness: .big)
        header.writeInteger(UInt8(9))
        raw.pipeline.writeAndFlush(header, promise: nil)
        try await raw.close(mode: .output)
        // No server.executeThenClose has started: the transport itself must close.
        try await server.channel.closeFuture.get()
        #expect(!server.channel.isActive)
        await #expect(throws: FrameError.truncatedFrame) {
          try await server.executeThenClose { inbound, _ in
            for try await _ in inbound {}
          }
        }
      }
      try? await raw.close()
    } catch {
      try? await raw.close()
      try? await listener.channel.close()
      throw error
    }
  }

}

extension Channel {
  fileprivate func setSendBuffer(bytes: SocketOptionValue) async throws {
    try await setOption(ChannelOptions.socketOption(.so_sndbuf), value: bytes).get()
  }

  fileprivate func bufferedWriteBytes() async throws -> Int {
    try await getOption(ChannelOptions.bufferedWritableBytes).get()
  }

  fileprivate func setAutoRead(_ enabled: Bool) async throws {
    try await setOption(ChannelOptions.autoRead, value: enabled).get()
  }
}

private enum TestFailure: Error {
  case application
}

/// Every test owns both accepted sockets and the listener, including failure paths.
private func withLoopbackPair(
  _ body: @Sendable (Connection, Connection) async throws -> Void
) async throws {
  let listener = try await TCP.listen(host: "127.0.0.1", port: 0)
  let port = try #require(listener.channel.localAddress?.port)
  do {
    let client = try await TCP.connect(
      host: "127.0.0.1", port: port, connectTimeout: .seconds(10))
    do {
      try await listener.executeThenClose { connections in
        var iterator = connections.makeAsyncIterator()
        let server = try #require(try await iterator.next())
        do {
          try await body(server, client)
          try? await server.channel.close()
        } catch {
          try? await server.channel.close()
          throw error
        }
      }
      try? await client.channel.close()
    } catch {
      try? await client.channel.close()
      throw error
    }
  } catch {
    try? await listener.channel.close()
    throw error
  }
}
