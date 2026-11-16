import Dispatch
import Synchronization

#if canImport(Darwin)
import Darwin
#elseif canImport(Musl)
import Musl
#else
import Glibc
#endif

final class ShutdownSignals: Sendable {
  private let sources = Mutex<[any DispatchSourceSignal]>([])
  private let stream: AsyncStream<Void>

  init() {
    let (stream, continuation) = AsyncStream<Void>.makeStream(bufferingPolicy: .bufferingNewest(1))
    self.stream = stream
    sources.withLock { sources in
      for number in [SIGINT, SIGTERM] {
        _ = signal(number, SIG_IGN)
        let source = DispatchSource.makeSignalSource(signal: number, queue: .global())
        source.setEventHandler {
          continuation.yield(())
          continuation.finish()
        }
        source.activate()
        sources.append(source)
      }
    }
  }

  func wait() async {
    for await _ in stream { break }
    cancel()
  }

  private func cancel() {
    sources.withLock { sources in
      for source in sources { source.cancel() }
      sources.removeAll()
    }
  }

  deinit { cancel() }
}
