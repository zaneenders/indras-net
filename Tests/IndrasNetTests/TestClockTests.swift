import TestUtils
import Testing

@Suite struct TestClockTests {
  private actor Flag {
    private(set) var value = false
    func set() { value = true }
  }

  @Test func sleepBlocksUntilAdvancedPastDeadline() async throws {
    let clock = TestClock()
    let flag = Flag()

    let task = Task {
      try await clock.sleep(until: clock.now.advanced(by: .seconds(10)))
      await flag.set()
    }

    // Let the sleeper park, then confirm it does not wake without enough advancement.
    try await Task.sleep(for: .milliseconds(20))
    #expect(await flag.value == false)

    clock.advance(by: .seconds(5))
    try await Task.sleep(for: .milliseconds(20))
    #expect(await flag.value == false)

    clock.advance(by: .seconds(5))
    try await task.value
    #expect(await flag.value == true)
  }

  @Test func sleepHonorsCancellation() async throws {
    let clock = TestClock()

    let task = Task { () -> Bool in
      do {
        try await clock.sleep(until: clock.now.advanced(by: .seconds(100)))
        return false
      } catch is CancellationError {
        return true
      } catch {
        return false
      }
    }

    try await Task.sleep(for: .milliseconds(20))
    task.cancel()
    #expect(await task.value)
  }
}
