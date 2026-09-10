import Foundation
import Testing

@testable import VMBridge

@Suite struct MulticasterTests {

    @Test func fansOutToAllSubscribers() async throws {
        let multicaster = Multicaster<Int>()
        let first = multicaster.makeStream()
        let second = multicaster.makeStream()

        multicaster.yield(1)
        multicaster.yield(2)
        multicaster.yield(3)
        multicaster.finish()

        let firstValues = await first.reduce(into: [Int]()) { $0.append($1) }
        let secondValues = await second.reduce(into: [Int]()) { $0.append($1) }
        #expect(firstValues == [1, 2, 3])
        #expect(secondValues == [1, 2, 3])
    }

    @Test func replaysLatestValueToNewSubscribers() async throws {
        let multicaster = Multicaster<Int>(bufferingPolicy: .bufferingNewest(1), replayingLatest: 0)

        let early = multicaster.makeStream()
        var earlyIterator = early.makeAsyncIterator()
        #expect(await earlyIterator.next() == 0)

        multicaster.yield(42)

        let late = multicaster.makeStream()
        var lateIterator = late.makeAsyncIterator()
        #expect(await lateIterator.next() == 42)
    }

    @Test func doesNotReplayWhenDisabled() async throws {
        let multicaster = Multicaster<Int>()
        multicaster.yield(1)

        let stream = multicaster.makeStream()
        multicaster.yield(2)
        multicaster.finish()

        let values = await stream.reduce(into: [Int]()) { $0.append($1) }
        #expect(values == [2])
    }

    @Test func finishEndsExistingAndFutureStreams() async throws {
        let multicaster = Multicaster<Int>()
        let before = multicaster.makeStream()
        multicaster.finish()
        multicaster.yield(99)

        let beforeValues = await before.reduce(into: [Int]()) { $0.append($1) }
        #expect(beforeValues.isEmpty)

        let after = multicaster.makeStream()
        let afterValues = await after.reduce(into: [Int]()) { $0.append($1) }
        #expect(afterValues.isEmpty)
    }

    @Test func terminatedSubscribersAreRemoved() async throws {
        let multicaster = Multicaster<Int>()

        let consumed = multicaster.makeStream()
        let abandoned = multicaster.makeStream()

        // Consume-and-break terminates the abandoned stream's iteration.
        let task = Task {
            for await _ in abandoned { break }
        }
        multicaster.yield(1)
        _ = await task.value

        multicaster.yield(2)
        multicaster.finish()

        let values = await consumed.reduce(into: [Int]()) { $0.append($1) }
        #expect(values == [1, 2])
    }
}
