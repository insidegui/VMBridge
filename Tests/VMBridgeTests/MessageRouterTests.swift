import Foundation
import Testing

@testable import VMBridge

private struct RoutedMessage: VMMessage, Equatable {
    var value: Int
}

private struct RenamedMessage: VMMessage, Equatable {
    static var messageID: String { "custom-wire-id" }
    var text: String
}

@Suite struct MessageRouterTests {

    @Test func deliversDecodedMessagesToSubscribers() async throws {
        let router = MessageRouter(undeliveredMessageRetention: .seconds(10))
        let stream = router.stream(for: RoutedMessage.self)

        let body = try JSONEncoder().encode(RoutedMessage(value: 42))
        router.deliver(messageID: RoutedMessage.messageID, body: body)
        router.finishAll()

        let received = await stream.reduce(into: [ReceivedMessage<RoutedMessage>]()) {
            $0.append($1)
        }
        #expect(received.count == 1)
        #expect(received.first?.payload == RoutedMessage(value: 42))
    }

    @Test func multipleSubscribersEachReceiveEveryMessage() async throws {
        let router = MessageRouter(undeliveredMessageRetention: .seconds(10))
        let first = router.stream(for: RoutedMessage.self)
        let second = router.stream(for: RoutedMessage.self)

        let body = try JSONEncoder().encode(RoutedMessage(value: 7))
        router.deliver(messageID: RoutedMessage.messageID, body: body)
        router.finishAll()

        let firstValues = await first.reduce(into: [ReceivedMessage<RoutedMessage>]()) {
            $0.append($1)
        }
        let secondValues = await second.reduce(into: [ReceivedMessage<RoutedMessage>]()) {
            $0.append($1)
        }
        #expect(firstValues.map(\.payload) == [RoutedMessage(value: 7)])
        #expect(secondValues.map(\.payload) == [RoutedMessage(value: 7)])
    }

    @Test func routesByCustomMessageID() async throws {
        let router = MessageRouter(undeliveredMessageRetention: .seconds(10))
        let stream = router.stream(for: RenamedMessage.self)

        let body = try JSONEncoder().encode(RenamedMessage(text: "hi"))
        router.deliver(messageID: "custom-wire-id", body: body)
        router.finishAll()

        let received = await stream.reduce(into: [ReceivedMessage<RenamedMessage>]()) {
            $0.append($1)
        }
        #expect(received.first?.payload == RenamedMessage(text: "hi"))
    }

    @Test func ignoresMessagesWithNoSubscribersAndUndecodableBodies() async throws {
        let router = MessageRouter(undeliveredMessageRetention: .seconds(10))

        // No subscribers for this ID: must not crash.
        router.deliver(messageID: "nobody-listens", body: Data([1, 2, 3]))

        // Undecodable body for a subscribed type: dropped, stream stays usable.
        let stream = router.stream(for: RoutedMessage.self)
        router.deliver(messageID: RoutedMessage.messageID, body: Data("not json".utf8))

        let body = try JSONEncoder().encode(RoutedMessage(value: 1))
        router.deliver(messageID: RoutedMessage.messageID, body: body)
        router.finishAll()

        let received = await stream.reduce(into: [ReceivedMessage<RoutedMessage>]()) {
            $0.append($1)
        }
        #expect(received.map(\.payload) == [RoutedMessage(value: 1)])
    }
}

@Suite struct MessageBufferingTests {

    private func encode(_ value: Int) throws -> Data {
        try JSONEncoder().encode(RoutedMessage(value: value))
    }

    @Test func undeliveredMessageReachesLateSubscriber() async throws {
        let router = MessageRouter(undeliveredMessageRetention: .seconds(10))

        let arrival = Date()
        router.deliver(messageID: RoutedMessage.messageID, body: try encode(1))

        // Subscribe after the fact: the message is drained from the buffer.
        let stream = router.stream(for: RoutedMessage.self)
        router.finishAll()

        let received = await stream.reduce(into: [ReceivedMessage<RoutedMessage>]()) {
            $0.append($1)
        }
        #expect(received.map(\.payload) == [RoutedMessage(value: 1)])

        // receivedAt reflects arrival, not drain time.
        let receivedAt = try #require(received.first?.receivedAt)
        #expect(abs(receivedAt.timeIntervalSince(arrival)) < 1)
    }

    @Test func deliveredMessagesAreNotBuffered() async throws {
        let router = MessageRouter(undeliveredMessageRetention: .seconds(10))

        // A live subscriber exists, so delivery is immediate...
        let live = router.stream(for: RoutedMessage.self)
        router.deliver(messageID: RoutedMessage.messageID, body: try encode(1))

        // ...and a later subscriber receives nothing from the past.
        let late = router.stream(for: RoutedMessage.self)
        router.finishAll()

        let liveValues = await live.reduce(into: [ReceivedMessage<RoutedMessage>]()) {
            $0.append($1)
        }
        let lateValues = await late.reduce(into: [ReceivedMessage<RoutedMessage>]()) {
            $0.append($1)
        }
        #expect(liveValues.map(\.payload) == [RoutedMessage(value: 1)])
        #expect(lateValues.isEmpty)
    }

    @Test func backlogIsConsumedOnce() async throws {
        let router = MessageRouter(undeliveredMessageRetention: .seconds(10))

        router.deliver(messageID: RoutedMessage.messageID, body: try encode(1))

        let first = router.stream(for: RoutedMessage.self)
        let second = router.stream(for: RoutedMessage.self)
        router.finishAll()

        let firstValues = await first.reduce(into: [ReceivedMessage<RoutedMessage>]()) {
            $0.append($1)
        }
        let secondValues = await second.reduce(into: [ReceivedMessage<RoutedMessage>]()) {
            $0.append($1)
        }
        #expect(firstValues.map(\.payload) == [RoutedMessage(value: 1)])
        #expect(secondValues.isEmpty)
    }

    @Test func backlogPrecedesLiveMessagesInArrivalOrder() async throws {
        let router = MessageRouter(undeliveredMessageRetention: .seconds(10))

        router.deliver(messageID: RoutedMessage.messageID, body: try encode(1))
        router.deliver(messageID: RoutedMessage.messageID, body: try encode(2))

        let stream = router.stream(for: RoutedMessage.self)
        router.deliver(messageID: RoutedMessage.messageID, body: try encode(3))
        router.finishAll()

        let received = await stream.reduce(into: [ReceivedMessage<RoutedMessage>]()) {
            $0.append($1)
        }
        #expect(received.map(\.payload.value) == [1, 2, 3])
    }

    @Test func zeroRetentionRestoresDropSemantics() async throws {
        let router = MessageRouter(undeliveredMessageRetention: .zero)

        router.deliver(messageID: RoutedMessage.messageID, body: try encode(1))

        let stream = router.stream(for: RoutedMessage.self)
        router.finishAll()

        let received = await stream.reduce(into: [ReceivedMessage<RoutedMessage>]()) {
            $0.append($1)
        }
        #expect(received.isEmpty)
    }

    @Test func expiredMessagesAreNotDelivered() async throws {
        let router = MessageRouter(undeliveredMessageRetention: .milliseconds(50))

        router.deliver(messageID: RoutedMessage.messageID, body: try encode(1))
        try await Task.sleep(for: .milliseconds(150))

        let stream = router.stream(for: RoutedMessage.self)
        router.finishAll()

        let received = await stream.reduce(into: [ReceivedMessage<RoutedMessage>]()) {
            $0.append($1)
        }
        #expect(received.isEmpty)
    }

    @Test func perTypeEntryCapKeepsNewest() async throws {
        let router = MessageRouter(undeliveredMessageRetention: .seconds(10))

        for value in 0..<80 {
            router.deliver(messageID: RoutedMessage.messageID, body: try encode(value))
        }

        let stream = router.stream(for: RoutedMessage.self)
        router.finishAll()

        let received = await stream.reduce(into: [ReceivedMessage<RoutedMessage>]()) {
            $0.append($1)
        }
        #expect(received.count == 64)
        #expect(received.map(\.payload.value) == Array(16..<80))
    }

    @Test func totalByteBudgetEvictsOldest() async throws {
        let router = MessageRouter(undeliveredMessageRetention: .seconds(10))

        // Two large undelivered payloads for another type push the total
        // over the 512 KiB budget, evicting the oldest entries first.
        let bigText = String(repeating: "x", count: 300 * 1024)
        let bigBody = try JSONEncoder().encode(RenamedMessage(text: bigText))

        router.deliver(messageID: RoutedMessage.messageID, body: try encode(1))
        router.deliver(messageID: "custom-wire-id", body: bigBody)
        router.deliver(messageID: "custom-wire-id", body: bigBody)

        let small = router.stream(for: RoutedMessage.self)
        let big = router.stream(for: RenamedMessage.self)
        router.finishAll()

        let smallValues = await small.reduce(into: [ReceivedMessage<RoutedMessage>]()) {
            $0.append($1)
        }
        let bigValues = await big.reduce(into: [ReceivedMessage<RenamedMessage>]()) {
            $0.append($1)
        }

        // The small early message and the first big one were evicted to fit
        // the second big message.
        #expect(smallValues.isEmpty)
        #expect(bigValues.count == 1)
    }
}

@Suite struct BufferLimitTests {
    @Test func emptyBodiesCannotCreateUnlimitedPendingTypes() async throws {
        let router = MessageRouter(undeliveredMessageRetention: .seconds(10))
        router.deliver(
            messageID: TestMessage.messageID,
            body: try JSONEncoder().encode(TestMessage(text: "oldest")))
        for index in 0..<65 { router.deliver(messageID: "unknown-\(index)", body: Data()) }
        let stream = router.stream(for: TestMessage.self)
        router.finishAll()
        #expect(await stream.reduce(0) { count, _ in count + 1 } == 0)
    }
}
