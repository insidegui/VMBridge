import Foundation
import Testing

@testable import VMBridge

@Suite(.timeLimit(.minutes(1))) struct SocketAndLifetimeTests {
    @Test func cancelledReadAndWriteCloseTheirDescriptors() async throws {
        let (a, b) = try socketPair()
        let read = Task { try await a.read() }
        read.cancel()
        await #expect(throws: (any Error).self) { try await read.value }
        await a.close()
        await b.close()

        let (c, d) = try socketPair()
        let write = Task { try await c.write(Data(repeating: 1, count: 16 * 1024 * 1024)) }
        write.cancel()
        await #expect(throws: (any Error).self) { try await write.value }
        await c.close()
        await d.close()
    }

    @Test func truncatedFrameFailsAtEOF() async throws {
        let (a, b) = try socketPair()
        let frame = WireProtocol.encodeFrame(type: .data, payload: Data([1, 2, 3]))
        try await b.write(Data(frame.dropLast()))
        await b.close()
        let reader = FrameReader(socket: a)
        await #expect(throws: VMBridgeError.malformedFrame) { try await reader.next() }
        await a.close()
    }

    @Test func cancellingHandshakeClosesCandidateAndRemovesListener() async throws {
        let source = TestHostSource()
        let host = VMConnection(hostSource: source, port: 1, configuration: .init())
        let (a, b) = try socketPair()
        await source.submit(a)
        let task = Task { try await host.run() }
        _ = try await firstValue(in: host.state) { $0 == .connecting }
        task.cancel()
        try await task.value
        #expect(await source.stops == 1)
        #expect(await host.currentState == .stopped)
        await b.close()
    }

    @Test func guestProtocolMismatchEndsRun() async throws {
        let (a, b) = try socketPair()
        let connector = TestConnector()
        connector.continuation.yield(a)
        let guest = VMConnection(connector: { try await connector.next() })
        let reader = FrameReader(socket: b)
        let writer = FrameWriter(socket: b)
        let task = Task { try await guest.run() }
        _ = try await reader.next()
        let wrongVersion = Data(#"{"protocolName":"VMBridge","version":99,"role":"host"}"#.utf8)
        try await writer.send(.accept, wrongVersion)
        await #expect(throws: VMBridgeError.incompatibleProtocol) { try await task.value }
        #expect(await guest.currentState == .stopped)
        await b.close()
    }

    @Test func cancellationDuringBackoffStopsRetryLoop() async throws {
        let guest = VMConnection(connector: { throw POSIXError(.ECONNREFUSED) })
        let task = Task { try await guest.run() }
        _ = try await firstValue(in: guest.state) { $0 == .disconnected }
        task.cancel()
        try await task.value
        #expect(await guest.currentState == .stopped)
    }

    @Test func disconnectClearsOnlyPendingBuffersAndExpiresOffers() async throws {
        let router = MessageRouter(undeliveredMessageRetention: .seconds(10))
        router.deliver(
            messageID: TestMessage.messageID,
            body: try JSONEncoder().encode(TestMessage(text: "old")))
        router.clearPending()
        let stream = router.stream(for: TestMessage.self)
        router.deliver(
            messageID: TestMessage.messageID,
            body: try JSONEncoder().encode(TestMessage(text: "new")))
        router.finishAll()
        let values = await stream.reduce(into: [String]()) { $0.append($1.payload.text) }
        #expect(values == ["new"])

        let manager = BulkTransferManager()
        await manager.activate { _, _, _ in }
        let offers = manager.makeOffersStream()
        let offerData = try JSONEncoder().encode(
            BulkOfferEnvelope(id: UUID(), metadata: .init(name: "old"), byteCount: 1))
        await manager.handle(type: .bulkOffer, payload: offerData)
        let offer = try await firstValue(in: offers)
        await manager.deactivate(with: .disconnected)
        await #expect(throws: VMBridgeError.disconnected) { try await offer.acceptAsStream() }
    }

    @Test func cancellingCompletionWaitDoesNotLeakContinuation() async throws {
        let operation = BulkTransferOperation(totalByteCount: 1, initialPhase: .transferring)
        let wait = Task { try await operation.waitForCompletion() }
        wait.cancel()
        await #expect(throws: CancellationError.self) { try await wait.value }
        operation.finish(with: .failure(VMBridgeError.disconnected))
        await #expect(throws: VMBridgeError.disconnected) {
            try await operation.waitForCompletion()
        }
    }
}

@Suite(.timeLimit(.minutes(1))) struct DeadlineTests {
    @Test func deadlineClosesBlockedHandshakeWrite() async throws {
        let (a, b) = try socketPair()
        let writer = FrameWriter(socket: a)
        await #expect(throws: VMBridgeError.handshakeTimedOut) {
            try await withHandshakeTimeout(
                {
                    try await withTaskCancellationHandler {
                        try await writer.send(.data, Data(repeating: 1, count: 8 * 1024 * 1024))
                    } onCancel: {
                        a.cancel()
                    }
                }, timeout: .milliseconds(30))
        }
        await writer.stop()
        await a.close()
        await b.close()
    }

    @Test func failedReplyEncodingStillConsumesHandle() async throws {
        let responder = RequestResponder(correlationID: UUID()) { _, _, _ in
            Issue.record("Unexpected reply")
        }
        await #expect(throws: TestFailure.timedOut) {
            try await responder.sendReply { throw TestFailure.timedOut }
        }
        await #expect(throws: VMBridgeError.alreadyResponded) {
            try await responder.sendReply { (Data(), "reply") }
        }
    }

    @Test func bufferedRequestRetainsReplyAndIsConsumedOnce() async throws {
        let router = MessageRouter(undeliveredMessageRetention: .seconds(10))
        let id = UUID()
        let replies = Multicaster<Data>()
        let responses = replies.makeStream()
        router.deliverRequest(
            messageID: Query.messageID, body: try JSONEncoder().encode(Query(value: 7)),
            correlationID: id
        ) { correlation, body, type in
            #expect(correlation == id)
            #expect(type == Answer.messageID)
            replies.yield(body)
        }
        let request = try await firstValue(in: router.requestStream(for: Query.self))
        try await request.reply(Answer(value: request.payload.value))
        #expect(
            try JSONDecoder().decode(Answer.self, from: await firstValue(in: responses)).value == 7)
        let later = router.requestStream(for: Query.self)
        router.finishAll()
        #expect(await later.reduce(0) { count, _ in count + 1 } == 0)
    }
}
