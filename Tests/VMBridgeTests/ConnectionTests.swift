import Foundation
import Testing

@testable import VMBridge

@Suite(.timeLimit(.minutes(1))) struct ConnectionTests {
    @Test func messagesFanOutAndLateSubscribersReceiveBuffer() async throws {
        try await withConnectedPair { host, guest, _, _ in
            let first = host.messages(of: TestMessage.self)
            let second = host.messages(of: TestMessage.self)
            try await guest.send(TestMessage(text: "hello"))
            #expect(try await firstValue(in: first).payload.text == "hello")
            #expect(try await firstValue(in: second).payload.text == "hello")
            try await host.send(TestMessage(text: "buffered"))
            #expect(
                try await firstValue(in: guest.messages(of: TestMessage.self)).payload.text
                    == "buffered")
        }
    }

    @Test func requestsCorrelateAndReplyExactlyOnce() async throws {
        try await withConnectedPair { host, guest, _, _ in
            let incoming = host.requests(of: Query.self)
            try await withThrowingTaskGroup(of: Void.self) { group in
                group.addTask {
                    var iterator = incoming.makeAsyncIterator()
                    for _ in 0..<20 {
                        let request = try #require(await iterator.next())
                        try await request.reply(Answer(value: request.payload.value * 2))
                        await #expect(throws: VMBridgeError.alreadyResponded) {
                            try await request.reply(Answer(value: 0))
                        }
                    }
                }
                for value in 0..<20 {
                    group.addTask {
                        let reply = try await guest.send(
                            Query(value: value), expecting: Answer.self)
                        #expect(reply.value == value * 2)
                    }
                }
                try await group.waitForAll()
            }
        }
    }

    @Test func replyTypeMustMatchAndTimeoutDoesNotDisconnect() async throws {
        try await withConnectedPair { host, guest, _, _ in
            let incoming = host.requests(of: Query.self)
            let responder = Task {
                let request = try await firstValue(in: incoming)
                try await request.reply(TestMessage(text: "wrong type"))
            }
            await #expect(
                throws: VMBridgeError.unexpectedReplyType(
                    expected: Answer.messageID, actual: TestMessage.messageID)
            ) {
                try await guest.send(Query(value: 1), expecting: Answer.self)
            }
            try await responder.value
            await #expect(throws: VMBridgeError.requestTimedOut) {
                try await guest.send(
                    TestMessage(text: "unhandled"), expecting: Answer.self,
                    timeout: .milliseconds(30))
            }
            #expect(await guest.currentState == .connected)
        }
    }

    @Test func cancellationAbandonsRequestWithoutClosingConnection() async throws {
        try await withConnectedPair { host, guest, _, _ in
            let incoming = host.requests(of: Query.self)
            let task = Task { try await guest.send(Query(value: 1), expecting: Answer.self) }
            _ = try await firstValue(in: incoming)
            task.cancel()
            await #expect(throws: CancellationError.self) { try await task.value }
            #expect(await guest.currentState == .connected)
            await #expect(throws: VMBridgeError.alreadyRunning) { try await guest.run() }
        }
    }

    @Test func hostReplacementInvalidatesOldRepliesAndPendingRequests() async throws {
        try await withConnectedPair { host, guest, source, _ in
            let incoming = host.requests(of: Query.self)
            let pending = Task { try await guest.send(Query(value: 1), expecting: Answer.self) }
            let oldRequest = try await firstValue(in: incoming)
            let messages = host.messages(of: TestMessage.self)
            let (hostSocket, guestSocket) = try socketPair()
            let connector = TestConnector()
            connector.continuation.yield(guestSocket)
            let replacement = VMConnection(connector: { try await connector.next() })
            await source.submit(hostSocket)
            let run = Task { try await replacement.run() }
            do {
                _ = try await firstValue(in: replacement.state) { $0 == .connected }
                try await replacement.send(TestMessage(text: "replacement"))
                #expect(try await firstValue(in: messages).payload.text == "replacement")
                await #expect(throws: VMBridgeError.disconnected) {
                    try await oldRequest.reply(Answer(value: 1))
                }
                await #expect(throws: VMBridgeError.disconnected) { try await pending.value }
            } catch {
                run.cancel()
                _ = try? await run.value
                throw error
            }
            run.cancel()
            try await run.value
        }
    }

    @Test func incompleteCandidateDoesNotDisplaceEstablishedSession() async throws {
        try await withConnectedPair { host, guest, source, _ in
            let (candidate, remote) = try socketPair()
            await source.submit(candidate)
            let messages = host.messages(of: TestMessage.self)
            try await guest.send(TestMessage(text: "still alive"))
            #expect(try await firstValue(in: messages).payload.text == "still alive")
            await remote.close()
        }
    }

    @Test func guestReconnectsUsingSameSubscriptions() async throws {
        try await withConnectedPair { host, guest, source, connector in
            let messages = guest.messages(of: TestMessage.self)
            // A new host-side connection replaces the existing pair, causing
            // the original guest to reconnect through its queued next socket.
            let (interloperHost, interloper) = try socketPair()
            await source.submit(interloperHost)
            let writer = FrameWriter(socket: interloper)
            let reader = FrameReader(socket: interloper)
            try await writer.send(.hello, JSONEncoder().encode(Handshake(role: "guest")))
            _ = try await reader.next()
            _ = try await firstValue(in: guest.state) { $0 == .disconnected }
            let (newHost, newGuest) = try socketPair()
            await source.submit(newHost)
            connector.continuation.yield(newGuest)
            _ = try await firstValue(in: guest.state) { $0 == .connected }
            // Wait for the replacement to be installed at the host as well.
            let ready = host.messages(of: Query.self)
            try await guest.send(Query(value: 42))
            _ = try await firstValue(in: ready)
            try await host.send(TestMessage(text: "reconnected"))
            #expect(try await firstValue(in: messages).payload.text == "reconnected")
            await interloper.close()
        }
    }

    @Test func lifecycleRestartsAndCancellationWhileWaitingCleansUp() async throws {
        let source = TestHostSource()
        let host = VMConnection(hostSource: source, port: 1, configuration: .init())
        let messages = host.messages(of: TestMessage.self)
        for cycle in 0..<2 {
            let connector = TestConnector()
            let guest = VMConnection(connector: { try await connector.next() })
            let (a, b) = try socketPair()
            await source.submit(a)
            connector.continuation.yield(b)
            try await withThrowingTaskGroup(of: Void.self) { group in
                group.addTask { try await host.run() }
                group.addTask { try await guest.run() }
                do {
                    _ = try await firstValue(in: guest.state) { $0 == .connected }
                    try await guest.send(TestMessage(text: "\(cycle)"))
                    #expect(try await firstValue(in: messages).payload.text == "\(cycle)")
                } catch {
                    group.cancelAll()
                    throw error
                }
                group.cancelAll()
            }
            #expect(await host.currentState == .stopped)
        }
        #expect(await source.starts == 2)
        #expect(await source.stops == 2)
        let idle = VMConnection(connector: {
            try await Task.sleep(for: .seconds(60))
            throw TestFailure.timedOut
        })
        let run = Task { try await idle.run() }
        _ = try await firstValue(in: idle.state) { $0 == .connecting }
        run.cancel()
        try await run.value
        #expect(await idle.currentState == .stopped)
    }

    @Test func invalidConfigurationAndDisconnectedSendsFail() async throws {
        let invalid = VMConnection.guest(port: 0)
        await #expect(throws: VMBridgeError.invalidPort) { try await invalid.run() }
        await #expect(throws: VMBridgeError.notRunning) {
            try await invalid.send(TestMessage(text: "x"))
        }
        #expect(throws: VMBridgeError.invalidDescriptor) { try SocketLink(duplicating: -1) }
    }

    @Test func filesAreMultiplexedWithMessages() async throws {
        try await withConnectedPair { host, guest, _, _ in
            let folder = FileManager.default.temporaryDirectory.appendingPathComponent(
                UUID().uuidString)
            try FileManager.default.createDirectory(at: folder, withIntermediateDirectories: true)
            defer { try? FileManager.default.removeItem(at: folder) }
            let source = folder.appendingPathComponent("source")
            let destination = folder.appendingPathComponent("destination")
            let contents = Data(repeating: 0xA5, count: 10 * 1024 * 1024)
            try contents.write(to: source)
            let offers = host.bulkTransferOffers
            let received = Task {
                let offer = try await firstValue(in: offers)
                let transfer = try await offer.accept(to: destination)
                return try await transfer.waitForCompletion()
            }
            let messages = host.messages(of: TestMessage.self)
            let transfer = try await guest.sendFile(at: source, metadata: .init(name: "file"))
            try await guest.send(TestMessage(text: "during transfer"))
            #expect(try await firstValue(in: messages).payload.text == "during transfer")
            let sent = try await transfer.waitForCompletion()
            #expect(try await received.value.sha256Digest == sent.sha256Digest)
            #expect(try Data(contentsOf: destination) == contents)
        }
    }
}
