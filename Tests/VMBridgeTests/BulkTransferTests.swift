import Foundation
import Testing

@testable import VMBridge

@Suite(.serialized, .timeLimit(.minutes(1)))
struct BulkTransferManagerTests {

    @Test func transfersLargeFileAtomically() async throws {
        let (alice, bob) = await makeManagers()
        let offers = bob.makeOffersStream()
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(
            at: directory,
            withIntermediateDirectories: true
        )
        defer { try? FileManager.default.removeItem(at: directory) }

        let sourceURL = directory.appendingPathComponent("source.bin")
        let destinationURL = directory.appendingPathComponent("received.bin")
        let source = Data(
            (0..<(2 * 1024 * 1024 + 137)).map { UInt8($0 % 251) }
        )
        try source.write(to: sourceURL)

        let outgoing = try await alice.start(
            source: .file(sourceURL),
            metadata: BulkTransferMetadata(
                name: "source.bin",
                contentType: "application/octet-stream"
            )
        )
        let offer = try await firstValue(in: offers)
        #expect(offer.byteCount == Int64(source.count))
        #expect(!FileManager.default.fileExists(atPath: destinationURL.path))

        let incoming = try await offer.accept(to: destinationURL)
        async let outgoingReceipt = outgoing.waitForCompletion()
        let incomingReceipt = try await incoming.waitForCompletion()
        let sentReceipt = try await outgoingReceipt

        #expect(try Data(contentsOf: destinationURL) == source)
        #expect(incomingReceipt.sha256Digest == sentReceipt.sha256Digest)
        #expect(incomingReceipt.byteCount == Int64(source.count))
    }

    @Test func streamTransferSupportsPauseResumeAndBackpressure() async throws {
        let (alice, bob) = await makeManagers()
        let offers = bob.makeOffersStream()
        let source = Data(
            (0..<(2 * 1024 * 1024 + 53)).map { UInt8($0 % 239) }
        )
        let (chunks, continuation) =
            AsyncThrowingStream<Data, any Error>.makeStream()
        continuation.yield(source)
        continuation.finish()

        let outgoing = try await alice.start(
            source: .stream(
                byteCount: Int64(source.count),
                chunks: chunks
            ),
            metadata: BulkTransferMetadata(name: "generated.data")
        )
        let offer = try await firstValue(in: offers)
        let accepted = try await offer.acceptAsStream()

        await accepted.transfer.pause()
        await accepted.transfer.resume()

        var received = Data()
        for try await chunk in accepted.bytes {
            #expect(chunk.count <= WireProtocol.maximumBulkChunkSize)
            received.append(chunk)
        }

        async let outgoingReceipt = outgoing.waitForCompletion()
        let incomingReceipt = try await accepted.transfer.waitForCompletion()
        let sentReceipt = try await outgoingReceipt
        #expect(received == source)
        #expect(incomingReceipt.sha256Digest == sentReceipt.sha256Digest)
    }

    @Test func rejectedAndShortSourcesFail() async throws {
        let (alice, bob) = await makeManagers()
        var offers = bob.makeOffersStream().makeAsyncIterator()

        let rejected = try await alice.start(
            source: .stream(
                byteCount: 0,
                chunks: AsyncThrowingStream { $0.finish() }
            ),
            metadata: BulkTransferMetadata(name: "reject-me")
        )
        let rejectedOffer = try #require(await offers.next())
        try await rejectedOffer.reject()
        await #expect(throws: VMBridgeError.self) {
            try await rejected.waitForCompletion()
        }

        let shortStream = AsyncThrowingStream<Data, any Error> {
            $0.yield(Data([1, 2, 3]))
            $0.finish()
        }
        let short = try await alice.start(
            source: .stream(byteCount: 4, chunks: shortStream),
            metadata: BulkTransferMetadata(name: "short")
        )
        let shortOffer = try #require(await offers.next())
        let accepted = try await shortOffer.acceptAsStream()
        let drain = Task {
            for try await _ in accepted.bytes {}
        }

        await #expect(throws: VMBridgeError.self) {
            try await short.waitForCompletion()
        }
        await #expect(throws: (any Error).self) {
            try await drain.value
        }
    }

    @Test func existingDestinationIsPreservedUntilVerifiedReplacement() async throws {
        let (alice, bob) = await makeManagers()
        var offers = bob.makeOffersStream().makeAsyncIterator()
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(
            at: directory,
            withIntermediateDirectories: true
        )
        defer { try? FileManager.default.removeItem(at: directory) }

        let sourceURL = directory.appendingPathComponent("new.bin")
        let destinationURL = directory.appendingPathComponent("existing.bin")
        let newContents = Data("new contents".utf8)
        let oldContents = Data("keep me".utf8)
        try newContents.write(to: sourceURL)
        try oldContents.write(to: destinationURL)

        let rejected = try await alice.start(
            source: .file(sourceURL),
            metadata: BulkTransferMetadata(name: "new.bin")
        )
        let firstOffer = try #require(await offers.next())
        await #expect(throws: VMBridgeError.self) {
            try await firstOffer.accept(to: destinationURL)
        }
        await #expect(throws: VMBridgeError.self) {
            try await rejected.waitForCompletion()
        }
        #expect(try Data(contentsOf: destinationURL) == oldContents)

        let replacement = try await alice.start(
            source: .file(sourceURL),
            metadata: BulkTransferMetadata(name: "new.bin")
        )
        let secondOffer = try #require(await offers.next())
        let incoming = try await secondOffer.accept(
            to: destinationURL,
            overwrite: true
        )
        async let sent = replacement.waitForCompletion()
        _ = try await incoming.waitForCompletion()
        _ = try await sent
        #expect(try Data(contentsOf: destinationURL) == newContents)
    }

    @Test func abandoningReceiveIteratorCancelsTransfer() async throws {
        let (alice, bob) = await makeManagers()
        let offers = bob.makeOffersStream()
        let byteCount = 2 * 1024 * 1024
        let chunks = AsyncThrowingStream<Data, any Error> {
            $0.yield(Data(repeating: 7, count: byteCount))
            $0.finish()
        }
        let outgoing = try await alice.start(
            source: .stream(
                byteCount: Int64(byteCount),
                chunks: chunks
            ),
            metadata: BulkTransferMetadata(name: "abandoned")
        )
        let offer = try await firstValue(in: offers)
        let accepted = try await offer.acceptAsStream()

        try await consumeOnlyFirstChunk(from: accepted.bytes)

        await #expect(throws: VMBridgeError.self) {
            try await accepted.transfer.waitForCompletion()
        }
        await #expect(throws: VMBridgeError.self) {
            try await outgoing.waitForCompletion()
        }
    }

    private func makeManagers() async -> (
        BulkTransferManager,
        BulkTransferManager
    ) {
        let alice = BulkTransferManager()
        let bob = BulkTransferManager()
        await alice.activate { type, payload, _ in
            await bob.handle(
                type: type,
                payload: payload
            )
        }
        await bob.activate { type, payload, _ in
            await alice.handle(
                type: type,
                payload: payload
            )
        }
        return (alice, bob)
    }

    private func consumeOnlyFirstChunk(
        from stream: BulkByteStream
    ) async throws {
        for try await _ in stream {
            break
        }
    }
}

@Suite(.timeLimit(.minutes(1))) struct BulkValidationTests {
    @Test func incomingAndOutgoingOffersExpire() async throws {
        let receiver = BulkTransferManager(offerTimeout: .milliseconds(30))
        let decisions = Multicaster<WireProtocol.FrameType>()
        let decisionStream = decisions.makeStream()
        await receiver.activate { type, _, _ in decisions.yield(type) }
        let offers = receiver.makeOffersStream()
        await receiver.handle(
            type: .bulkOffer,
            payload: try JSONEncoder().encode(
                BulkOfferEnvelope(id: UUID(), metadata: .init(name: "expiry"), byteCount: 0)))
        let offer = try await firstValue(in: offers)
        _ = try await firstValue(in: decisionStream) { $0 == .bulkDecision }
        await #expect(throws: VMBridgeError.bulkTransferOfferTimedOut) {
            try await offer.acceptAsStream()
        }
        await receiver.deactivate()

        let sender = BulkTransferManager(offerTimeout: .milliseconds(30))
        await sender.activate { _, _, _ in }
        let source = AsyncThrowingStream<Data, any Error> { $0.finish() }
        let transfer = try await sender.start(
            source: .stream(byteCount: 0, chunks: source), metadata: .init(name: "expiry"))
        await #expect(throws: VMBridgeError.bulkTransferOfferTimedOut) {
            try await transfer.waitForCompletion()
        }
        await sender.deactivate()
    }

    @Test func corruptDigestRemovesTemporaryFile() async throws {
        let receiver = BulkTransferManager()
        await receiver.activate { _, _, _ in }
        let folder = FileManager.default.temporaryDirectory.appendingPathComponent(
            UUID().uuidString)
        try FileManager.default.createDirectory(at: folder, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: folder) }
        let offers = receiver.makeOffersStream()
        let id = UUID()
        await receiver.handle(
            type: .bulkOffer,
            payload: try JSONEncoder().encode(
                BulkOfferEnvelope(id: id, metadata: .init(name: "corrupt"), byteCount: 3)))
        let offer = try await firstValue(in: offers)
        let transfer = try await offer.accept(to: folder.appendingPathComponent("destination"))
        await receiver.handle(
            type: .bulkChunk,
            payload: try BulkChunkEnvelope.encode(id: id, offset: 0, bytes: Data([1, 2, 3])))
        await receiver.handle(
            type: .bulkFinish,
            payload: try JSONEncoder().encode(
                BulkFinishEnvelope(
                    id: id, byteCount: 3, sha256Digest: Data(repeating: 0, count: 32))))
        await #expect(throws: VMBridgeError.bulkTransferIntegrityFailure) {
            try await transfer.waitForCompletion()
        }
        #expect(try FileManager.default.contentsOfDirectory(atPath: folder.path).isEmpty)
        await receiver.deactivate()
    }

    @Test func remoteCannotExceedReceiveCredit() async throws {
        let receiver = BulkTransferManager()
        await receiver.activate { _, _, _ in }
        let offers = receiver.makeOffersStream()
        let id = UUID()
        await receiver.handle(
            type: .bulkOffer,
            payload: try JSONEncoder().encode(
                BulkOfferEnvelope(
                    id: id, metadata: .init(name: "overflow"), byteCount: 2 * 1024 * 1024)))
        let offer = try await firstValue(in: offers)
        let accepted = try await offer.acceptAsStream()
        let bytes = Data(repeating: 1, count: 64 * 1024)
        for index in 0..<17 {
            await receiver.handle(
                type: .bulkChunk,
                payload: try BulkChunkEnvelope.encode(
                    id: id, offset: Int64(index * bytes.count), bytes: bytes))
        }
        await #expect(throws: VMBridgeError.malformedFrame) {
            try await accepted.transfer.waitForCompletion()
        }
        await receiver.deactivate()
    }
}
