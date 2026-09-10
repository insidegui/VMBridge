import Foundation
import Testing

@testable import VMBridge

@Suite(.timeLimit(.minutes(1))) struct DuplexTransferTests {
    @Test func simultaneousTransfersDoNotBlockReadsBehindControlWrites() async throws {
        try await withConnectedPair { host, guest, _, _ in
            let folder = FileManager.default.temporaryDirectory.appendingPathComponent(
                UUID().uuidString)
            try FileManager.default.createDirectory(at: folder, withIntermediateDirectories: true)
            defer { try? FileManager.default.removeItem(at: folder) }
            let source = folder.appendingPathComponent("source")
            let bytes = Data(repeating: 0xBD, count: 4 * 1024 * 1024)
            try bytes.write(to: source)
            let hostOffers = host.bulkTransferOffers
            let guestOffers = guest.bulkTransferOffers
            try await withThrowingTaskGroup(of: Void.self) { group in
                group.addTask {
                    let offer = try await firstValue(in: hostOffers)
                    let transfer = try await offer.accept(to: folder.appendingPathComponent("host"))
                    _ = try await transfer.waitForCompletion()
                }
                group.addTask {
                    let offer = try await firstValue(in: guestOffers)
                    let transfer = try await offer.accept(
                        to: folder.appendingPathComponent("guest"))
                    _ = try await transfer.waitForCompletion()
                }
                group.addTask {
                    let transfer = try await host.sendFile(
                        at: source, metadata: .init(name: "to guest"))
                    _ = try await transfer.waitForCompletion()
                }
                group.addTask {
                    let transfer = try await guest.sendFile(
                        at: source, metadata: .init(name: "to host"))
                    _ = try await transfer.waitForCompletion()
                }
                try await group.waitForAll()
            }
            #expect(try Data(contentsOf: folder.appendingPathComponent("host")) == bytes)
            #expect(try Data(contentsOf: folder.appendingPathComponent("guest")) == bytes)
        }
    }

    @Test func disconnectFailsActiveTransferAndRemovesPartialFile() async throws {
        try await withConnectedPair { host, guest, source, _ in
            let folder = FileManager.default.temporaryDirectory.appendingPathComponent(
                UUID().uuidString)
            try FileManager.default.createDirectory(at: folder, withIntermediateDirectories: true)
            defer { try? FileManager.default.removeItem(at: folder) }
            let dataStream = AsyncThrowingStream<Data, any Error> { continuation in
                continuation.yield(Data(repeating: 1, count: 64 * 1024))
                // Leave the generated source open until disconnect cancels it.
            }
            let offers = host.bulkTransferOffers
            let outgoing = try await guest.sendBulkTransfer(
                .stream(byteCount: 10 * 1024 * 1024, chunks: dataStream),
                metadata: .init(name: "partial"))
            let offer = try await firstValue(in: offers)
            let incoming = try await offer.accept(to: folder.appendingPathComponent("destination"))
            _ = try await firstValue(in: incoming.progress) { $0.transferredByteCount > 0 }
            let (a, b) = try socketPair()
            await source.submit(a)
            let writer = FrameWriter(socket: b)
            let reader = FrameReader(socket: b)
            try await writer.send(.hello, JSONEncoder().encode(Handshake(role: "guest")))
            _ = try await reader.next()
            await #expect(throws: VMBridgeError.disconnected) {
                try await incoming.waitForCompletion()
            }
            await #expect(throws: VMBridgeError.disconnected) {
                try await outgoing.waitForCompletion()
            }
            #expect(try FileManager.default.contentsOfDirectory(atPath: folder.path).isEmpty)
            await b.close()
        }
    }
}
