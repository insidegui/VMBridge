import Foundation
import os

/// Application-defined information accompanying a bulk transfer.
///
/// Paths are never inferred from this value. Receivers choose the destination
/// explicitly, so names received from an untrusted peer cannot escape an
/// application-selected directory.
public struct BulkTransferMetadata: Codable, Hashable, Sendable {
    public var name: String
    public var contentType: String?
    public var userInfo: [String: String]

    public init(
        name: String,
        contentType: String? = nil,
        userInfo: [String: String] = [:]
    ) {
        self.name = name
        self.contentType = contentType
        self.userInfo = userInfo
    }
}

/// A source whose bytes can be transferred without first loading the entire
/// value into memory.
public enum BulkTransferSource: Sendable {
    /// A regular file. Its size is captured when the transfer is offered.
    case file(URL)

    /// A generated sequence with an exact expected length.
    ///
    /// Producing more or fewer bytes than `byteCount` fails the transfer.
    case stream(
        byteCount: Int64,
        chunks: AsyncThrowingStream<Data, any Error>
    )
}

/// The lifecycle phase reported by a bulk transfer.
public enum BulkTransferPhase: Hashable, Sendable {
    case waitingForAcceptance
    case transferring
    case pausedLocally
    case pausedByRemote
    case completed
}

/// A point-in-time bulk transfer progress snapshot.
public struct BulkTransferProgress: Hashable, Sendable {
    public let transferredByteCount: Int64
    public let totalByteCount: Int64
    public let phase: BulkTransferPhase

    public var fractionCompleted: Double {
        guard totalByteCount > 0 else {
            return phase == .completed ? 1 : 0
        }
        return min(1, Double(transferredByteCount) / Double(totalByteCount))
    }

    init(
        transferredByteCount: Int64,
        totalByteCount: Int64,
        phase: BulkTransferPhase
    ) {
        self.transferredByteCount = transferredByteCount
        self.totalByteCount = totalByteCount
        self.phase = phase
    }
}

/// Proof that the receiver accepted and verified all bytes in a transfer.
public struct BulkTransferReceipt: Hashable, Sendable {
    public let id: UUID
    public let byteCount: Int64
    public let sha256Digest: Data

    init(id: UUID, byteCount: Int64, sha256Digest: Data) {
        self.id = id
        self.byteCount = byteCount
        self.sha256Digest = sha256Digest
    }
}

/// A running outgoing bulk transfer.
public final class OutgoingBulkTransfer: Sendable {
    public let id: UUID

    public let metadata: BulkTransferMetadata
    public let byteCount: Int64

    private let operation: BulkTransferOperation
    private let controls: BulkTransferControls

    init(
        id: UUID,
        metadata: BulkTransferMetadata,
        byteCount: Int64,
        operation: BulkTransferOperation,
        controls: BulkTransferControls
    ) {
        self.id = id

        self.metadata = metadata
        self.byteCount = byteCount
        self.operation = operation
        self.controls = controls
    }

    /// A fresh stream that immediately yields the current progress and then
    /// subsequent changes.
    public var progress: AsyncStream<BulkTransferProgress> {
        operation.progress
    }

    public func pause() async {
        await controls.pause()
    }

    public func resume() async {
        await controls.resume()
    }

    public func cancel() async {
        await controls.cancel()
    }

    public func waitForCompletion() async throws -> BulkTransferReceipt {
        try await operation.waitForCompletion()
    }
}

/// A running incoming bulk transfer.
public final class IncomingBulkTransfer: Sendable {
    public let id: UUID

    public let metadata: BulkTransferMetadata
    public let byteCount: Int64
    public let destinationURL: URL?

    private let operation: BulkTransferOperation
    private let controls: BulkTransferControls

    init(
        id: UUID,
        metadata: BulkTransferMetadata,
        byteCount: Int64,
        destinationURL: URL?,
        operation: BulkTransferOperation,
        controls: BulkTransferControls
    ) {
        self.id = id

        self.metadata = metadata
        self.byteCount = byteCount
        self.destinationURL = destinationURL
        self.operation = operation
        self.controls = controls
    }

    public var progress: AsyncStream<BulkTransferProgress> {
        operation.progress
    }

    public func pause() async {
        await controls.pause()
    }

    public func resume() async {
        await controls.resume()
    }

    public func cancel() async {
        await controls.cancel()
    }

    public func waitForCompletion() async throws -> BulkTransferReceipt {
        try await operation.waitForCompletion()
    }
}

/// A bounded, single-consumer byte sequence for an accepted incoming transfer.
///
/// Receiver credit is replenished only as iteration hands chunks to the
/// consumer. This applies transport backpressure without buffering a large
/// transfer in memory.
public struct BulkByteStream: AsyncSequence, Sendable {
    public typealias Element = Data

    public struct AsyncIterator: AsyncIteratorProtocol {
        fileprivate let storage: BulkByteStreamStorage
        fileprivate let lifetime: BulkByteStreamLifetime

        public mutating func next() async throws -> Data? {
            try await storage.next()
        }
    }

    fileprivate let storage: BulkByteStreamStorage
    fileprivate let lifetime: BulkByteStreamLifetime

    init(storage: BulkByteStreamStorage) {
        self.storage = storage
        self.lifetime = BulkByteStreamLifetime(storage: storage)
    }

    public func makeAsyncIterator() -> AsyncIterator {
        AsyncIterator(
            storage: storage,
            lifetime: BulkByteStreamLifetime(storage: storage)
        )
    }
}

/// An incoming transfer awaiting an explicit accept or reject decision.
public struct IncomingBulkTransferOffer: Sendable {
    public let id: UUID

    public let metadata: BulkTransferMetadata
    public let byteCount: Int64
    public let receivedAt: Date

    private let responder: IncomingBulkTransferResponder

    init(
        id: UUID,
        metadata: BulkTransferMetadata,
        byteCount: Int64,
        receivedAt: Date,
        responder: IncomingBulkTransferResponder
    ) {
        self.id = id

        self.metadata = metadata
        self.byteCount = byteCount
        self.receivedAt = receivedAt
        self.responder = responder
    }

    /// Accepts into an application-selected destination.
    ///
    /// Bytes are written to a temporary sibling and installed atomically only
    /// after the declared length and SHA-256 digest have been verified.
    public func accept(
        to destinationURL: URL,
        overwrite: Bool = false
    ) async throws -> IncomingBulkTransfer {
        let acceptance = try await responder.respond(
            .file(destinationURL, overwrite: overwrite)
        )
        guard let acceptance else {
            preconditionFailure("File acceptance did not create a transfer")
        }
        return acceptance.transfer
    }

    /// Accepts as a bounded asynchronous byte stream.
    public func acceptAsStream() async throws -> (
        transfer: IncomingBulkTransfer,
        bytes: BulkByteStream
    ) {
        let acceptance = try await responder.respond(.stream)
        guard let acceptance, let bytes = acceptance.bytes else {
            preconditionFailure("Stream acceptance did not create a byte stream")
        }
        return (acceptance.transfer, bytes)
    }

    public func reject() async throws {
        _ = try await responder.respond(.reject)
    }
}

struct BulkTransferControls: Sendable {
    let pause: @Sendable () async -> Void
    let resume: @Sendable () async -> Void
    let cancel: @Sendable () async -> Void
}

final class BulkTransferOperation: Sendable {
    private struct State {
        var result: Result<BulkTransferReceipt, any Error>?
        var waiters: [UUID: CheckedContinuation<BulkTransferReceipt, any Error>] = [:]
    }

    private let state = OSAllocatedUnfairLock(initialState: State())
    private let progressMulticaster: Multicaster<BulkTransferProgress>

    init(totalByteCount: Int64, initialPhase: BulkTransferPhase) {
        progressMulticaster = Multicaster(
            bufferingPolicy: .bufferingNewest(1),
            replayingLatest: BulkTransferProgress(
                transferredByteCount: 0,
                totalByteCount: totalByteCount,
                phase: initialPhase
            )
        )
    }

    var progress: AsyncStream<BulkTransferProgress> {
        progressMulticaster.makeStream()
    }

    func yield(
        transferredByteCount: Int64,
        totalByteCount: Int64,
        phase: BulkTransferPhase
    ) {
        progressMulticaster.yield(
            BulkTransferProgress(
                transferredByteCount: transferredByteCount,
                totalByteCount: totalByteCount,
                phase: phase
            )
        )
    }

    func finish(with result: Result<BulkTransferReceipt, any Error>) {
        let waiters: [CheckedContinuation<BulkTransferReceipt, any Error>] = state.withLock {
            state in
            guard state.result == nil else { return [] }
            state.result = result
            let waiters = Array(state.waiters.values)
            state.waiters.removeAll()
            return waiters
        }
        for waiter in waiters {
            waiter.resume(with: result)
        }
        progressMulticaster.finish()
    }

    func waitForCompletion() async throws -> BulkTransferReceipt {
        try Task.checkCancellation()
        let id = UUID()
        return try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation { continuation in
                let result: Result<BulkTransferReceipt, any Error>? = state.withLock { state in
                    if Task.isCancelled { return .failure(CancellationError()) }
                    if let result = state.result { return result }
                    state.waiters[id] = continuation
                    return nil
                }
                if let result { continuation.resume(with: result) }
            }
        } onCancel: {
            let waiter = self.state.withLock { $0.waiters.removeValue(forKey: id) }
            waiter?.resume(throwing: CancellationError())
        }
    }

}

enum IncomingBulkTransferDecision: Sendable {
    case file(URL, overwrite: Bool)
    case stream
    case reject
}

struct IncomingBulkTransferAcceptance: Sendable {
    let transfer: IncomingBulkTransfer
    let bytes: BulkByteStream?
}

actor IncomingBulkTransferResponder {
    typealias Respond =
        @Sendable (
            IncomingBulkTransferDecision
        ) async throws -> IncomingBulkTransferAcceptance?

    private var hasResponded = false
    private let respondClosure: Respond

    init(respond: @escaping Respond) {
        respondClosure = respond
    }

    func respond(
        _ decision: IncomingBulkTransferDecision
    ) async throws -> IncomingBulkTransferAcceptance? {
        guard !hasResponded else {
            throw VMBridgeError.bulkTransferAlreadyDecided
        }
        hasResponded = true
        return try await respondClosure(decision)
    }
}

actor BulkByteStreamStorage {
    private var queue: [Data] = []
    private var waiter: CheckedContinuation<Data?, any Error>?
    private var completion: Result<Void, any Error>?
    private let onConsumed: @Sendable (Int) async -> Void
    private let onCancelled: @Sendable () async -> Void

    init(
        onConsumed: @escaping @Sendable (Int) async -> Void,
        onCancelled: @escaping @Sendable () async -> Void
    ) {
        self.onConsumed = onConsumed
        self.onCancelled = onCancelled
    }

    func append(_ data: Data) {
        guard completion == nil else { return }
        if let waiter {
            self.waiter = nil
            waiter.resume(returning: data)
            Task { await onConsumed(data.count) }
        } else {
            queue.append(data)
        }
    }

    func finish(throwing error: (any Error)? = nil) {
        guard completion == nil else { return }
        if let error {
            completion = .failure(error)
            queue.removeAll()
        } else {
            completion = .success(())
        }
        guard queue.isEmpty, let waiter else { return }
        self.waiter = nil
        switch completion {
        case .success:
            waiter.resume(returning: nil)
        case .failure(let error):
            waiter.resume(throwing: error)
        case nil:
            break
        }
    }

    func next() async throws -> Data? {
        try Task.checkCancellation()
        if !queue.isEmpty {
            let data = queue.removeFirst()
            await onConsumed(data.count)
            return data
        }
        if let completion {
            try completion.get()
            return nil
        }
        return try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation { continuation in
                precondition(waiter == nil, "BulkByteStream supports one consumer")
                waiter = continuation
                if Task.isCancelled {
                    waiter = nil
                    continuation.resume(throwing: CancellationError())
                    Task { await onCancelled() }
                }
            }
        } onCancel: {
            Task { await self.consumerCancelled() }
        }
    }

    private func consumerCancelled() {
        if let waiter {
            self.waiter = nil
            waiter.resume(throwing: CancellationError())
        }
        Task { await onCancelled() }
    }

    func abandon() {
        guard completion == nil else { return }
        consumerCancelled()
    }
}

final class BulkByteStreamLifetime: Sendable {
    private let storage: BulkByteStreamStorage

    init(storage: BulkByteStreamStorage) {
        self.storage = storage
    }

    deinit {
        let storage = self.storage
        Task { await storage.abandon() }
    }
}
