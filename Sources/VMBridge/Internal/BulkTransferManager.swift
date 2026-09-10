import CryptoKit
import Foundation

/// Owns connection-bound bulk-transfer state independently of the concrete
/// transport. All mutable state is scoped to one physical connection.
actor BulkTransferManager {
    typealias SendFrame =
        @Sendable (
            _ type: WireProtocol.FrameType,
            _ payload: Data,
            _ transferID: UUID
        ) async throws -> Void

    private let offerTimeout: Duration
    static let receiveWindowByteCount: Int64 = 1024 * 1024

    private nonisolated let offersMulticaster: Multicaster<IncomingBulkTransferOffer>

    init(
        offers: Multicaster<IncomingBulkTransferOffer> = Multicaster(),
        offerTimeout: Duration = .seconds(30)
    ) {
        offersMulticaster = offers
        self.offerTimeout = offerTimeout
    }

    private enum Direction: Sendable {
        case outgoing
        case incoming
    }

    private struct OutgoingState {

        let source: BulkTransferSource
        let metadata: BulkTransferMetadata
        let byteCount: Int64
        let operation: BulkTransferOperation
        var sentByteCount: Int64 = 0
        var acknowledgedByteCount: Int64 = 0
        var digest: Data?
        var isAccepted = false
        var isPausedLocally = false
        var isPausedRemotely = false
        var signal: CheckedContinuation<Void, Never>?
        var task: Task<Void, Never>?
        var timeoutTask: Task<Void, Never>?
    }

    private struct PendingIncomingState {

        let metadata: BulkTransferMetadata
        let byteCount: Int64
        let receivedAt: Date
        var timeoutTask: Task<Void, Never>?
    }

    private struct FileSink {
        let destinationURL: URL
        let temporaryURL: URL
        let overwrite: Bool
        let handle: FileHandle
    }

    private enum IncomingSink {
        case file(FileSink)
        case stream(BulkByteStreamStorage)
    }

    private struct IncomingState {

        let metadata: BulkTransferMetadata
        let byteCount: Int64
        let operation: BulkTransferOperation
        let sink: IncomingSink
        var receivedByteCount: Int64 = 0
        var consumedByteCount: Int64 = 0
        var hasher = SHA256()
        var finishedDigest: Data?
        var isPausedLocally = false
        var isPausedRemotely = false
    }

    private var sendFrame: SendFrame?
    private var outgoing: [UUID: OutgoingState] = [:]
    private var pendingIncoming: [UUID: PendingIncomingState] = [:]
    private var incoming: [UUID: IncomingState] = [:]

    nonisolated func makeOffersStream() -> AsyncStream<IncomingBulkTransferOffer> {
        offersMulticaster.makeStream()
    }

    func activate(sendFrame: @escaping SendFrame) {
        self.sendFrame = sendFrame
    }

    func deactivate(with error: VMBridgeError = .notRunning) async {
        sendFrame = nil

        let outgoingIDs = Array(outgoing.keys)
        for id in outgoingIDs {
            failOutgoing(id: id, error: error)
        }

        let incomingIDs = Array(incoming.keys)
        for id in incomingIDs {
            await failIncoming(
                id: id,
                error: error,
                notifyRemote: false
            )
        }

        for pending in pendingIncoming.values {
            pending.timeoutTask?.cancel()
        }
        pendingIncoming.removeAll()
    }

    func start(
        source: BulkTransferSource,
        metadata: BulkTransferMetadata
    ) async throws -> OutgoingBulkTransfer {
        guard sendFrame != nil else { throw VMBridgeError.notRunning }
        guard outgoing.count < 32 else { throw VMBridgeError.tooManyTransfers }
        let byteCount = try Self.byteCount(for: source)
        guard byteCount >= 0 else {
            throw VMBridgeError.bulkTransferLengthMismatch(
                expected: 0,
                actual: byteCount
            )
        }

        let id = UUID()
        let envelope = try encodeControl(
            BulkOfferEnvelope(
                id: id,
                metadata: metadata,
                byteCount: byteCount
            )
        )
        let operation = BulkTransferOperation(
            totalByteCount: byteCount,
            initialPhase: .waitingForAcceptance
        )
        outgoing[id] = OutgoingState(
            source: source,
            metadata: metadata,
            byteCount: byteCount,
            operation: operation
        )

        do {
            try await send(.bulkOffer, envelope, transferID: id)
        } catch {
            outgoing[id] = nil
            operation.finish(with: .failure(error))
            throw error
        }

        guard outgoing[id] != nil, sendFrame != nil else { throw VMBridgeError.disconnected }
        let duration = offerTimeout
        let timeout = Task { [weak self] in
            try? await Task.sleep(for: duration)
            guard !Task.isCancelled else { return }
            await self?.outgoingOfferTimedOut(id)
        }
        outgoing[id]?.timeoutTask = timeout

        return OutgoingBulkTransfer(
            id: id,
            metadata: metadata,
            byteCount: byteCount,
            operation: operation,
            controls: controls(for: id, direction: .outgoing)
        )
    }

    func handle(
        type: WireProtocol.FrameType,
        payload: Data
    ) async {
        guard sendFrame != nil else { return }
        do {
            switch type {
            case .bulkOffer:
                try await receiveOffer(payload)
            case .bulkDecision:
                try await receiveDecision(payload)
            case .bulkChunk:
                try await receiveChunk(payload)
            case .bulkControl:
                try await receiveControl(payload)
            case .bulkFinish:
                try await receiveFinish(payload)
            case .bulkCompletion:
                try receiveCompletion(payload)
            default:
                break
            }
        } catch {
            // Malformed transfer data fails the implicated
            // transfer when its ID could be decoded by the relevant handler.
            // Frames too malformed to identify are discarded without taking
            // down the message connection.
        }
    }

    private func receiveOffer(_ payload: Data) async throws {
        let offer = try decodeControl(BulkOfferEnvelope.self, from: payload)
        guard offer.version == BulkOfferEnvelope.currentVersion,
            offer.byteCount >= 0,
            outgoing[offer.id] == nil,
            incoming[offer.id] == nil,
            pendingIncoming[offer.id] == nil
        else {
            return
        }

        guard pendingIncoming.count + incoming.count < 32 else {
            try await sendDecision(id: offer.id, accepted: false)
            return
        }

        let receivedAt = Date()
        pendingIncoming[offer.id] = PendingIncomingState(
            metadata: offer.metadata,
            byteCount: offer.byteCount,
            receivedAt: receivedAt
        )
        let duration = offerTimeout
        let timeout = Task { [weak self] in
            try? await Task.sleep(for: duration)
            guard !Task.isCancelled else { return }
            await self?.incomingOfferTimedOut(offer.id)
        }
        pendingIncoming[offer.id]?.timeoutTask = timeout

        let responder = IncomingBulkTransferResponder { [weak self] decision in
            guard let self else { throw VMBridgeError.notRunning }
            return try await self.respond(to: offer.id, decision: decision)
        }
        offersMulticaster.yield(
            IncomingBulkTransferOffer(
                id: offer.id,
                metadata: offer.metadata,
                byteCount: offer.byteCount,
                receivedAt: receivedAt,
                responder: responder
            )
        )
    }

    private func respond(
        to id: UUID,
        decision: IncomingBulkTransferDecision
    ) async throws -> IncomingBulkTransferAcceptance? {
        guard sendFrame != nil else { throw VMBridgeError.disconnected }
        guard let pending = pendingIncoming.removeValue(forKey: id) else {
            throw VMBridgeError.bulkTransferOfferTimedOut
        }
        pending.timeoutTask?.cancel()

        if case .reject = decision {
            try await sendDecision(
                id: id,
                accepted: false
            )
            return nil
        }

        let operation = BulkTransferOperation(
            totalByteCount: pending.byteCount,
            initialPhase: .transferring
        )
        let sink: IncomingSink
        let destinationURL: URL?
        var stream: BulkByteStream?

        do {
            switch decision {
            case .file(let url, let overwrite):
                let fileSink = try Self.makeFileSink(
                    destinationURL: url,
                    overwrite: overwrite,
                    transferID: id
                )
                sink = .file(fileSink)
                destinationURL = url
            case .stream:
                let storage = BulkByteStreamStorage(
                    onConsumed: { [weak self] byteCount in
                        await self?.streamConsumed(
                            id: id,
                            byteCount: byteCount
                        )
                    },
                    onCancelled: { [weak self] in
                        await self?.cancel(id: id, direction: .incoming)
                    }
                )
                sink = .stream(storage)
                destinationURL = nil
                stream = BulkByteStream(storage: storage)
            case .reject:
                preconditionFailure("Reject handled before sink creation")
            }
        } catch {
            try? await sendDecision(
                id: id,
                accepted: false
            )
            throw error
        }

        incoming[id] = IncomingState(
            metadata: pending.metadata,
            byteCount: pending.byteCount,
            operation: operation,
            sink: sink
        )
        do {
            try await sendDecision(
                id: id,
                accepted: true
            )
        } catch {
            await failIncoming(id: id, error: error, notifyRemote: false)
            throw error
        }

        let transfer = IncomingBulkTransfer(
            id: id,
            metadata: pending.metadata,
            byteCount: pending.byteCount,
            destinationURL: destinationURL,
            operation: operation,
            controls: controls(for: id, direction: .incoming)
        )
        return IncomingBulkTransferAcceptance(
            transfer: transfer,
            bytes: stream
        )
    }

    private func receiveDecision(_ payload: Data) async throws {
        let decision = try decodeControl(
            BulkDecisionEnvelope.self,
            from: payload
        )
        guard decision.version == BulkDecisionEnvelope.currentVersion,
            var state = outgoing[decision.id],
            !state.isAccepted
        else {
            return
        }
        state.timeoutTask?.cancel()
        state.timeoutTask = nil

        guard decision.accepted else {
            outgoing[decision.id] = nil
            state.operation.finish(
                with: .failure(VMBridgeError.bulkTransferRejected)
            )
            return
        }

        state.isAccepted = true
        state.operation.yield(
            transferredByteCount: 0,
            totalByteCount: state.byteCount,
            phase: .transferring
        )
        outgoing[decision.id] = state
        let task = Task { [weak self] in
            guard let self else { return }
            await self.runOutgoing(id: decision.id)
        }
        outgoing[decision.id]?.task = task
    }

    private func runOutgoing(id: UUID) async {
        guard let state = outgoing[id] else { return }
        var hasher = SHA256()
        var producedByteCount: Int64 = 0

        do {
            switch state.source {
            case .file(let url):
                let handle = try FileHandle(forReadingFrom: url)
                defer { try? handle.close() }
                while let bytes = try handle.read(
                    upToCount: WireProtocol.maximumBulkChunkSize
                ), !bytes.isEmpty {
                    producedByteCount += Int64(bytes.count)
                    guard producedByteCount <= state.byteCount else {
                        throw VMBridgeError.bulkTransferLengthMismatch(
                            expected: state.byteCount,
                            actual: producedByteCount
                        )
                    }
                    hasher.update(data: bytes)
                    try await transmit(bytes, id: id)
                }

            case .stream(_, let chunks):
                for try await sourceChunk in chunks {
                    producedByteCount += Int64(sourceChunk.count)
                    guard producedByteCount <= state.byteCount else {
                        throw VMBridgeError.bulkTransferLengthMismatch(
                            expected: state.byteCount,
                            actual: producedByteCount
                        )
                    }
                    hasher.update(data: sourceChunk)
                    var offset = sourceChunk.startIndex
                    while offset < sourceChunk.endIndex {
                        let end = min(
                            sourceChunk.endIndex,
                            offset + WireProtocol.maximumBulkChunkSize
                        )
                        try await transmit(
                            Data(sourceChunk[offset..<end]),
                            id: id
                        )
                        offset = end
                    }
                }
            }

            guard let current = outgoing[id] else { return }
            guard producedByteCount == current.byteCount else {
                throw VMBridgeError.bulkTransferLengthMismatch(
                    expected: current.byteCount,
                    actual: producedByteCount
                )
            }
            let digest = Data(hasher.finalize())
            outgoing[id]?.digest = digest
            let finish = try encodeControl(
                BulkFinishEnvelope(
                    id: id,
                    byteCount: producedByteCount,
                    sha256Digest: digest
                )
            )
            try await send(
                .bulkFinish,
                finish,
                transferID: id
            )
        } catch is CancellationError {
            failOutgoing(
                id: id,
                error: VMBridgeError.bulkTransferCancelled
            )
        } catch {
            let wasActive = outgoing[id] != nil
            failOutgoing(id: id, error: error)
            if wasActive {
                try? await sendControl(
                    id: id,
                    action: .cancel
                )
            }
        }
    }

    private func transmit(_ data: Data, id: UUID) async throws {
        var offset = data.startIndex
        while offset < data.endIndex {
            let allowance = try await nextAllowance(
                id: id,
                requested: data.endIndex - offset
            )
            let end = offset + allowance
            guard let state = outgoing[id] else {
                throw VMBridgeError.bulkTransferCancelled
            }
            let frameOffset = state.sentByteCount
            let payload = try BulkChunkEnvelope.encode(
                id: id,
                offset: frameOffset,
                bytes: Data(data[offset..<end])
            )
            outgoing[id]?.sentByteCount += Int64(allowance)
            do {
                try await send(
                    .bulkChunk,
                    payload,
                    transferID: id
                )
            } catch {
                outgoing[id]?.sentByteCount -= Int64(allowance)
                throw error
            }
            offset = end
        }
    }

    private func nextAllowance(id: UUID, requested: Int) async throws -> Int {
        while true {
            try Task.checkCancellation()
            guard var state = outgoing[id] else {
                throw VMBridgeError.bulkTransferCancelled
            }
            let available =
                Self.receiveWindowByteCount
                - (state.sentByteCount - state.acknowledgedByteCount)
            if state.isAccepted,
                !state.isPausedLocally,
                !state.isPausedRemotely,
                available > 0
            {
                return min(requested, Int(available))
            }

            await withCheckedContinuation { continuation in
                state.signal = continuation
                outgoing[id] = state
            }
        }
    }

    private func receiveChunk(_ payload: Data) async throws {
        let chunk = try BulkChunkEnvelope.decode(payload)
        guard var state = incoming[chunk.id]
        else {
            return
        }
        guard chunk.offset == state.receivedByteCount else {
            await failIncoming(
                id: chunk.id,
                error: WireProtocol.WireError.malformedEnvelope,
                notifyRemote: true
            )
            return
        }
        let nextCount = state.receivedByteCount + Int64(chunk.bytes.count)
        guard nextCount - state.consumedByteCount <= Self.receiveWindowByteCount else {
            await failIncoming(
                id: chunk.id, error: VMBridgeError.malformedFrame, notifyRemote: true)
            return
        }
        guard nextCount <= state.byteCount else {
            await failIncoming(
                id: chunk.id,
                error: VMBridgeError.bulkTransferLengthMismatch(
                    expected: state.byteCount,
                    actual: nextCount
                ),
                status: .invalidLength,
                notifyRemote: true
            )
            return
        }

        do {
            switch state.sink {
            case .file(let sink):
                try sink.handle.write(contentsOf: chunk.bytes)
                state.consumedByteCount = nextCount
                state.hasher.update(data: chunk.bytes)
                state.receivedByteCount = nextCount
                incoming[chunk.id] = state
                state.operation.yield(
                    transferredByteCount: nextCount,
                    totalByteCount: state.byteCount,
                    phase: incomingPhase(state)
                )
                try await acknowledgeIncoming(id: chunk.id)
            case .stream(let storage):
                // Publish the received offset before waking a waiting stream
                // consumer. Its consumption callback may immediately re-enter
                // this actor and must observe the new contiguous range.
                state.hasher.update(data: chunk.bytes)
                state.receivedByteCount = nextCount
                incoming[chunk.id] = state
                await storage.append(chunk.bytes)
            }
        } catch {
            await failIncoming(id: chunk.id, error: error, notifyRemote: true)
        }
    }

    private func streamConsumed(id: UUID, byteCount: Int) async {
        guard var state = incoming[id],
            case .stream = state.sink
        else {
            return
        }
        state.consumedByteCount = min(
            state.receivedByteCount,
            state.consumedByteCount + Int64(byteCount)
        )
        incoming[id] = state
        state.operation.yield(
            transferredByteCount: state.consumedByteCount,
            totalByteCount: state.byteCount,
            phase: incomingPhase(state)
        )
        try? await acknowledgeIncoming(id: id)
        if state.finishedDigest != nil,
            state.consumedByteCount == state.byteCount
        {
            await completeIncomingStream(id: id)
        }
    }

    private func acknowledgeIncoming(id: UUID) async throws {
        guard let state = incoming[id], !state.isPausedLocally else { return }
        try await sendControl(
            id: id,
            action: .acknowledgement,
            acknowledgedOffset: state.consumedByteCount
        )
    }

    private func receiveControl(_ payload: Data) async throws {
        let control = try decodeControl(
            BulkControlEnvelope.self,
            from: payload
        )
        guard control.version == BulkControlEnvelope.currentVersion else {
            return
        }

        if var state = outgoing[control.id] {
            switch control.action {
            case .acknowledgement:
                guard let offset = control.acknowledgedOffset,
                    offset >= state.acknowledgedByteCount,
                    offset <= state.sentByteCount
                else {
                    return
                }
                state.acknowledgedByteCount = offset
                state.operation.yield(
                    transferredByteCount: offset,
                    totalByteCount: state.byteCount,
                    phase: outgoingPhase(state)
                )
            case .pause:
                state.isPausedRemotely = true
                state.operation.yield(
                    transferredByteCount: state.acknowledgedByteCount,
                    totalByteCount: state.byteCount,
                    phase: .pausedByRemote
                )
            case .resume:
                state.isPausedRemotely = false
                state.operation.yield(
                    transferredByteCount: state.acknowledgedByteCount,
                    totalByteCount: state.byteCount,
                    phase: outgoingPhase(state)
                )
            case .cancel:
                outgoing[control.id] = state
                failOutgoing(
                    id: control.id,
                    error: VMBridgeError.bulkTransferCancelled
                )
                return
            }
            let signal = state.signal
            state.signal = nil
            outgoing[control.id] = state
            signal?.resume()
            return
        }

        if var state = incoming[control.id] {
            switch control.action {
            case .pause:
                state.isPausedRemotely = true
            case .resume:
                state.isPausedRemotely = false
            case .cancel:
                incoming[control.id] = state
                await failIncoming(
                    id: control.id,
                    error: VMBridgeError.bulkTransferCancelled,
                    notifyRemote: false
                )
                return
            case .acknowledgement:
                return
            }
            state.operation.yield(
                transferredByteCount: state.consumedByteCount,
                totalByteCount: state.byteCount,
                phase: incomingPhase(state)
            )
            incoming[control.id] = state
        }
    }

    private func receiveFinish(_ payload: Data) async throws {
        let finish = try decodeControl(
            BulkFinishEnvelope.self,
            from: payload
        )
        guard finish.version == BulkFinishEnvelope.currentVersion,
            var state = incoming[finish.id]
        else {
            return
        }

        guard finish.byteCount == state.byteCount,
            state.receivedByteCount == state.byteCount
        else {
            await failIncoming(
                id: finish.id,
                error: VMBridgeError.bulkTransferLengthMismatch(
                    expected: state.byteCount,
                    actual: state.receivedByteCount
                ),
                status: .invalidLength,
                notifyRemote: true
            )
            return
        }

        let digest = Data(state.hasher.finalize())
        guard digest == finish.sha256Digest else {
            await failIncoming(
                id: finish.id,
                error: VMBridgeError.bulkTransferIntegrityFailure,
                status: .integrityFailure,
                notifyRemote: true
            )
            return
        }
        state.finishedDigest = digest
        incoming[finish.id] = state

        switch state.sink {
        case .file:
            await completeIncomingFile(id: finish.id)
        case .stream(let storage):
            await storage.finish()
            if state.consumedByteCount == state.byteCount {
                await completeIncomingStream(id: finish.id)
            }
        }
    }

    private func completeIncomingFile(id: UUID) async {
        guard let state = incoming[id],
            let digest = state.finishedDigest,
            case .file(let sink) = state.sink
        else {
            return
        }
        do {
            try sink.handle.close()
            let fileManager = FileManager.default
            if fileManager.fileExists(atPath: sink.destinationURL.path) {
                guard sink.overwrite else {
                    throw VMBridgeError.bulkTransferDestinationExists(
                        sink.destinationURL
                    )
                }
                _ = try fileManager.replaceItemAt(
                    sink.destinationURL,
                    withItemAt: sink.temporaryURL
                )
            } else {
                try fileManager.moveItem(
                    at: sink.temporaryURL,
                    to: sink.destinationURL
                )
            }
            try await completeIncoming(
                id: id,
                state: state,
                digest: digest
            )
        } catch {
            await failIncoming(id: id, error: error, notifyRemote: true)
        }
    }

    private func completeIncomingStream(id: UUID) async {
        guard let state = incoming[id],
            let digest = state.finishedDigest
        else {
            return
        }
        do {
            try await completeIncoming(id: id, state: state, digest: digest)
        } catch {
            await failIncoming(id: id, error: error, notifyRemote: false)
        }
    }

    private func completeIncoming(
        id: UUID,
        state: IncomingState,
        digest: Data
    ) async throws {
        try await sendCompletion(id: id, status: .success)
        incoming[id] = nil
        state.operation.yield(
            transferredByteCount: state.byteCount,
            totalByteCount: state.byteCount,
            phase: .completed
        )
        state.operation.finish(
            with: .success(
                BulkTransferReceipt(
                    id: id,
                    byteCount: state.byteCount,
                    sha256Digest: digest
                )
            )
        )
    }

    private func receiveCompletion(_ payload: Data) throws {
        let completion = try decodeControl(
            BulkCompletionEnvelope.self,
            from: payload
        )
        guard completion.version == BulkCompletionEnvelope.currentVersion,
            let state = outgoing[completion.id]
        else {
            return
        }
        outgoing[completion.id] = nil
        state.timeoutTask?.cancel()
        state.task?.cancel()
        state.signal?.resume()

        switch completion.status {
        case .success:
            guard let digest = state.digest else {
                state.operation.finish(
                    with: .failure(
                        VMBridgeError.bulkTransferIntegrityFailure
                    )
                )
                return
            }
            state.operation.yield(
                transferredByteCount: state.byteCount,
                totalByteCount: state.byteCount,
                phase: .completed
            )
            state.operation.finish(
                with: .success(
                    BulkTransferReceipt(
                        id: completion.id,
                        byteCount: state.byteCount,
                        sha256Digest: digest
                    )
                )
            )
        case .rejected:
            state.operation.finish(
                with: .failure(VMBridgeError.bulkTransferRejected)
            )
        case .cancelled:
            state.operation.finish(
                with: .failure(VMBridgeError.bulkTransferCancelled)
            )
        case .invalidLength:
            state.operation.finish(
                with: .failure(
                    VMBridgeError.bulkTransferLengthMismatch(
                        expected: state.byteCount,
                        actual: state.sentByteCount
                    )
                )
            )
        case .integrityFailure:
            state.operation.finish(
                with: .failure(
                    VMBridgeError.bulkTransferIntegrityFailure
                )
            )
        case .failed:
            state.operation.finish(
                with: .failure(VMBridgeError.bulkTransferCancelled)
            )
        }
    }

    private func pause(id: UUID, direction: Direction) async {
        switch direction {
        case .outgoing:
            guard var state = outgoing[id], !state.isPausedLocally else {
                return
            }
            state.isPausedLocally = true
            state.operation.yield(
                transferredByteCount: state.acknowledgedByteCount,
                totalByteCount: state.byteCount,
                phase: .pausedLocally
            )
            outgoing[id] = state
            try? await sendControl(id: id, action: .pause)
        case .incoming:
            guard var state = incoming[id], !state.isPausedLocally else {
                return
            }
            state.isPausedLocally = true
            state.operation.yield(
                transferredByteCount: state.consumedByteCount,
                totalByteCount: state.byteCount,
                phase: .pausedLocally
            )
            incoming[id] = state
            try? await sendControl(id: id, action: .pause)
        }
    }

    private func resume(id: UUID, direction: Direction) async {
        switch direction {
        case .outgoing:
            guard var state = outgoing[id], state.isPausedLocally else {
                return
            }
            state.isPausedLocally = false
            state.operation.yield(
                transferredByteCount: state.acknowledgedByteCount,
                totalByteCount: state.byteCount,
                phase: outgoingPhase(state)
            )
            let signal = state.signal
            state.signal = nil
            outgoing[id] = state
            signal?.resume()
            try? await sendControl(id: id, action: .resume)
        case .incoming:
            guard var state = incoming[id], state.isPausedLocally else {
                return
            }
            state.isPausedLocally = false
            state.operation.yield(
                transferredByteCount: state.consumedByteCount,
                totalByteCount: state.byteCount,
                phase: incomingPhase(state)
            )
            incoming[id] = state
            try? await sendControl(id: id, action: .resume)
            try? await acknowledgeIncoming(id: id)
        }
    }

    private func cancel(id: UUID, direction: Direction) async {
        switch direction {
        case .outgoing:
            guard outgoing[id] != nil else { return }
            failOutgoing(
                id: id,
                error: VMBridgeError.bulkTransferCancelled
            )
            try? await sendControl(id: id, action: .cancel)
        case .incoming:
            guard incoming[id] != nil else { return }
            await failIncoming(
                id: id,
                error: VMBridgeError.bulkTransferCancelled,
                notifyRemote: false
            )
            try? await sendControl(id: id, action: .cancel)
        }
    }

    private func outgoingOfferTimedOut(_ id: UUID) {
        guard let state = outgoing[id], !state.isAccepted else { return }
        failOutgoing(
            id: id,
            error: VMBridgeError.bulkTransferOfferTimedOut
        )
    }

    private func incomingOfferTimedOut(_ id: UUID) async {
        guard pendingIncoming.removeValue(forKey: id) != nil else {
            return
        }
        try? await sendDecision(id: id, accepted: false)
    }

    private func failOutgoing(id: UUID, error: any Error) {
        guard let state = outgoing.removeValue(forKey: id) else { return }
        state.timeoutTask?.cancel()
        state.task?.cancel()
        state.signal?.resume()
        state.operation.finish(with: .failure(error))
    }

    private func failIncoming(
        id: UUID,
        error: any Error,
        status: BulkCompletionEnvelope.Status = .failed,
        notifyRemote: Bool
    ) async {
        guard let state = incoming.removeValue(forKey: id) else { return }
        switch state.sink {
        case .file(let sink):
            try? sink.handle.close()
            try? FileManager.default.removeItem(at: sink.temporaryURL)
        case .stream(let storage):
            await storage.finish(throwing: error)
        }
        state.operation.finish(with: .failure(error))
        if notifyRemote {
            try? await sendCompletion(
                id: id,
                status: status
            )
        }
    }

    private func controls(
        for id: UUID,
        direction: Direction
    ) -> BulkTransferControls {
        BulkTransferControls(
            pause: { [weak self] in
                await self?.pause(id: id, direction: direction)
            },
            resume: { [weak self] in
                await self?.resume(id: id, direction: direction)
            },
            cancel: { [weak self] in
                await self?.cancel(id: id, direction: direction)
            }
        )
    }

    private func sendDecision(
        id: UUID,
        accepted: Bool
    ) async throws {
        try await send(
            .bulkDecision,
            try encodeControl(
                BulkDecisionEnvelope(id: id, accepted: accepted)
            ),
            transferID: id
        )
    }

    private func sendControl(
        id: UUID,
        action: BulkControlEnvelope.Action,
        acknowledgedOffset: Int64? = nil
    ) async throws {
        try await send(
            .bulkControl,
            try encodeControl(
                BulkControlEnvelope(
                    id: id,
                    action: action,
                    acknowledgedOffset: acknowledgedOffset
                )
            ),
            transferID: id
        )
    }

    private func sendCompletion(
        id: UUID,
        status: BulkCompletionEnvelope.Status
    ) async throws {
        try await send(
            .bulkCompletion,
            try encodeControl(
                BulkCompletionEnvelope(id: id, status: status)
            ),
            transferID: id
        )
    }

    private func send(
        _ type: WireProtocol.FrameType,
        _ payload: Data,
        transferID: UUID
    ) async throws {
        guard let sendFrame else { throw VMBridgeError.notRunning }
        try await sendFrame(type, payload, transferID)
    }

    private func encodeControl<Value: Encodable>(
        _ value: Value
    ) throws -> Data {
        let data = try JSONEncoder().encode(value)
        guard data.count <= WireProtocol.maximumBulkControlPayload else {
            throw VMBridgeError.bulkTransferMetadataTooLarge(
                byteCount: data.count,
                limit: WireProtocol.maximumBulkControlPayload
            )
        }
        return data
    }

    private func decodeControl<Value: Decodable>(
        _ type: Value.Type,
        from data: Data
    ) throws -> Value {
        guard data.count <= WireProtocol.maximumBulkControlPayload else {
            throw WireProtocol.WireError.framePayloadTooLarge(data.count)
        }
        return try JSONDecoder().decode(type, from: data)
    }

    private func outgoingPhase(
        _ state: OutgoingState
    ) -> BulkTransferPhase {
        if state.isPausedLocally { return .pausedLocally }
        if state.isPausedRemotely { return .pausedByRemote }
        return state.isAccepted ? .transferring : .waitingForAcceptance
    }

    private func incomingPhase(
        _ state: IncomingState
    ) -> BulkTransferPhase {
        if state.isPausedLocally { return .pausedLocally }
        if state.isPausedRemotely { return .pausedByRemote }
        return .transferring
    }

    private static func byteCount(
        for source: BulkTransferSource
    ) throws -> Int64 {
        switch source {
        case .file(let url):
            let values = try url.resourceValues(
                forKeys: [.fileSizeKey, .isRegularFileKey]
            )
            guard values.isRegularFile == true, let size = values.fileSize else {
                throw CocoaError(.fileReadUnsupportedScheme)
            }
            return Int64(size)
        case .stream(let byteCount, _):
            return byteCount
        }
    }

    private static func makeFileSink(
        destinationURL: URL,
        overwrite: Bool,
        transferID: UUID
    ) throws -> FileSink {
        let fileManager = FileManager.default
        if !overwrite,
            fileManager.fileExists(atPath: destinationURL.path)
        {
            throw VMBridgeError.bulkTransferDestinationExists(destinationURL)
        }
        let temporaryURL =
            destinationURL
            .deletingLastPathComponent()
            .appendingPathComponent(
                ".\(destinationURL.lastPathComponent).\(transferID.uuidString).partial"
            )
        guard
            fileManager.createFile(
                atPath: temporaryURL.path,
                contents: nil
            )
        else {
            throw CocoaError(.fileWriteUnknown)
        }
        do {
            return FileSink(
                destinationURL: destinationURL,
                temporaryURL: temporaryURL,
                overwrite: overwrite,
                handle: try FileHandle(forWritingTo: temporaryURL)
            )
        } catch {
            try? fileManager.removeItem(at: temporaryURL)
            throw error
        }
    }
}
