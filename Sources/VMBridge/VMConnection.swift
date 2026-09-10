import Foundation

/// One host–guest pair. Subscribe before run(); cancellation owns teardown.
/// Subscriptions survive reconnections, but requests and transfers do not.
public actor VMConnection {
    public static let maximumMessageSize = 8 * 1024 * 1024
    public nonisolated let configuration: VMConfiguration

    private enum Source: Sendable {
        case guest(@Sendable () async throws -> SocketLink)
        case host(any VMHostSource)
    }
    private final class Session: Sendable {
        let id = UUID()
        let link: VMTransportLink
        let reader: FrameReader
        let writer: FrameWriter
        let bulk: BulkTransferManager
        init(link: VMTransportLink, offers: Multicaster<IncomingBulkTransferOffer>) {
            self.link = link
            reader = FrameReader(socket: link.socket)
            writer = FrameWriter(socket: link.socket)
            bulk = BulkTransferManager(offers: offers)
        }
    }
    private struct PendingRequest {
        let expectedID: String
        let continuation: CheckedContinuation<Data, any Error>
        let timeout: Task<Void, Never>
        let send: Task<Void, Never>
    }

    private let source: Source
    private let port: UInt32
    private nonisolated let router: MessageRouter
    private nonisolated let states = Multicaster<VMConnectionState>(
        bufferingPolicy: .bufferingNewest(1), replayingLatest: .stopped)
    private nonisolated let offers = Multicaster<IncomingBulkTransferOffer>(
        bufferingPolicy: .bufferingNewest(32))
    private var runID: UUID?
    private var active: Session?
    private var requests: [UUID: PendingRequest] = [:]
    public private(set) var currentState: VMConnectionState = .stopped

    private init(source: Source, port: UInt32, configuration: VMConfiguration) {
        self.source = source
        self.port = port
        self.configuration = configuration
        router = MessageRouter(
            undeliveredMessageRetention: configuration.undeliveredMessageRetention)
    }

    /// Creates an idle guest connection. While running, retries transient socket
    /// failures automatically. The port must match the host's reserved port.
    public nonisolated static func guest(port: UInt32, configuration: VMConfiguration = .init())
        -> VMConnection
    {
        VMConnection(
            source: .guest { try await SocketLink.connect(port: port) }, port: port,
            configuration: configuration)
    }

    package init(hostSource: any VMHostSource, port: UInt32, configuration: VMConfiguration) {
        source = .host(hostSource)
        self.port = port
        self.configuration = configuration
        router = MessageRouter(
            undeliveredMessageRetention: configuration.undeliveredMessageRetention)
    }

    // Deterministic socket-pair and retry injection for this package's tests.
    init(
        connector: @escaping @Sendable () async throws -> SocketLink,
        configuration: VMConfiguration = .init()
    ) {
        source = .guest(connector)
        port = 1
        self.configuration = configuration
        router = MessageRouter(
            undeliveredMessageRetention: configuration.undeliveredMessageRetention)
    }

    public nonisolated var state: AsyncStream<VMConnectionState> { states.makeStream() }
    public nonisolated var bulkTransferOffers: AsyncStream<IncomingBulkTransferOffer> {
        offers.makeStream()
    }
    public nonisolated func messages<M: VMMessage>(of type: M.Type) -> AsyncStream<
        ReceivedMessage<M>
    > { router.stream(for: type) }
    public nonisolated func requests<M: VMMessage>(of type: M.Type) -> AsyncStream<
        ReceivedRequest<M>
    > { router.requestStream(for: type) }

    /// Runs until cancellation or a permanent setup/protocol failure. Ordinary
    /// disconnects keep the listener or guest retry loop alive.
    public func run() async throws {
        guard runID == nil else { throw VMBridgeError.alreadyRunning }
        guard port > 0, port < UInt32.max else { throw VMBridgeError.invalidPort }
        let id = UUID()
        runID = id
        setState(.connecting)
        var failure: (any Error)?
        do {
            try Task.checkCancellation()
            switch source {
            case .guest(let connect): try await runGuest(connect, run: id)
            case .host(let source): try await runHost(source, run: id)
            }
        } catch {
            if !Task.isCancelled { failure = error }
        }
        await disconnect(active, error: .notRunning)
        if case .host(let source) = source { await source.stop() }
        runID = nil
        setState(.stopped)
        if let failure { throw failure }
    }

    private func runGuest(_ connect: @escaping @Sendable () async throws -> SocketLink, run: UUID)
        async throws
    {
        var delay = 0.5
        while !Task.isCancelled {
            setState(.connecting)
            do {
                let socket = try await withHandshakeTimeout(connect)
                let established = try await serve(
                    VMTransportLink(socket: socket), isHost: false, run: run)
                if established { delay = 0.5 }
            } catch {
                if Task.isCancelled { break }
                if let error = error as? VMBridgeError, error == .incompatibleProtocol {
                    throw error
                }
                if error is DecodingError || error is WireProtocol.WireError {
                    throw VMBridgeError.incompatibleProtocol
                }
                if let error = error as? POSIXError,
                    [.EAFNOSUPPORT, .EPROTONOSUPPORT, .EACCES, .EPERM].contains(error.code)
                {
                    throw error
                }
                VMBridgeLog.connection.debug(
                    "Guest connection attempt failed: \(String(describing: error), privacy: .public)"
                )
            }
            guard !Task.isCancelled else { break }
            setState(.disconnected)
            try await Task.sleep(for: .seconds(delay))
            delay = min(5, delay * 2)
        }
    }

    private func runHost(_ source: any VMHostSource, run: UUID) async throws {
        let links = try await source.start()
        enum Event: Sendable {
            case accepted(VMTransportLink?)
            case finished
        }
        try await withThrowingTaskGroup(of: Event.self) { group in
            func nextLink() async throws -> Event {
                var iterator = links.makeAsyncIterator()
                return .accepted(try await iterator.next())
            }
            group.addTask { try await nextLink() }
            var candidates = 0
            while let event = try await group.next() {
                switch event {
                case .accepted(let link):
                    guard let link else {
                        group.cancelAll()
                        continue
                    }
                    if Task.isCancelled || candidates >= 5 {
                        await link.close()
                    } else {
                        candidates += 1
                        group.addTask {
                            do { _ = try await self.serve(link, isHost: true, run: run) } catch {
                                VMBridgeLog.connection.debug(
                                    "Host connection ended: \(String(describing: error), privacy: .public)"
                                )
                            }
                            return .finished
                        }
                    }
                    if !Task.isCancelled {
                        group.addTask { try await nextLink() }
                    } else {
                        group.cancelAll()
                    }
                case .finished:
                    candidates -= 1
                }
            }
        }
    }

    private func serve(_ link: VMTransportLink, isHost: Bool, run: UUID) async throws -> Bool {
        let session = Session(link: link, offers: offers)
        var established = false
        var failure: (any Error)?
        await withTaskCancellationHandler {
            do {
                try await withHandshakeTimeout {
                    try await withTaskCancellationHandler {
                        if isHost {
                            guard let hello = try await session.reader.next(), hello.type == .hello
                            else { throw VMBridgeError.incompatibleProtocol }
                            try JSONDecoder().decode(Handshake.self, from: hello.payload).validate(
                                expectedRole: "guest")
                            try await session.writer.send(
                                .accept, JSONEncoder().encode(Handshake(role: "host")))
                        } else {
                            try await session.writer.send(
                                .hello, JSONEncoder().encode(Handshake(role: "guest")))
                            guard let accept = try await session.reader.next(),
                                accept.type == .accept
                            else { throw VMBridgeError.incompatibleProtocol }
                            try JSONDecoder().decode(Handshake.self, from: accept.payload).validate(
                                expectedRole: "host")
                        }
                    } onCancel: {
                        link.socket.cancel()
                    }
                }
                try Task.checkCancellation()
                await session.bulk.activate { type, payload, transferID in
                    if type.isBulkControl {
                        try await session.writer.queueControl(type, payload)
                    } else {
                        try await session.writer.send(type, payload, transferID: transferID)
                    }
                }
                guard runID == run, !Task.isCancelled else { throw CancellationError() }
                let previous = active
                if previous != nil { setState(.disconnected) }
                active = session
                router.clearPending()
                failRequests(with: .disconnected)
                setState(.connected)
                established = true
                previous?.link.socket.cancel()
                if let previous { await previous.bulk.deactivate(with: .disconnected) }
                while let frame = try await session.reader.next() {
                    try Task.checkCancellation()
                    guard active?.id == session.id else { break }
                    try await handle(frame, session: session)
                }
            } catch { failure = error }
            await disconnect(session, error: .disconnected)
            await session.writer.stop()
            await session.bulk.deactivate(with: .disconnected)
            await link.close()
        } onCancel: {
            link.socket.cancel()
        }
        if let failure, !established { throw failure }
        return established
    }

    private func disconnect(_ session: Session?, error: VMBridgeError) async {
        guard let session, active?.id == session.id else { return }
        active = nil
        router.clearPending()
        failRequests(with: error)
        setState(.disconnected)
        session.link.socket.cancel()
        await session.writer.stop()
        await session.bulk.deactivate(with: error)
    }

    private func setState(_ state: VMConnectionState) {
        guard currentState != state else { return }
        currentState = state
        states.yield(state)
    }

    private func connectedSession() throws -> Session {
        guard runID != nil else { throw VMBridgeError.notRunning }
        guard let active else { throw VMBridgeError.disconnected }
        return active
    }

    private nonisolated func encode<M: VMMessage>(_ message: M) throws -> Data {
        let body = try JSONEncoder().encode(message)
        guard body.count <= Self.maximumMessageSize else {
            throw VMBridgeError.payloadTooLarge(
                byteCount: body.count, limit: Self.maximumMessageSize)
        }
        return body
    }

    public func send<M: VMMessage>(_ message: M) async throws {
        let session = try connectedSession()
        try await session.writer.send(
            .data, MessageEnvelope.encode(messageID: M.messageID, body: encode(message)))
    }

    public func send<Request: VMMessage, Reply: VMMessage>(
        _ request: Request, expecting reply: Reply.Type, timeout: Duration = .seconds(10)
    ) async throws -> Reply {
        try Task.checkCancellation()
        let session = try connectedSession()
        let id = UUID()
        let envelope = try RequestEnvelope.encode(
            correlationID: id, messageID: Request.messageID, body: encode(request))
        let data: Data = try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation { continuation in
                let watchdog = Task {
                    do { try await Task.sleep(for: timeout) } catch { return }
                    self.finishRequest(id, result: .failure(VMBridgeError.requestTimedOut))
                }
                let send = Task {
                    do { try await session.writer.send(.request, envelope) } catch {
                        self.finishRequest(id, result: .failure(error))
                    }
                }
                requests[id] = PendingRequest(
                    expectedID: Reply.messageID, continuation: continuation, timeout: watchdog,
                    send: send)
                if Task.isCancelled { finishRequest(id, result: .failure(CancellationError())) }
            }
        } onCancel: {
            Task { await self.finishRequest(id, result: .failure(CancellationError())) }
        }
        return try JSONDecoder().decode(Reply.self, from: data)
    }

    private func finishRequest(_ id: UUID, result: Result<Data, any Error>) {
        guard let pending = requests.removeValue(forKey: id) else { return }
        pending.timeout.cancel()
        pending.send.cancel()
        pending.continuation.resume(with: result)
    }

    private func failRequests(with error: VMBridgeError) {
        for id in Array(requests.keys) { finishRequest(id, result: .failure(error)) }
    }

    private func handle(_ frame: WireProtocol.RawFrame, session: Session) async throws {
        switch frame.type {
        case .data:
            let envelope = try MessageEnvelope.decode(frame.payload)
            try validateBody(envelope.body)
            router.deliver(messageID: envelope.messageID, body: envelope.body)
        case .request:
            let envelope = try RequestEnvelope.decode(frame.payload)
            try validateBody(envelope.body)
            router.deliverRequest(
                messageID: envelope.messageID, body: envelope.body,
                correlationID: envelope.correlationID
            ) { [weak self] id, body, messageID in
                guard let self else { throw VMBridgeError.notRunning }
                try await self.reply(
                    id: id, body: body, messageID: messageID, sessionID: session.id)
            }
        case .response:
            let envelope = try ResponseEnvelope.decode(frame.payload)
            try validateBody(envelope.body)
            guard envelope.status == ResponseEnvelope.successStatus else {
                throw VMBridgeError.malformedFrame
            }
            guard let pending = requests[envelope.correlationID] else { return }
            if pending.expectedID != envelope.messageID {
                finishRequest(
                    envelope.correlationID,
                    result: .failure(
                        VMBridgeError.unexpectedReplyType(
                            expected: pending.expectedID, actual: envelope.messageID)))
            } else {
                finishRequest(envelope.correlationID, result: .success(envelope.body))
            }
        case .bulkOffer, .bulkDecision, .bulkChunk, .bulkControl, .bulkFinish, .bulkCompletion:
            await session.bulk.handle(type: frame.type!, payload: frame.payload)
        default: throw VMBridgeError.malformedFrame
        }
    }

    private func validateBody(_ body: Data) throws {
        guard body.count <= Self.maximumMessageSize else { throw VMBridgeError.malformedFrame }
    }

    private func reply(id: UUID, body: Data, messageID: String, sessionID: UUID) async throws {
        guard let session = active, session.id == sessionID else {
            throw VMBridgeError.disconnected
        }
        try await session.writer.send(
            .response,
            ResponseEnvelope.encode(
                correlationID: id, status: ResponseEnvelope.successStatus, messageID: messageID,
                body: body))
    }

    public func sendFile(at url: URL, metadata: BulkTransferMetadata) async throws
        -> OutgoingBulkTransfer
    {
        try await sendBulkTransfer(.file(url), metadata: metadata)
    }

    public func sendBulkTransfer(_ source: BulkTransferSource, metadata: BulkTransferMetadata)
        async throws -> OutgoingBulkTransfer
    {
        let session = try connectedSession()
        return try await session.bulk.start(source: source, metadata: metadata)
    }
}
