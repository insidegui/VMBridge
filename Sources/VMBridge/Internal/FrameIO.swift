import Foundation

struct Handshake: Codable, Sendable {
    let protocolName: String
    let version: Int
    let role: String

    init(role: String) {
        protocolName = "VMBridge"
        version = 1
        self.role = role
    }

    func validate(expectedRole: String) throws {
        guard protocolName == "VMBridge", version == 1, role == expectedRole else {
            throw VMBridgeError.incompatibleProtocol
        }
    }
}

actor FrameReader {
    private let socket: SocketLink
    private var parser = WireProtocol.FrameParser()
    init(socket: SocketLink) { self.socket = socket }

    func next() async throws -> WireProtocol.RawFrame? {
        while true {
            if let frame = try parser.next() { return frame }
            guard let bytes = try await socket.read() else {
                guard parser.isEmpty else { throw VMBridgeError.malformedFrame }
                return nil
            }
            parser.append(bytes)
        }
    }
}

/// One serialized writer with priority for control traffic, alternating
/// ordinary messages and round-robin transfer chunks. Awaiting send applies
/// backpressure to each transfer producer.
actor FrameWriter {
    private struct Write {
        let id: UUID
        let type: WireProtocol.FrameType
        let payload: Data
        let continuation: CheckedContinuation<Void, any Error>?
    }
    private let socket: SocketLink
    private var controls: [Write] = []
    private var ordinary: [Write] = []
    private var bulk: [UUID: [Write]] = [:]
    private var order: [UUID] = []
    private var preferBulk = false
    private var draining = false
    private var stopped = false

    init(socket: SocketLink) { self.socket = socket }

    func send(_ type: WireProtocol.FrameType, _ payload: Data, transferID: UUID? = nil) async throws
    {
        try Task.checkCancellation()
        guard !stopped else { throw VMBridgeError.disconnected }
        guard payload.count <= WireProtocol.maximumPayload(for: type.rawValue) else {
            throw VMBridgeError.payloadTooLarge(
                byteCount: payload.count, limit: WireProtocol.maximumPayload(for: type.rawValue))
        }
        let id = UUID()
        try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation {
                (continuation: CheckedContinuation<Void, any Error>) in
                let write = Write(id: id, type: type, payload: payload, continuation: continuation)
                if let transferID, type == .bulkChunk {
                    if bulk[transferID] == nil { order.append(transferID) }
                    bulk[transferID, default: []].append(write)
                } else if type.isBulkControl || type == .hello || type == .accept {
                    controls.append(write)
                } else {
                    ordinary.append(write)
                }
                if !draining {
                    draining = true
                    Task { await self.drain() }
                }
            }
        } onCancel: {
            Task { await self.cancelQueued(id) }
        }
    }

    /// Control responses must not block the read loop behind a full socket:
    /// both sides may be transferring simultaneously. Bound this queue and let
    /// write failure close the session, which fails its dependent operations.
    func queueControl(_ type: WireProtocol.FrameType, _ payload: Data) throws {
        guard !stopped else { throw VMBridgeError.disconnected }
        guard payload.count <= WireProtocol.maximumPayload(for: type.rawValue),
            controls.count < 1024,
            controls.reduce(payload.count, { $0 + $1.payload.count }) <= 1024 * 1024
        else {
            stop()
            throw VMBridgeError.outboundQueueFull
        }
        controls.append(Write(id: UUID(), type: type, payload: payload, continuation: nil))
        if !draining {
            draining = true
            Task { await self.drain() }
        }
    }

    private func cancelQueued(_ id: UUID) {
        if let index = ordinary.firstIndex(where: { $0.id == id }) {
            ordinary.remove(at: index).continuation?.resume(throwing: CancellationError())
        } else if let index = controls.firstIndex(where: { $0.id == id }) {
            controls.remove(at: index).continuation?.resume(throwing: CancellationError())
        } else {
            for key in order {
                if let index = bulk[key]?.firstIndex(where: { $0.id == id }) {
                    bulk[key]?.remove(at: index).continuation?.resume(throwing: CancellationError())
                    break
                }
            }
        }
        // An in-flight frame must finish to preserve framing. Its caller may
        // abandon a request independently while the writer completes the frame.
    }

    func stop() {
        guard !stopped else { return }
        stopped = true
        socket.cancel()
        let writes = controls + ordinary + bulk.values.flatMap { $0 }
        controls.removeAll()
        ordinary.removeAll()
        bulk.removeAll()
        order.removeAll()
        for write in writes { write.continuation?.resume(throwing: VMBridgeError.disconnected) }
    }

    private func next() -> Write? {
        if !controls.isEmpty { return controls.removeFirst() }
        if !ordinary.isEmpty {
            preferBulk.toggle()
            if order.isEmpty || !preferBulk { return ordinary.removeFirst() }
        }
        while !order.isEmpty {
            let id = order.removeFirst()
            guard var writes = bulk.removeValue(forKey: id), !writes.isEmpty else { continue }
            let next = writes.removeFirst()
            if !writes.isEmpty {
                bulk[id] = writes
                order.append(id)
            }
            return next
        }
        return ordinary.isEmpty ? nil : ordinary.removeFirst()
    }

    private func drain() async {
        while !stopped, let write = next() {
            do {
                try await socket.write(
                    WireProtocol.encodeFrame(type: write.type, payload: write.payload))
                write.continuation?.resume()
            } catch {
                write.continuation?.resume(throwing: error)
                stop()
            }
        }
        draining = false
    }
}

func withHandshakeTimeout<T: Sendable>(
    _ operation: @escaping @Sendable () async throws -> T,
    timeout: Duration = .seconds(10)
) async throws -> T {
    try await withThrowingTaskGroup(of: T.self) { group in
        group.addTask { try await operation() }
        group.addTask {
            try await Task.sleep(for: timeout)
            throw VMBridgeError.handshakeTimedOut
        }
        defer { group.cancelAll() }
        return try await group.next()!
    }
}

package struct VMTransportLink: Sendable {
    package let socket: SocketLink
    package let release: @Sendable () async -> Void

    package init(socket: SocketLink, release: @escaping @Sendable () async -> Void = {}) {
        self.socket = socket
        self.release = release
    }

    package func close() async {
        await socket.close()
        await release()
    }
}

/// Package-only adapter seam. VMBridge exposes no custom transport API.
package protocol VMHostSource: Sendable {
    func start() async throws -> AsyncThrowingStream<VMTransportLink, any Error>
    func stop() async
}
