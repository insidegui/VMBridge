import Foundation
import os

/// Routes incoming message envelopes to typed subscriber streams.
///
/// All routing state is per-instance: there are no global registries. Routes
/// are keyed by `messageID`; each distinct payload type decodes an incoming
/// body exactly once, then fans the value out to all of its subscribers via a
/// ``Multicaster``.
///
/// Messages that arrive while their type has no live subscriber are kept in
/// a bounded pending buffer for a configurable retention period and handed
/// to the next subscriber of that type (consume-once). Delivery and
/// subscribe-plus-drain are both fully serialized under this type's lock, so
/// a message racing a subscription is either delivered live or buffered and
/// drained — never missed, never duplicated. Continuation yields are
/// non-blocking and ``Multicaster`` never calls back into the router, so the
/// router → multicaster lock nesting cannot deadlock.
final class MessageRouter: Sendable {

    private static let maximumPendingTypes = 64
    private static let maximumPendingPerType = 64
    private static let maximumPendingBytes = 512 * 1024

    private struct PendingMessage {
        let body: Data

        let receivedAt: Date
    }

    private struct PendingRequest {
        let body: Data
        let receivedAt: Date
        let responder: RequestResponder
    }

    private struct State {
        var routes: [String: [ObjectIdentifier: any MessageRoute]] = [:]
        var pending: [String: [PendingMessage]] = [:]
        var pendingBytes = 0
        var requestRoutes: [String: [ObjectIdentifier: any RequestRouteBox]] = [:]
        var pendingRequests: [String: [PendingRequest]] = [:]
        var pendingRequestBytes = 0
    }

    private let retentionInterval: TimeInterval
    private let state = OSAllocatedUnfairLock(initialState: State())

    init(undeliveredMessageRetention: Duration) {
        self.retentionInterval =
            Double(undeliveredMessageRetention.components.seconds) + Double(
                undeliveredMessageRetention.components.attoseconds) / 1e18
    }

    func stream<M: VMMessage>(for type: M.Type) -> AsyncStream<ReceivedMessage<M>> {
        state.withLock { state in
            let key = M.messageID
            let typeKey = ObjectIdentifier(M.self)

            let route: Route<M>
            if let existing = state.routes[key]?[typeKey] as? Route<M> {
                route = existing
            } else {
                route = Route<M>()
                state.routes[key, default: [:]][typeKey] = route
            }

            // Register the subscriber, then hand it the backlog, atomically
            // with respect to deliver(...): a concurrently arriving message
            // either lands after the drain (live delivery, subscriber is
            // registered) or was already buffered (part of the drain).
            let stream = route.multicaster.makeStream()
            drainPending(for: key, into: route, state: &state)
            return stream
        }
    }

    func deliver(messageID: String, body: Data) {
        let receivedAt = Date()
        state.withLock { state in
            pruneExpired(&state, now: receivedAt)

            let targets = state.routes[messageID].map { Array($0.values) } ?? []
            let liveTargets = targets.filter(\.hasSubscribers)

            if !liveTargets.isEmpty {
                for target in liveTargets {
                    target.deliver(body: body, receivedAt: receivedAt)
                }
                return
            }

            guard retentionInterval > 0 else {
                VMBridgeLog.messages.debug(
                    "Dropping message with no subscribers: \(messageID, privacy: .public)")
                return
            }

            buffer(
                PendingMessage(body: body, receivedAt: receivedAt), for: messageID, state: &state)
        }
    }

    func clearPending() {
        state.withLock {
            $0.pending.removeAll()
            $0.pendingRequests.removeAll()
            $0.pendingBytes = 0
            $0.pendingRequestBytes = 0
        }
    }

    func finishAll() {
        let (messageRoutes, requestRoutes) = state.withLock { state in
            (state.routes.values.flatMap(\.values), state.requestRoutes.values.flatMap(\.values))
        }
        for route in messageRoutes {
            route.finish()
        }
        for route in requestRoutes {
            route.finish()
        }
    }

    // MARK: - Requests

    /// Same contract as `stream(for:)`/`deliver(...)`, for the request
    /// namespace: requests and fire-and-forget messages of the same payload
    /// type never interfere.
    func requestStream<M: VMMessage>(for type: M.Type) -> AsyncStream<ReceivedRequest<M>> {
        state.withLock { state in
            let key = M.messageID
            let typeKey = ObjectIdentifier(M.self)

            let route: RequestRoute<M>
            if let existing = state.requestRoutes[key]?[typeKey] as? RequestRoute<M> {
                route = existing
            } else {
                route = RequestRoute<M>()
                state.requestRoutes[key, default: [:]][typeKey] = route
            }

            let stream = route.multicaster.makeStream()
            drainPendingRequests(for: key, into: route, state: &state)
            return stream
        }
    }

    func deliverRequest(
        messageID: String,
        body: Data,
        correlationID: UUID,
        respond: @escaping RequestResponder.Send
    ) {
        let receivedAt = Date()
        let responder = RequestResponder(correlationID: correlationID, send: respond)
        state.withLock { state in
            pruneExpiredRequests(&state, now: receivedAt)

            let targets = state.requestRoutes[messageID].map { Array($0.values) } ?? []
            let liveTargets = targets.filter(\.hasSubscribers)

            if !liveTargets.isEmpty {
                for target in liveTargets {
                    target.deliver(body: body, receivedAt: receivedAt, responder: responder)
                }
                return
            }

            guard retentionInterval > 0 else {
                VMBridgeLog.messages.debug(
                    "Dropping request with no subscribers: \(messageID, privacy: .public)")
                return
            }

            state.pendingRequests[messageID, default: []].append(
                PendingRequest(body: body, receivedAt: receivedAt, responder: responder)
            )
            state.pendingRequestBytes += body.count

            if var entries = state.pendingRequests[messageID],
                entries.count > Self.maximumPendingPerType
            {
                let removed = entries.removeFirst()
                state.pendingRequestBytes -= removed.body.count
                state.pendingRequests[messageID] = entries
            }

            while state.pendingRequestBytes > Self.maximumPendingBytes
                || state.pendingRequests.count > Self.maximumPendingTypes
            {
                guard
                    let oldestKey = state.pendingRequests.min(by: { lhs, rhs in
                        (lhs.value.first?.receivedAt ?? .distantFuture)
                            < (rhs.value.first?.receivedAt ?? .distantFuture)
                    })?.key, var entries = state.pendingRequests[oldestKey], !entries.isEmpty
                else {
                    break
                }
                let removed = entries.removeFirst()
                state.pendingRequestBytes -= removed.body.count
                state.pendingRequests[oldestKey] = entries.isEmpty ? nil : entries
            }
        }
    }

    private func drainPendingRequests(
        for messageID: String, into route: any RequestRouteBox, state: inout State
    ) {
        guard var entries = state.pendingRequests.removeValue(forKey: messageID) else { return }
        state.pendingRequestBytes -= entries.reduce(0) { $0 + $1.body.count }

        if retentionInterval > 0 {
            let cutoff = Date().addingTimeInterval(-retentionInterval)
            entries.removeAll { $0.receivedAt < cutoff }
        } else {
            entries.removeAll()
        }

        for entry in entries {
            route.deliver(
                body: entry.body, receivedAt: entry.receivedAt, responder: entry.responder)
        }
    }

    private func pruneExpiredRequests(_ state: inout State, now: Date) {
        guard !state.pendingRequests.isEmpty, retentionInterval > 0 else { return }
        let cutoff = now.addingTimeInterval(-retentionInterval)
        for (key, entries) in state.pendingRequests {
            let kept = entries.filter { $0.receivedAt >= cutoff }
            guard kept.count != entries.count else { continue }
            let removedBytes =
                entries.reduce(0) { $0 + $1.body.count } - kept.reduce(0) { $0 + $1.body.count }
            state.pendingRequestBytes -= removedBytes
            state.pendingRequests[key] = kept.isEmpty ? nil : kept
        }
    }

    // MARK: - Pending buffer

    private func buffer(_ message: PendingMessage, for messageID: String, state: inout State) {
        state.pending[messageID, default: []].append(message)
        state.pendingBytes += message.body.count

        // Per-type cap: oldest out first.
        if var entries = state.pending[messageID], entries.count > Self.maximumPendingPerType {
            let removed = entries.removeFirst()
            state.pendingBytes -= removed.body.count
            state.pending[messageID] = entries
        }

        // Global byte budget: evict the oldest entry across all types until
        // within bounds. A single oversized message can evict itself, which
        // just means it was too large to buffer.
        while state.pendingBytes > Self.maximumPendingBytes
            || state.pending.count > Self.maximumPendingTypes
        {
            guard
                let oldestKey = state.pending.min(by: { lhs, rhs in
                    (lhs.value.first?.receivedAt ?? .distantFuture)
                        < (rhs.value.first?.receivedAt ?? .distantFuture)
                })?.key, var entries = state.pending[oldestKey], !entries.isEmpty
            else {
                break
            }
            let removed = entries.removeFirst()
            state.pendingBytes -= removed.body.count
            state.pending[oldestKey] = entries.isEmpty ? nil : entries
        }
    }

    private func drainPending(
        for messageID: String, into route: any MessageRoute, state: inout State
    ) {
        guard var entries = state.pending.removeValue(forKey: messageID) else { return }
        state.pendingBytes -= entries.reduce(0) { $0 + $1.body.count }

        if retentionInterval > 0 {
            let cutoff = Date().addingTimeInterval(-retentionInterval)
            entries.removeAll { $0.receivedAt < cutoff }
        } else {
            entries.removeAll()
        }

        for entry in entries {
            route.deliver(body: entry.body, receivedAt: entry.receivedAt)
        }
    }

    private func pruneExpired(_ state: inout State, now: Date) {
        guard !state.pending.isEmpty, retentionInterval > 0 else { return }
        let cutoff = now.addingTimeInterval(-retentionInterval)
        for (key, entries) in state.pending {
            let kept = entries.filter { $0.receivedAt >= cutoff }
            guard kept.count != entries.count else { continue }
            let removedBytes =
                entries.reduce(0) { $0 + $1.body.count } - kept.reduce(0) { $0 + $1.body.count }
            state.pendingBytes -= removedBytes
            state.pending[key] = kept.isEmpty ? nil : kept
        }
    }
}

private protocol MessageRoute: Sendable {
    var hasSubscribers: Bool { get }
    func deliver(body: Data, receivedAt: Date)
    func finish()
}

private final class Route<M: VMMessage>: MessageRoute, Sendable {
    let multicaster = Multicaster<ReceivedMessage<M>>()

    var hasSubscribers: Bool {
        multicaster.hasSubscribers
    }

    func deliver(body: Data, receivedAt: Date) {
        do {
            let payload = try JSONDecoder().decode(M.self, from: body)
            multicaster.yield(ReceivedMessage(payload: payload, receivedAt: receivedAt))
        } catch {
            VMBridgeLog.messages.error(
                "Failed to decode \(M.messageID, privacy: .public): \(String(describing: error), privacy: .public)"
            )
        }
    }

    func finish() {
        multicaster.finish()
    }
}

private protocol RequestRouteBox: Sendable {
    var hasSubscribers: Bool { get }
    func deliver(body: Data, receivedAt: Date, responder: RequestResponder)
    func finish()
}

private final class RequestRoute<M: VMMessage>: RequestRouteBox, Sendable {
    let multicaster = Multicaster<ReceivedRequest<M>>()

    var hasSubscribers: Bool {
        multicaster.hasSubscribers
    }

    func deliver(body: Data, receivedAt: Date, responder: RequestResponder) {
        do {
            let payload = try JSONDecoder().decode(M.self, from: body)
            multicaster.yield(
                ReceivedRequest(payload: payload, receivedAt: receivedAt, responder: responder))
        } catch {
            VMBridgeLog.messages.error(
                "Failed to decode request \(M.messageID, privacy: .public): \(String(describing: error), privacy: .public)"
            )
        }
    }

    func finish() {
        multicaster.finish()
    }
}
