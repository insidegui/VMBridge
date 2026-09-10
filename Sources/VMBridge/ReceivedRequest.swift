import Foundation
import os

/// An incoming request with a one-shot reply handle,
/// delivered via ``VMConnection/requests(of:)``.
///
/// ```swift
/// for await request in connection.requests(of: GetStatus.self) {
///     try? await request.reply(StatusReply(batteryLevel: level))
/// }
/// ```
public struct ReceivedRequest<Request: VMMessage>: Sendable {
    /// The decoded request payload.
    public let payload: Request

    /// When the request arrived from the connection. For requests that waited in
    /// the undelivered buffer, this is the arrival time, not the delivery time.
    public let receivedAt: Date

    let responder: RequestResponder

    /// Replies once. The handle expires when its physical connection ends.
    /// A failed attempt does not re-arm the handle.
    public func reply<Reply: VMMessage>(_ reply: Reply) async throws {
        try await responder.sendReply {
            let body = try JSONEncoder().encode(reply)
            guard body.count <= VMConnection.maximumMessageSize else {
                throw VMBridgeError.payloadTooLarge(
                    byteCount: body.count, limit: VMConnection.maximumMessageSize)
            }
            return (body, Reply.messageID)
        }
    }
}

/// The one-shot reply handle behind ``ReceivedRequest/reply(_:)``.
///
/// Holds the request's correlation ID plus a closure that routes the encoded
/// reply back through the connection that delivered the request. The
/// replied flag is strict one-shot: a reply attempt that fails does not
/// re-arm the handle.
final class RequestResponder: Sendable {

    typealias Send =
        @Sendable (_ correlationID: UUID, _ body: Data, _ messageID: String) async throws -> Void

    let correlationID: UUID

    private let hasReplied = OSAllocatedUnfairLock(initialState: false)
    private let send: Send

    init(correlationID: UUID, send: @escaping Send) {
        self.correlationID = correlationID

        self.send = send
    }

    func sendReply(encoding: @Sendable () throws -> (Data, String)) async throws {
        let isFirst = hasReplied.withLock { replied in
            if replied { return false }
            replied = true
            return true
        }
        guard isFirst else {
            throw VMBridgeError.alreadyResponded
        }
        let (body, messageID) = try encoding()
        try await send(correlationID, body, messageID)
    }
}
