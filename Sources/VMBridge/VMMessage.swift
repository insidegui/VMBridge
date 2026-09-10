import Foundation

/// A value that can be exchanged between a host and guest.
///
/// Conforming types are encoded as JSON on the wire, discriminated by
/// ``messageID``. The default identifier is the unqualified type name, which
/// keeps zero-config ergonomics; override it to keep wire compatibility across
/// type renames, or to disambiguate identically-named types from different
/// modules.
public protocol VMMessage: Codable, Sendable {
    /// Stable identifier used to route this message type on the wire.
    static var messageID: String { get }
}

extension VMMessage {
    public static var messageID: String { String(describing: Self.self) }
}

/// An incoming message with its arrival time.
public struct ReceivedMessage<Payload: VMMessage>: Sendable {
    /// The decoded message payload.
    public let payload: Payload

    /// When the message arrived from the connection.
    ///
    /// For messages that waited in the undelivered buffer (see
    /// ``VMConfiguration/undeliveredMessageRetention``), this is the
    /// arrival time, not the delivery time — so consumers can judge staleness.
    public let receivedAt: Date

    init(payload: Payload, receivedAt: Date) {
        self.payload = payload

        self.receivedAt = receivedAt
    }
}

extension ReceivedMessage: Equatable where Payload: Equatable {}
extension ReceivedMessage: Hashable where Payload: Hashable {}
