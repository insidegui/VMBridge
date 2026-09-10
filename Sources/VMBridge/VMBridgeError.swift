import Foundation

/// Failures of a VM connection or an operation bound to that connection.
public enum VMBridgeError: Error, Sendable, Equatable {
    case alreadyRunning
    case notRunning
    case disconnected
    case invalidPort
    case invalidDescriptor
    case incompatibleProtocol
    case handshakeTimedOut
    case malformedFrame
    case unexpectedReplyType(expected: String, actual: String)
    case requestTimedOut
    case alreadyResponded
    case payloadTooLarge(byteCount: Int, limit: Int)
    case bulkTransferRejected
    case outboundQueueFull
    case tooManyTransfers
    case bulkTransferOfferTimedOut
    case bulkTransferCancelled
    case bulkTransferAlreadyDecided
    case bulkTransferMetadataTooLarge(byteCount: Int, limit: Int)
    case bulkTransferLengthMismatch(expected: Int64, actual: Int64)
    case bulkTransferIntegrityFailure
    case bulkTransferDestinationExists(URL)
}

extension VMBridgeError: LocalizedError {
    public var errorDescription: String? {
        switch self {
        case .alreadyRunning: "The connection is already running."
        case .notRunning: "The connection is not running."
        case .disconnected: "The host–guest connection is disconnected."
        case .invalidPort: "Choose a Virtio socket port other than 0 or UInt32.max."
        case .invalidDescriptor: "The descriptor is not a connected stream socket."
        case .incompatibleProtocol: "The remote endpoint uses an incompatible VMBridge protocol."
        case .handshakeTimedOut: "The host–guest handshake timed out."
        case .malformedFrame: "The remote endpoint sent an invalid frame."
        case .unexpectedReplyType(let expected, let actual):
            "Expected a reply of type \(expected), received \(actual)."
        case .requestTimedOut: "The request timed out."
        case .alreadyResponded: "The request has already been answered."
        case .payloadTooLarge(let count, let limit):
            "The message contains \(count) bytes; the limit is \(limit)."
        case .bulkTransferRejected: "The transfer was rejected."
        case .outboundQueueFull: "The connection cannot keep up with control traffic."
        case .tooManyTransfers: "The connection already has 32 outgoing transfers."
        case .bulkTransferOfferTimedOut: "The transfer offer expired."
        case .bulkTransferCancelled: "The transfer was cancelled."
        case .bulkTransferAlreadyDecided: "The transfer offer has already been answered."
        case .bulkTransferMetadataTooLarge(let count, let limit):
            "Transfer metadata contains \(count) bytes; the limit is \(limit)."
        case .bulkTransferLengthMismatch(let expected, let actual):
            "The transfer declared \(expected) bytes but produced \(actual)."
        case .bulkTransferIntegrityFailure: "The transfer failed integrity verification."
        case .bulkTransferDestinationExists(let url): "A file already exists at \(url.path)."
        }
    }
}
