import Foundation

/// VMBridge v1: VMB1 magic, frame discriminator, big-endian UInt32 length.
enum WireProtocol {

    enum FrameType: UInt8, Sendable {
        case hello = 0x01
        case accept = 0x02
        case data = 0x04
        case request = 0x05
        case response = 0x06
        case bulkOffer = 0x07
        case bulkDecision = 0x08
        case bulkChunk = 0x09
        case bulkControl = 0x0A
        case bulkFinish = 0x0B
        case bulkCompletion = 0x0C

        var isBulk: Bool {
            switch self {
            case .bulkOffer, .bulkDecision, .bulkChunk, .bulkControl,
                .bulkFinish, .bulkCompletion:
                true
            default:
                false
            }
        }

        var isBulkControl: Bool {
            self != .bulkChunk && isBulk
        }
    }

    struct RawFrame: Sendable {
        let rawType: UInt8
        let payload: Data

        var type: FrameType? { FrameType(rawValue: rawType) }
    }

    enum WireError: Error {
        case framePayloadTooLarge(Int)
        case malformedEnvelope
        case unsupportedEnvelopeVersion(UInt8)
        case messageIDTooLong
    }

    static let headerSize = 9
    static let magic = Data([0x56, 0x4D, 0x42, 0x31])  // VMB1

    /// Maximum ordinary-frame plaintext: the public message size limit plus
    /// headroom for the largest message/request/response envelope.
    static let maximumOrdinaryPlaintext =
        VMConnection.maximumMessageSize + 66_000
    static let maximumBulkControlPayload = 64 * 1024
    static let maximumBulkChunkSize = 64 * 1024
    static let maxFramePayload = maximumOrdinaryPlaintext

    static func maximumPayload(for rawType: UInt8) -> Int {
        switch FrameType(rawValue: rawType) {
        case .bulkChunk:
            return maximumBulkChunkSize
                + BulkChunkEnvelope.headerSize

        case .bulkOffer, .bulkDecision, .bulkControl, .bulkFinish,
            .bulkCompletion:
            return maximumBulkControlPayload

        case .hello, .accept:
            return 1024
        default:
            return maxFramePayload
        }
    }

    static func encodeFrame(type: FrameType, payload: Data) -> Data {
        var packet = Data(capacity: headerSize + payload.count)
        packet.append(magic)
        packet.append(type.rawValue)
        let length = UInt32(payload.count)
        packet.append(UInt8((length >> 24) & 0xFF))
        packet.append(UInt8((length >> 16) & 0xFF))
        packet.append(UInt8((length >> 8) & 0xFF))
        packet.append(UInt8(length & 0xFF))
        packet.append(payload)
        return packet
    }

    /// Incrementally parses the framing layer from a stream of received chunks.
    struct FrameParser {
        private var buffer = Data()
        var isEmpty: Bool { buffer.isEmpty }

        mutating func append(_ data: Data) {
            buffer.append(data)
        }

        /// Returns the next complete frame, or `nil` when more data is needed.
        /// Throws when a frame declares a payload larger than
        /// ``WireProtocol/maxFramePayload``, which should close the connection.
        mutating func next() throws -> RawFrame? {
            guard buffer.count >= headerSize else { return nil }

            let start = buffer.startIndex
            guard buffer[start..<(start + 4)].elementsEqual(magic) else {
                throw WireError.malformedEnvelope
            }
            let rawType = buffer[start + 4]
            guard FrameType(rawValue: rawType) != nil else { throw WireError.malformedEnvelope }

            var length: UInt32 = 0
            for byte in buffer[(start + 5)..<(start + headerSize)] {
                length = (length << 8) | UInt32(byte)
            }
            let payloadLength = Int(length)

            guard payloadLength <= maximumPayload(for: rawType) else {
                throw WireError.framePayloadTooLarge(payloadLength)
            }

            guard buffer.count >= headerSize + payloadLength else { return nil }

            let payloadStart = start + headerSize
            let payload = Data(buffer[payloadStart..<(payloadStart + payloadLength)])
            buffer.removeFirst(headerSize + payloadLength)

            return RawFrame(rawType: rawType, payload: payload)
        }
    }
}

struct BulkOfferEnvelope: Codable, Sendable {
    static let currentVersion = 1

    let version: Int
    let id: UUID
    let metadata: BulkTransferMetadata
    let byteCount: Int64

    init(id: UUID, metadata: BulkTransferMetadata, byteCount: Int64) {
        self.version = Self.currentVersion
        self.id = id
        self.metadata = metadata
        self.byteCount = byteCount
    }
}

struct BulkDecisionEnvelope: Codable, Sendable {
    static let currentVersion = 1

    let version: Int
    let id: UUID
    let accepted: Bool

    init(id: UUID, accepted: Bool) {
        self.version = Self.currentVersion
        self.id = id
        self.accepted = accepted
    }
}

struct BulkControlEnvelope: Codable, Sendable {
    enum Action: String, Codable, Sendable {
        case acknowledgement
        case pause
        case resume
        case cancel
    }

    static let currentVersion = 1

    let version: Int
    let id: UUID
    let action: Action
    let acknowledgedOffset: Int64?

    init(
        id: UUID,
        action: Action,
        acknowledgedOffset: Int64? = nil
    ) {
        self.version = Self.currentVersion
        self.id = id
        self.action = action
        self.acknowledgedOffset = acknowledgedOffset
    }
}

struct BulkFinishEnvelope: Codable, Sendable {
    static let currentVersion = 1

    let version: Int
    let id: UUID
    let byteCount: Int64
    let sha256Digest: Data

    init(id: UUID, byteCount: Int64, sha256Digest: Data) {
        self.version = Self.currentVersion
        self.id = id
        self.byteCount = byteCount
        self.sha256Digest = sha256Digest
    }
}

struct BulkCompletionEnvelope: Codable, Sendable {
    enum Status: String, Codable, Sendable {
        case success
        case rejected
        case cancelled
        case invalidLength
        case integrityFailure
        case failed
    }

    static let currentVersion = 1

    let version: Int
    let id: UUID
    let status: Status

    init(id: UUID, status: Status) {
        self.version = Self.currentVersion
        self.id = id
        self.status = status
    }
}

enum BulkChunkEnvelope {
    static let version: UInt8 = 1
    static let headerSize = 1 + 16 + 8

    static func encode(id: UUID, offset: Int64, bytes: Data) throws -> Data {
        guard offset >= 0, bytes.count <= WireProtocol.maximumBulkChunkSize else {
            throw WireProtocol.WireError.malformedEnvelope
        }
        var data = Data(capacity: headerSize + bytes.count)
        data.append(version)
        data.append(contentsOf: uuidBytes(id))
        let value = UInt64(offset)
        for shift in stride(from: 56, through: 0, by: -8) {
            data.append(UInt8((value >> UInt64(shift)) & 0xFF))
        }
        data.append(bytes)
        return data
    }

    static func decode(_ data: Data) throws -> (
        id: UUID,
        offset: Int64,
        bytes: Data
    ) {
        guard data.count >= headerSize else {
            throw WireProtocol.WireError.malformedEnvelope
        }
        var reader = EnvelopeReader(data)
        try reader.expectVersion(version)
        let id = try reader.readUUID()
        var offset: UInt64 = 0
        for _ in 0..<8 {
            offset = (offset << 8) | UInt64(try reader.readByte())
        }
        guard offset <= UInt64(Int64.max) else {
            throw WireProtocol.WireError.malformedEnvelope
        }
        let bytes = reader.remainder()
        guard bytes.count <= WireProtocol.maximumBulkChunkSize else {
            throw WireProtocol.WireError.malformedEnvelope
        }
        return (id, Int64(offset), bytes)
    }
}

/// The payload of a `data` frame:
///
/// ```
/// [u8 envelope_version][u16 big-endian id_length][id_length bytes messageID][message body]
/// ```
///
/// The explicit version byte lets future versions add fields (e.g. a
/// request/response correlation ID) without breaking older peers.
enum MessageEnvelope {

    static let version: UInt8 = 1

    static func encode(messageID: String, body: Data) throws -> Data {
        let idBytes = Data(messageID.utf8)
        guard idBytes.count <= Int(UInt16.max) else {
            throw WireProtocol.WireError.messageIDTooLong
        }

        var envelope = Data(capacity: 3 + idBytes.count + body.count)
        envelope.append(version)
        envelope.append(UInt8((idBytes.count >> 8) & 0xFF))
        envelope.append(UInt8(idBytes.count & 0xFF))
        envelope.append(idBytes)
        envelope.append(body)
        return envelope
    }

    static func decode(_ data: Data) throws -> (messageID: String, body: Data) {
        guard data.count >= 3 else {
            throw WireProtocol.WireError.malformedEnvelope
        }

        let start = data.startIndex
        let envelopeVersion = data[start]
        guard envelopeVersion == version else {
            throw WireProtocol.WireError.unsupportedEnvelopeVersion(envelopeVersion)
        }

        let idLength = (Int(data[start + 1]) << 8) | Int(data[start + 2])
        let idStart = start + 3
        guard data.count >= 3 + idLength else {
            throw WireProtocol.WireError.malformedEnvelope
        }

        guard
            let messageID = String(
                data: Data(data[idStart..<(idStart + idLength)]), encoding: .utf8)
        else {
            throw WireProtocol.WireError.malformedEnvelope
        }

        let body = Data(data[(idStart + idLength)...])
        return (messageID, body)
    }
}

/// The payload of a `request` frame:
///
/// ```
/// [u8 version][16 bytes correlation UUID][u16 BE id_length][messageID][body]
/// ```
enum RequestEnvelope {

    static let version: UInt8 = 1

    static func encode(correlationID: UUID, messageID: String, body: Data) throws -> Data {
        let idBytes = Data(messageID.utf8)
        guard idBytes.count <= Int(UInt16.max) else {
            throw WireProtocol.WireError.messageIDTooLong
        }

        var envelope = Data(capacity: 19 + idBytes.count + body.count)
        envelope.append(version)
        envelope.append(contentsOf: uuidBytes(correlationID))
        envelope.append(UInt8((idBytes.count >> 8) & 0xFF))
        envelope.append(UInt8(idBytes.count & 0xFF))
        envelope.append(idBytes)
        envelope.append(body)
        return envelope
    }

    static func decode(_ data: Data) throws -> (correlationID: UUID, messageID: String, body: Data)
    {
        var reader = EnvelopeReader(data)
        try reader.expectVersion(version)
        let correlationID = try reader.readUUID()
        let messageID = try reader.readLengthPrefixedString()
        return (correlationID, messageID, reader.remainder())
    }
}

/// The payload of a `response` frame:
///
/// ```
/// [u8 version][16 bytes correlation UUID][u8 status][u16 BE id_length][messageID][body]
/// ```
///
/// Status `0` means success; all other values are reserved for future
/// protocol versions and rejected by current requesters.
enum ResponseEnvelope {

    static let version: UInt8 = 1
    static let successStatus: UInt8 = 0

    static func encode(correlationID: UUID, status: UInt8, messageID: String, body: Data) throws
        -> Data
    {
        let idBytes = Data(messageID.utf8)
        guard idBytes.count <= Int(UInt16.max) else {
            throw WireProtocol.WireError.messageIDTooLong
        }

        var envelope = Data(capacity: 20 + idBytes.count + body.count)
        envelope.append(version)
        envelope.append(contentsOf: uuidBytes(correlationID))
        envelope.append(status)
        envelope.append(UInt8((idBytes.count >> 8) & 0xFF))
        envelope.append(UInt8(idBytes.count & 0xFF))
        envelope.append(idBytes)
        envelope.append(body)
        return envelope
    }

    static func decode(_ data: Data) throws -> (
        correlationID: UUID, status: UInt8, messageID: String, body: Data
    ) {
        var reader = EnvelopeReader(data)
        try reader.expectVersion(version)
        let correlationID = try reader.readUUID()
        let status = try reader.readByte()
        let messageID = try reader.readLengthPrefixedString()
        return (correlationID, status, messageID, reader.remainder())
    }
}

/// Sequential parser for envelope payloads; all bounds-checked, no unsafe
/// pointer use.
private struct EnvelopeReader {
    private let data: Data
    private var offset: Data.Index

    init(_ data: Data) {
        self.data = data
        self.offset = data.startIndex
    }

    mutating func expectVersion(_ expected: UInt8) throws {
        let byte = try readByte()
        guard byte == expected else {
            throw WireProtocol.WireError.unsupportedEnvelopeVersion(byte)
        }
    }

    mutating func readByte() throws -> UInt8 {
        guard offset < data.endIndex else {
            throw WireProtocol.WireError.malformedEnvelope
        }
        defer { offset += 1 }
        return data[offset]
    }

    mutating func readUUID() throws -> UUID {
        guard data.distance(from: offset, to: data.endIndex) >= 16 else {
            throw WireProtocol.WireError.malformedEnvelope
        }
        let bytes = Array(data[offset..<(offset + 16)])
        offset += 16
        return UUID(
            uuid: (
                bytes[0], bytes[1], bytes[2], bytes[3],
                bytes[4], bytes[5], bytes[6], bytes[7],
                bytes[8], bytes[9], bytes[10], bytes[11],
                bytes[12], bytes[13], bytes[14], bytes[15]
            ))
    }

    mutating func readLengthPrefixedString() throws -> String {
        let high = try readByte()
        let low = try readByte()
        let length = (Int(high) << 8) | Int(low)
        guard data.distance(from: offset, to: data.endIndex) >= length else {
            throw WireProtocol.WireError.malformedEnvelope
        }
        let stringData = Data(data[offset..<(offset + length)])
        offset += length
        guard let string = String(data: stringData, encoding: .utf8) else {
            throw WireProtocol.WireError.malformedEnvelope
        }
        return string
    }

    func remainder() -> Data {
        Data(data[offset...])
    }
}

private func uuidBytes(_ uuid: UUID) -> [UInt8] {
    let u = uuid.uuid
    return [
        u.0, u.1, u.2, u.3, u.4, u.5, u.6, u.7,
        u.8, u.9, u.10, u.11, u.12, u.13, u.14, u.15,
    ]
}
