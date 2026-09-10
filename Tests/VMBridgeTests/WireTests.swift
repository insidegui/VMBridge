import Foundation
import Testing

@testable import VMBridge

@Suite struct WireTests {
    @Test func arbitraryFragmentationAndCoalescing() throws {
        let frame = WireProtocol.encodeFrame(type: .data, payload: Data("abc".utf8))
        var parser = WireProtocol.FrameParser()
        for byte in frame.dropLast() {
            parser.append(Data([byte]))
            #expect(try parser.next() == nil)
        }
        parser.append(Data([frame.last!]) + frame + frame)
        for _ in 0..<3 { #expect(try parser.next()?.payload == Data("abc".utf8)) }
        #expect(parser.isEmpty)
    }
    @Test func invalidMagicTypesAndLengthsFailAtHeader() {
        for data in [
            Data(repeating: 0, count: 9), WireProtocol.magic + Data([255, 0, 0, 0, 0]),
            WireProtocol.magic + Data([1, 255, 255, 255, 255]),
        ] {
            var parser = WireProtocol.FrameParser()
            parser.append(data)
            #expect(throws: (any Error).self) { try parser.next() }
        }
    }
    @Test func handshakeRejectsVersionsAndRoles() throws {
        try Handshake(role: "host").validate(expectedRole: "host")
        #expect(throws: VMBridgeError.incompatibleProtocol) {
            try Handshake(role: "guest").validate(expectedRole: "host")
        }
        let json = Data(#"{"protocolName":"VMBridge","version":2,"role":"host"}"#.utf8)
        let future = try JSONDecoder().decode(Handshake.self, from: json)
        #expect(throws: VMBridgeError.incompatibleProtocol) {
            try future.validate(expectedRole: "host")
        }
    }
    @Test func envelopesRoundTripAndRejectTruncation() throws {
        let id = UUID()
        let data = try RequestEnvelope.encode(
            correlationID: id, messageID: "query", body: Data([1, 2]))
        let decoded = try RequestEnvelope.decode(data)
        #expect(decoded.correlationID == id)
        #expect(decoded.messageID == "query")
        #expect(decoded.body == Data([1, 2]))
        #expect(throws: (any Error).self) { try RequestEnvelope.decode(Data(data.prefix(10))) }
        let response = try ResponseEnvelope.encode(
            correlationID: id, status: 0, messageID: "answer", body: Data())
        #expect(try ResponseEnvelope.decode(response).messageID == "answer")
    }
}
