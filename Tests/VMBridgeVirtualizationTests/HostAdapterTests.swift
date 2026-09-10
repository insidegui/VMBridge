import Darwin
import Foundation
import Testing
import VMBridge

@testable import VMBridgeVirtualization

@MainActor private final class FakeConnection: HostSocketConnection {
    let descriptor: Int32
    private(set) var closeCount = 0
    init(descriptor: Int32) { self.descriptor = descriptor }
    func close() {
        closeCount += 1
        if closeCount == 1 { Darwin.close(descriptor) }
    }
}

@MainActor private final class FakeDevice: HostSocketDevice {
    var accept: (@MainActor (any HostSocketConnection) -> Bool)?
    var installed: [UInt32] = []
    var removed: [UInt32] = []
    func install(port: UInt32, accept: @escaping @MainActor (any HostSocketConnection) -> Bool) {
        installed.append(port)
        self.accept = accept
    }
    func remove(port: UInt32) {
        removed.append(port)
        accept = nil
    }
}

@Suite @MainActor struct HostAdapterTests {
    @Test func listenerRetainsAcceptedConnectionAndClosesOnRelease() async throws {
        let device = FakeDevice()
        let source = HostSource(device: device, port: 123)
        let stream = try await source.start()
        #expect(device.installed == [123])
        var descriptors: [Int32] = [-1, -1]
        let result = descriptors.withUnsafeMutableBufferPointer {
            unsafe Darwin.socketpair(AF_UNIX, SOCK_STREAM, 0, $0.baseAddress!)
        }
        #expect(result == 0)
        defer { Darwin.close(descriptors[1]) }
        let connection = FakeConnection(descriptor: descriptors[0])
        #expect(device.accept?(connection) == true)
        var iterator = stream.makeAsyncIterator()
        let link = try #require(try await iterator.next())
        #expect(connection.closeCount == 0)
        await link.close()
        #expect(connection.closeCount == 1)
        await source.stop()
        #expect(device.removed == [123])
        #expect(connection.closeCount == 1)
        #expect(try await iterator.next() == nil)
    }

    @Test func stopClosesUndeliveredConnectionsAndCanRestart() async throws {
        let device = FakeDevice()
        let source = HostSource(device: device, port: 123)
        let stream = try await source.start()
        defer { withExtendedLifetime(stream) {} }
        var descriptors: [Int32] = [-1, -1]
        let result = descriptors.withUnsafeMutableBufferPointer {
            unsafe Darwin.socketpair(AF_UNIX, SOCK_STREAM, 0, $0.baseAddress!)
        }
        #expect(result == 0)
        defer { Darwin.close(descriptors[1]) }
        let connection = FakeConnection(descriptor: descriptors[0])
        #expect(device.accept?(connection) == true)
        await source.stop()
        #expect(connection.closeCount == 1)
        _ = try await source.start()
        await source.stop()
        #expect(device.installed == [123, 123])
        #expect(device.removed == [123, 123])
    }
}
