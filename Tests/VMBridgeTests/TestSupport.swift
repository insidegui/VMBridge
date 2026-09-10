import Darwin
import Foundation
import Testing

@testable import VMBridge

struct TestMessage: VMMessage, Equatable { var text: String }
struct Query: VMMessage { var value: Int }
struct Answer: VMMessage, Equatable { var value: Int }

enum TestFailure: Error { case streamEnded, timedOut }

func firstValue<T: Sendable>(
    in stream: AsyncStream<T>, where predicate: @escaping @Sendable (T) -> Bool = { _ in true }
) async throws -> T {
    try await withThrowingTaskGroup(of: T.self) { group in
        group.addTask {
            for await value in stream where predicate(value) { return value }
            throw TestFailure.streamEnded
        }
        group.addTask {
            try await Task.sleep(for: .seconds(5))
            throw TestFailure.timedOut
        }
        defer { group.cancelAll() }
        return try await group.next()!
    }
}

func socketPair() throws -> (SocketLink, SocketLink) {
    var descriptors: [Int32] = [-1, -1]
    let result = descriptors.withUnsafeMutableBufferPointer {
        unsafe Darwin.socketpair(AF_UNIX, SOCK_STREAM, 0, $0.baseAddress!)
    }
    guard result == 0 else { throw POSIXError(.EIO) }
    defer {
        Darwin.close(descriptors[0])
        Darwin.close(descriptors[1])
    }
    return try (SocketLink(duplicating: descriptors[0]), SocketLink(duplicating: descriptors[1]))
}

actor TestHostSource: VMHostSource {
    private var continuation: AsyncThrowingStream<VMTransportLink, any Error>.Continuation?
    private var queued: [VMTransportLink] = []
    private(set) var starts = 0
    private(set) var stops = 0

    func start() -> AsyncThrowingStream<VMTransportLink, any Error> {
        starts += 1
        let (stream, continuation) = AsyncThrowingStream<VMTransportLink, any Error>.makeStream()
        self.continuation = continuation
        for link in queued { continuation.yield(link) }
        queued.removeAll()
        return stream
    }
    func submit(_ socket: SocketLink) {
        let link = VMTransportLink(socket: socket)
        if let continuation { continuation.yield(link) } else { queued.append(link) }
    }
    func stop() async {
        stops += 1
        continuation?.finish()
        continuation = nil
        let pending = queued
        queued.removeAll()
        for link in pending { await link.close() }
    }
}

final class TestConnector: Sendable {
    let stream: AsyncStream<SocketLink>
    let continuation: AsyncStream<SocketLink>.Continuation
    init() { (stream, continuation) = AsyncStream.makeStream() }
    func next() async throws -> SocketLink {
        var iterator = stream.makeAsyncIterator()
        guard let socket = await iterator.next() else { throw CancellationError() }
        return socket
    }
}

func withConnectedPair(
    _ body:
        @escaping @Sendable (VMConnection, VMConnection, TestHostSource, TestConnector) async throws
        -> Void
) async throws {
    let source = TestHostSource()
    let connector = TestConnector()
    let host = VMConnection(hostSource: source, port: 1, configuration: .init())
    let guest = VMConnection(connector: { try await connector.next() })
    let (a, b) = try socketPair()
    await source.submit(a)
    connector.continuation.yield(b)
    try await withThrowingTaskGroup(of: Void.self) { group in
        group.addTask { try await host.run() }
        group.addTask { try await guest.run() }
        do {
            _ = try await firstValue(in: host.state) { $0 == .connected }
            _ = try await firstValue(in: guest.state) { $0 == .connected }
            try await body(host, guest, source, connector)
        } catch {
            group.cancelAll()
            throw error
        }
        group.cancelAll()
    }
    #expect(await host.currentState == .stopped)
    #expect(await guest.currentState == .stopped)
    #expect(await source.stops == 1)
}
