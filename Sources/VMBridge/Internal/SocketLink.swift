import Darwin
import Dispatch
import Foundation
import os

/// Blocking socket I/O runs on independent queues so a full send buffer cannot
/// prevent reads from making progress. Darwin's Virtio sockets require blocking
/// connect on macOS 14 and can lose nonblocking writes under backpressure.
/// Cancellation shuts down the socket; its descriptor stays alive until every
/// in-flight system call has returned, preventing descriptor-reuse races.
package final class SocketLink: Sendable {
    private let readQueue = DispatchQueue(label: "codes.rambo.VMBridge.socket.read")
    private let writeQueue = DispatchQueue(label: "codes.rambo.VMBridge.socket.write")
    private let lifetime: SocketLifetime

    package convenience init(duplicating descriptor: Int32) throws {
        var type: Int32 = 0
        var size = socklen_t(MemoryLayout<Int32>.size)
        let result = unsafe getsockopt(descriptor, SOL_SOCKET, SO_TYPE, &type, &size)
        guard result == 0, type == SOCK_STREAM else { throw VMBridgeError.invalidDescriptor }
        var address = sockaddr_storage()
        var addressSize = socklen_t(MemoryLayout<sockaddr_storage>.size)
        let connected = withUnsafeMutablePointer(to: &address) { pointer in
            unsafe pointer.withMemoryRebound(to: sockaddr.self, capacity: 1) {
                unsafe getpeername(descriptor, $0, &addressSize)
            }
        }
        guard connected == 0 else { throw VMBridgeError.invalidDescriptor }
        let copy = dup(descriptor)
        guard copy >= 0 else { throw Self.posixError() }
        try self.init(owning: copy)
    }

    private init(owning descriptor: Int32) throws {
        let flags = fcntl(descriptor, F_GETFL)
        var noSignal: Int32 = 1
        guard flags >= 0,
            fcntl(descriptor, F_SETFL, flags & ~O_NONBLOCK) == 0,
            fcntl(descriptor, F_SETFD, FD_CLOEXEC) == 0,
            unsafe setsockopt(
                descriptor, SOL_SOCKET, SO_NOSIGPIPE, &noSignal, socklen_t(MemoryLayout<Int32>.size)
            ) == 0
        else {
            let error = Self.posixError()
            Darwin.close(descriptor)
            throw error
        }
        lifetime = SocketLifetime(descriptor: descriptor)
    }

    deinit { lifetime.cancel() }

    static func connect(port: UInt32) async throws -> SocketLink {
        let descriptor = Darwin.socket(AF_VSOCK, SOCK_STREAM, 0)
        guard descriptor >= 0 else { throw posixError() }
        let socket = try SocketLink(owning: descriptor)
        do {
            try await socket.perform(on: socket.writeQueue) { descriptor in
                // Unlike established reads/writes, a pending connect may not be
                // interrupted by shutdown. Bound it in the kernel as well as
                // at the handshake layer, then remove the timeout for writes.
                var timeout = timeval(tv_sec: 10, tv_usec: 0)
                guard unsafe setsockopt(
                    descriptor, SOL_SOCKET, SO_SNDTIMEO, &timeout,
                    socklen_t(MemoryLayout<timeval>.size)
                ) == 0 else { throw posixError() }
                var address = sockaddr_vm()
                address.svm_len = UInt8(MemoryLayout<sockaddr_vm>.size)
                address.svm_family = sa_family_t(AF_VSOCK)
                address.svm_port = port
                address.svm_cid = UInt32(VMADDR_CID_HOST)
                let result = withUnsafePointer(to: &address) { pointer in
                    unsafe pointer.withMemoryRebound(to: sockaddr.self, capacity: 1) {
                        unsafe Darwin.connect(
                            descriptor, $0, socklen_t(MemoryLayout<sockaddr_vm>.size))
                    }
                }
                guard result == 0 else { throw posixError() }
                timeout = timeval()
                guard unsafe setsockopt(
                    descriptor, SOL_SOCKET, SO_SNDTIMEO, &timeout,
                    socklen_t(MemoryLayout<timeval>.size)
                ) == 0 else { throw posixError() }
            }
            return socket
        } catch {
            await socket.close()
            throw error
        }
    }

    func read() async throws -> Data? {
        try await perform(on: readQueue) { descriptor in
            var data = Data(count: 65_536)
            while true {
                let count = unsafe data.withUnsafeMutableBytes {
                    unsafe Darwin.read(descriptor, $0.baseAddress!, $0.count)
                }
                if count < 0, errno == EINTR { continue }
                guard count >= 0 else { throw Self.posixError() }
                guard count > 0 else {
                    self.cancel()
                    return nil
                }
                data.count = count
                return data
            }
        }
    }

    func write(_ data: Data) async throws {
        try await perform(on: writeQueue) { descriptor in
            var offset = 0
            while offset < data.count {
                let count = unsafe data.withUnsafeBytes { buffer in
                    unsafe Darwin.write(
                        descriptor, buffer.baseAddress!.advanced(by: offset),
                        buffer.count - offset)
                }
                if count < 0, errno == EINTR { continue }
                guard count >= 0 else { throw Self.posixError() }
                guard count > 0 else { throw VMBridgeError.disconnected }
                offset += count
            }
        }
    }

    private func perform<T: Sendable>(
        on queue: DispatchQueue,
        _ operation: @escaping @Sendable (Int32) throws -> T
    ) async throws -> T {
        try await withTaskCancellationHandler {
            try Task.checkCancellation()
            let value: T = try await withCheckedThrowingContinuation { continuation in
                queue.async {
                    guard let descriptor = self.lifetime.beginOperation() else {
                        continuation.resume(throwing: VMBridgeError.disconnected)
                        return
                    }
                    let result = Result { try operation(descriptor) }
                    if case .failure = result { self.cancel() }
                    self.lifetime.endOperation()
                    continuation.resume(with: result)
                }
            }
            try Task.checkCancellation()
            return value
        } onCancel: {
            self.cancel()
        }
    }

    package func cancel() { lifetime.cancel() }

    package func close() async {
        cancel()
        await lifetime.waitUntilClosed()
    }

    private static func posixError() -> POSIXError {
        POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO)
    }
}

private final class SocketLifetime: Sendable {
    private struct State {
        var operations = 0
        var cancelled = false
        var closed = false
        var waiters: [CheckedContinuation<Void, Never>] = []
    }
    private let descriptor: Int32
    private let state = OSAllocatedUnfairLock(initialState: State())
    init(descriptor: Int32) { self.descriptor = descriptor }

    func beginOperation() -> Int32? {
        state.withLock { state in
            guard !state.cancelled else { return nil }
            state.operations += 1
            return descriptor
        }
    }

    func endOperation() {
        let waiters = state.withLock { state in
            state.operations -= 1
            return closeIfIdle(&state)
        }
        for waiter in waiters { waiter.resume() }
    }

    func cancel() {
        let waiters = state.withLock { state -> [CheckedContinuation<Void, Never>] in
            guard !state.cancelled else { return [] }
            state.cancelled = true
            Darwin.shutdown(descriptor, SHUT_RDWR)
            return closeIfIdle(&state)
        }
        for waiter in waiters { waiter.resume() }
    }

    private func closeIfIdle(_ state: inout State) -> [CheckedContinuation<Void, Never>] {
        guard state.cancelled, state.operations == 0, !state.closed else { return [] }
        Darwin.close(descriptor)
        state.closed = true
        let waiters = state.waiters
        state.waiters.removeAll()
        return waiters
    }

    func waitUntilClosed() async {
        await withCheckedContinuation { continuation in
            let done = state.withLock { state in
                guard !state.closed else { return true }
                state.waiters.append(continuation)
                return false
            }
            if done { continuation.resume() }
        }
    }
}
