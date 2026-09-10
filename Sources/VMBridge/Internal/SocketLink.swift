import Darwin
import Dispatch
import Foundation
import os

/// Queue-confined nonblocking socket I/O. Each direction has at most one
/// operation, and reads pull at most 64 KiB. The descriptor closes only after
/// both dispatch sources have cancelled, preventing descriptor-reuse races.
/// Safety: all mutable fields are accessed on `queue`; event handlers retain
/// self only while executing. The immutable sources and lifetime are Sendable.
package final class SocketLink: @unchecked Sendable {
    private let queue = DispatchQueue(label: "codes.rambo.VMBridge.socket")
    private let descriptor: Int32
    private let readSource: any DispatchSourceRead
    private let writeSource: any DispatchSourceWrite
    private let lifetime: SocketLifetime
    private var readSuspended = true
    private var writeSuspended = true
    private var closed = false
    private var reader: CheckedContinuation<Data?, any Error>?
    private var writer: CheckedContinuation<Void, any Error>?
    private var writeData = Data()
    private var writeOffset = 0
    private var connecting = false

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
            fcntl(descriptor, F_SETFL, flags | O_NONBLOCK) == 0,
            fcntl(descriptor, F_SETFD, FD_CLOEXEC) == 0,
            unsafe setsockopt(
                descriptor, SOL_SOCKET, SO_NOSIGPIPE, &noSignal, socklen_t(MemoryLayout<Int32>.size)
            ) == 0
        else {
            let error = Self.posixError()
            Darwin.close(descriptor)
            throw error
        }
        self.descriptor = descriptor
        lifetime = SocketLifetime(descriptor: descriptor)
        readSource = DispatchSource.makeReadSource(fileDescriptor: descriptor, queue: queue)
        writeSource = DispatchSource.makeWriteSource(fileDescriptor: descriptor, queue: queue)
        readSource.setEventHandler { [weak self] in self?.readReady() }
        writeSource.setEventHandler { [weak self] in self?.writeReady() }
        let lifetime = lifetime
        readSource.setCancelHandler { lifetime.sourceClosed() }
        writeSource.setCancelHandler { lifetime.sourceClosed() }
    }

    deinit {
        readSource.cancel()
        writeSource.cancel()
        if readSuspended { readSource.resume() }
        if writeSuspended { writeSource.resume() }
    }

    static func connect(port: UInt32) async throws -> SocketLink {
        let descriptor = Darwin.socket(AF_VSOCK, SOCK_STREAM, 0)
        guard descriptor >= 0 else { throw posixError() }
        let socket = try SocketLink(owning: descriptor)
        do {
            try await withTaskCancellationHandler {
                try Task.checkCancellation()
                try await withCheckedThrowingContinuation {
                    (continuation: CheckedContinuation<Void, any Error>) in
                    socket.queue.async {
                        guard !socket.closed else {
                            continuation.resume(throwing: CancellationError())
                            return
                        }
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
                        if result == 0 {
                            continuation.resume()
                            return
                        }
                        guard errno == EINPROGRESS else {
                            continuation.resume(throwing: posixError())
                            return
                        }
                        socket.connecting = true
                        socket.writer = continuation
                        socket.writeSuspended = false
                        socket.writeSource.resume()
                    }
                }
            } onCancel: {
                socket.cancel()
            }
            try Task.checkCancellation()
            return socket
        } catch {
            await socket.close()
            throw error
        }
    }

    func read() async throws -> Data? {
        try await withTaskCancellationHandler {
            try Task.checkCancellation()
            return try await withCheckedThrowingContinuation { continuation in
                queue.async {
                    guard !self.closed else {
                        continuation.resume(throwing: VMBridgeError.disconnected)
                        return
                    }
                    precondition(self.reader == nil, "Only one socket reader is permitted")
                    self.reader = continuation
                    self.readSuspended = false
                    self.readSource.resume()
                }
            }
        } onCancel: {
            self.cancel()
        }
    }

    func write(_ data: Data) async throws {
        try await withTaskCancellationHandler {
            try Task.checkCancellation()
            try await withCheckedThrowingContinuation {
                (continuation: CheckedContinuation<Void, any Error>) in
                queue.async {
                    guard !self.closed else {
                        continuation.resume(throwing: VMBridgeError.disconnected)
                        return
                    }
                    precondition(self.writer == nil, "Writes must be serialized")
                    self.writer = continuation
                    self.writeData = data
                    self.writeOffset = 0
                    self.writeSuspended = false
                    self.writeSource.resume()
                }
            }
        } onCancel: {
            self.cancel()
        }
    }

    package func cancel() {
        queue.async { self.closeOnQueue() }
    }

    package func close() async {
        cancel()
        await lifetime.waitUntilClosed()
    }

    private func readReady() {
        guard !closed, let reader else { return }
        var data = Data(count: 65_536)
        let count = unsafe data.withUnsafeMutableBytes {
            unsafe Darwin.read(descriptor, $0.baseAddress!, $0.count)
        }
        if count < 0, errno == EAGAIN || errno == EINTR { return }
        self.reader = nil
        readSource.suspend()
        readSuspended = true
        if count < 0 {
            reader.resume(throwing: Self.posixError())
            closeOnQueue()
        } else if count == 0 {
            reader.resume(returning: nil)
            closeOnQueue()
        } else {
            data.count = count
            reader.resume(returning: data)
        }
    }

    private func writeReady() {
        guard !closed, let writer else { return }
        if connecting {
            var code: Int32 = 0
            var size = socklen_t(MemoryLayout<Int32>.size)
            if unsafe getsockopt(descriptor, SOL_SOCKET, SO_ERROR, &code, &size) != 0 {
                code = errno
            }
            connecting = false
            self.writer = nil
            writeSource.suspend()
            writeSuspended = true
            if code == 0 {
                writer.resume()
            } else {
                writer.resume(throwing: POSIXError(POSIXErrorCode(rawValue: code) ?? .EIO))
                closeOnQueue()
            }
            return
        }
        let count = unsafe writeData.withUnsafeBytes { buffer in
            unsafe Darwin.write(
                descriptor, buffer.baseAddress!.advanced(by: writeOffset),
                buffer.count - writeOffset)
        }
        if count < 0, errno == EAGAIN || errno == EINTR { return }
        if count < 0 {
            self.writer = nil
            writer.resume(throwing: Self.posixError())
            closeOnQueue()
            return
        }
        writeOffset += count
        guard writeOffset == writeData.count else { return }
        self.writer = nil
        writeData.removeAll()
        writeSource.suspend()
        writeSuspended = true
        writer.resume()
    }

    private func closeOnQueue() {
        guard !closed else { return }
        closed = true
        readSource.cancel()
        writeSource.cancel()
        if readSuspended {
            readSuspended = false
            readSource.resume()
        }
        if writeSuspended {
            writeSuspended = false
            writeSource.resume()
        }
        reader?.resume(throwing: VMBridgeError.disconnected)
        writer?.resume(throwing: VMBridgeError.disconnected)
        reader = nil
        writer = nil
        writeData.removeAll()
    }

    private static func posixError() -> POSIXError {
        POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO)
    }
}

private final class SocketLifetime: Sendable {
    private struct State {
        var sources = 2
        var waiters: [CheckedContinuation<Void, Never>] = []
    }
    private let descriptor: Int32
    private let state = OSAllocatedUnfairLock(initialState: State())
    init(descriptor: Int32) { self.descriptor = descriptor }

    func sourceClosed() {
        let waiters = state.withLock { state -> [CheckedContinuation<Void, Never>] in
            state.sources -= 1
            guard state.sources == 0 else { return [] }
            Darwin.close(descriptor)
            let result = state.waiters
            state.waiters.removeAll()
            return result
        }
        for waiter in waiters { waiter.resume() }
    }

    func waitUntilClosed() async {
        await withCheckedContinuation { continuation in
            let done = state.withLock { state in
                guard state.sources > 0 else { return true }
                state.waiters.append(continuation)
                return false
            }
            if done { continuation.resume() }
        }
    }
}
