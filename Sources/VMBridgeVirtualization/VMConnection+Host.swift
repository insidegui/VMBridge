import Foundation
import VMBridge
import Virtualization

extension VMConnection {
    /// Creates an idle host connection for one VM. The VM must operate on the
    /// main queue and have a Virtio socket device configured. Reserve this port
    /// exclusively for this connection; run() installs and removes its listener.
    @MainActor public static func host(
        socketDevice: VZVirtioSocketDevice,
        port: UInt32,
        configuration: VMConfiguration = .init()
    ) -> VMConnection {
        VMConnection(
            hostSource: HostSource(device: LiveHostDevice(socketDevice: socketDevice), port: port),
            port: port, configuration: configuration)
    }
}

@MainActor protocol HostSocketConnection: AnyObject {
    var descriptor: Int32 { get }
    func close()
}

@MainActor protocol HostSocketDevice: AnyObject {
    func install(port: UInt32, accept: @escaping @MainActor (any HostSocketConnection) -> Bool)
    func remove(port: UInt32)
}

@MainActor final class HostSource: VMHostSource {
    private struct Entry {
        let connection: any HostSocketConnection
        let socket: SocketLink
    }
    private let device: any HostSocketDevice
    private let port: UInt32
    private var continuation: AsyncThrowingStream<VMTransportLink, any Error>.Continuation?
    private var entries: [UUID: Entry] = [:]

    init(device: any HostSocketDevice, port: UInt32) {
        self.device = device
        self.port = port
    }

    @MainActor func start() async throws -> AsyncThrowingStream<VMTransportLink, any Error> {
        guard continuation == nil else { throw VMBridgeError.alreadyRunning }
        let (stream, continuation) = AsyncThrowingStream<VMTransportLink, any Error>.makeStream(
            bufferingPolicy: .bufferingNewest(4))
        self.continuation = continuation
        device.install(port: port) { [weak self] connection in
            self?.accept(connection) ?? false
        }
        return stream
    }

    private func accept(_ connection: any HostSocketConnection) -> Bool {
        guard let continuation, entries.count < 8 else { return false }
        do {
            let socket = try SocketLink(duplicating: connection.descriptor)
            let id = UUID()
            entries[id] = Entry(connection: connection, socket: socket)
            let link = VMTransportLink(socket: socket) { [weak self] in await self?.release(id) }
            switch continuation.yield(link) {
            case .terminated:
                socket.cancel()
                release(id)
                return false
            case .dropped(let previous):
                Task { await previous.close() }
            case .enqueued: break
            @unknown default: break
            }
            return true
        } catch { return false }
    }

    private func release(_ id: UUID) {
        entries.removeValue(forKey: id)?.connection.close()
    }

    @MainActor func stop() async {
        guard let continuation else { return }
        self.continuation = nil
        device.remove(port: port)
        continuation.finish()
        let remaining = Array(entries.values)
        entries.removeAll()
        for entry in remaining {
            entry.socket.cancel()
            entry.connection.close()
        }
        for entry in remaining { await entry.socket.close() }
    }
}

@MainActor private final class LiveHostConnection: HostSocketConnection {
    let connection: VZVirtioSocketConnection
    init(_ connection: VZVirtioSocketConnection) { self.connection = connection }
    var descriptor: Int32 { connection.fileDescriptor }
    func close() { connection.close() }
}

// Virtualization invokes this delegate on the VM's queue. The public factory
// explicitly requires a main-queue VM, so the legacy nonisolated Objective-C
// requirement can safely enter MainActor. Keep this bridge in the host target.
@MainActor
private final class LiveHostDevice: NSObject, HostSocketDevice,
    @preconcurrency VZVirtioSocketListenerDelegate
{
    private let socketDevice: VZVirtioSocketDevice
    private var listener: VZVirtioSocketListener?
    private var accept: (@MainActor (any HostSocketConnection) -> Bool)?

    init(socketDevice: VZVirtioSocketDevice) { self.socketDevice = socketDevice }

    func install(port: UInt32, accept: @escaping @MainActor (any HostSocketConnection) -> Bool) {
        let listener = VZVirtioSocketListener()
        listener.delegate = self
        self.listener = listener
        self.accept = accept
        socketDevice.setSocketListener(listener, forPort: port)
    }

    func remove(port: UInt32) {
        socketDevice.removeSocketListener(forPort: port)
        listener?.delegate = nil
        listener = nil
        accept = nil
    }

    func listener(
        _ listener: VZVirtioSocketListener,
        shouldAcceptNewConnection connection: VZVirtioSocketConnection,
        from socketDevice: VZVirtioSocketDevice
    ) -> Bool {
        accept?(LiveHostConnection(connection)) ?? false
    }
}
