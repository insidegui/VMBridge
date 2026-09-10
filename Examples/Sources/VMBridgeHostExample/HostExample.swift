import VMBridge
import VMBridgeExampleMessages
import VMBridgeVirtualization
import Virtualization

/// Call from a task owned by your VM controller. Cancel and await that task
/// before stopping or replacing the VM. The VM must use its default main queue.
@MainActor public func runEchoHost(socketDevice: VZVirtioSocketDevice, port: UInt32) async throws {
    let connection = VMConnection.host(socketDevice: socketDevice, port: port)
    let requests = connection.requests(of: EchoRequest.self)
    try await withThrowingTaskGroup(of: Void.self) { group in
        group.addTask { try await connection.run() }
        group.addTask {
            for await request in requests {
                do {
                    try await request.reply(EchoReply(text: request.payload.text))
                } catch VMBridgeError.disconnected {
                    // The guest restarted before the reply. Keep listening.
                }
            }
        }
        defer { group.cancelAll() }
        _ = try await group.next()
    }
}
