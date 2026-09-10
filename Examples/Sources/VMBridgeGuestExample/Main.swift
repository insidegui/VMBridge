import Foundation
import VMBridge
import VMBridgeExampleMessages

@main struct GuestExample {
    static func main() async throws {
        guard CommandLine.arguments.count == 2, let port = UInt32(CommandLine.arguments[1]) else {
            print("Usage: VMBridgeGuestExample <port>")
            return
        }
        let connection = VMConnection.guest(port: port)
        let states = connection.state
        try await withThrowingTaskGroup(of: Void.self) { group in
            group.addTask { try await connection.run() }
            group.addTask {
                for await state in states {
                    print("Connection: \(state)")
                    guard state == .connected else { continue }
                    let reply = try await connection.send(
                        EchoRequest(text: "Hello from the guest"), expecting: EchoReply.self)
                    print("Host replied: \(reply.text)")
                    return
                }
            }
            defer { group.cancelAll() }
            _ = try await group.next()
        }
    }
}
