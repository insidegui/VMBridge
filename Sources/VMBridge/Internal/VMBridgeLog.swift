import os

enum VMBridgeLog {
    static let messages = Logger(subsystem: "codes.rambo.VMBridge", category: "Messages")
    static let connection = Logger(subsystem: "codes.rambo.VMBridge", category: "Connection")
}
