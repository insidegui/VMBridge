import VMBridge

public struct EchoRequest: VMMessage {
    public static let messageID = "example.echo.request.v1"
    public var text: String
    public init(text: String) { self.text = text }
}

public struct EchoReply: VMMessage {
    public static let messageID = "example.echo.reply.v1"
    public var text: String
    public init(text: String) { self.text = text }
}
