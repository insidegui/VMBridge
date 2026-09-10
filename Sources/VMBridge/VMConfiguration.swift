import Foundation

public struct VMConfiguration: Sendable {
    /// Retention for messages or requests that have no subscriber yet.
    /// Retained data is discarded when its physical connection ends.
    public var undeliveredMessageRetention: Duration

    public init(undeliveredMessageRetention: Duration = .seconds(10)) {
        self.undeliveredMessageRetention = undeliveredMessageRetention
    }
}

public enum VMConnectionState: Sendable, Equatable {
    case stopped
    case connecting
    case connected
    case disconnected
}
