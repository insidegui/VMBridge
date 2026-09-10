import Foundation
import os

/// Fans out values to any number of independently-minted `AsyncStream`s.
///
/// `AsyncStream` is single-consumer, so the public API mints a fresh stream per
/// access; this type holds the shared subscriber table. When a replay value is
/// configured, new subscribers immediately receive the most recent value.
///
/// Continuations are never finished while holding the lock: `finish()` fires
/// `onTermination` synchronously, which re-enters the lock to unregister the
/// subscriber and would abort on recursion. Value yields happen after
/// snapshotting the subscriber table; producers are expected to be serialized
/// (all yields come from a single actor), which preserves per-subscriber
/// ordering, including relative to the replayed value.
final class Multicaster<Element: Sendable>: Sendable {

    private struct State {
        var subscribers: [UUID: AsyncStream<Element>.Continuation] = [:]
        var latest: Element?
        var isFinished = false
    }

    private let state: OSAllocatedUnfairLock<State>
    private let bufferingPolicy: AsyncStream<Element>.Continuation.BufferingPolicy
    private let replaysLatest: Bool

    init(
        bufferingPolicy: AsyncStream<Element>.Continuation.BufferingPolicy = .unbounded,
        replayingLatest initialValue: Element? = nil
    ) {
        self.bufferingPolicy = bufferingPolicy
        self.replaysLatest = initialValue != nil
        self.state = OSAllocatedUnfairLock(initialState: State(latest: initialValue))
    }

    deinit {
        finish()
    }

    var hasSubscribers: Bool {
        state.withLock { !$0.subscribers.isEmpty }
    }

    func makeStream() -> AsyncStream<Element> {
        AsyncStream(bufferingPolicy: bufferingPolicy) { continuation in
            let id = UUID()

            continuation.onTermination = { [weak self] _ in
                self?.state.withLock { $0.subscribers[id] = nil }
            }

            let alreadyFinished = state.withLock { state in
                if state.isFinished { return true }
                state.subscribers[id] = continuation
                if replaysLatest, let latest = state.latest {
                    // Safe inside the lock: a brand-new stream cannot have
                    // terminated yet, so this cannot recurse via onTermination.
                    continuation.yield(latest)
                }
                return false
            }
            if alreadyFinished {
                continuation.finish()
            }
        }
    }

    func yield(_ element: Element) {
        let subscribers: [AsyncStream<Element>.Continuation] = state.withLock { state in
            guard !state.isFinished else { return [] }
            if replaysLatest {
                state.latest = element
            }
            return Array(state.subscribers.values)
        }
        for continuation in subscribers {
            continuation.yield(element)
        }
    }

    func finish() {
        let subscribers: [AsyncStream<Element>.Continuation] = state.withLock { state in
            guard !state.isFinished else { return [] }
            state.isFinished = true
            let current = Array(state.subscribers.values)
            state.subscribers.removeAll()
            return current
        }
        for continuation in subscribers {
            continuation.finish()
        }
    }
}
