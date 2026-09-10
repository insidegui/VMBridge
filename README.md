# VMBridge

Typed, asynchronous messaging between a macOS virtual machine and its host,
using Virtio sockets. One `VMConnection` represents one host–guest pair.

VMBridge requires macOS 14 or later and a Swift 6.3.3 or newer toolchain with
strict memory safety support. It compiles in Swift 6 language mode. The package
has no external dependencies and contains two library products:

- **VMBridge**: messages, requests, bulk transfers, and the guest socket adapter.
- **VMBridgeVirtualization**: the host adapter. Add this product only to the host.

The host supplies a main-queue `VZVirtioSocketDevice`; the guest connects to a
matching port. There is no IP networking, discovery, peer registry, encryption
configuration, or public transport plugin API. VMBridge does not create or run
virtual machines. It is an independent package.

## Add the package

While working locally, add this directory as a Swift package dependency:

```swift
.package(path: "../VMBridge")
```

Guest targets depend on `.product(name: "VMBridge", package: "VMBridge")`.
Host targets additionally depend on
`.product(name: "VMBridgeVirtualization", package: "VMBridge")`.

## Create a connection

On the guest:

```swift
import VMBridge

let connection = VMConnection.guest(port: 51_780)
```

On the host, on the main actor:

```swift
import Virtualization
import VMBridge
import VMBridgeVirtualization

// Before creating your VZVirtualMachine:
configuration.socketDevices = [VZVirtioSocketDeviceConfiguration()]

// Once your main-queue VM exists:
let device = virtualMachine.socketDevices[0] as! VZVirtioSocketDevice
let connection = VMConnection.host(socketDevice: device, port: 51_780)
```

Reserve the same port for VMBridge on both sides. Ports 0 and UInt32.max are
rejected by `run()`. Each VM has its own socket device, so the same port can
be used across multiple VMs. Keep your own `[VM.ID: VMConnection]` mapping.
Do not install another listener on that device/port while VMBridge is running.

Creation is idle. Listening, connection attempts, I/O, and retries exist only
while `try await connection.run()` is executing. Cancel the task to stop:

```swift
let runTask = Task { try await connection.run() }
// ... application lifetime ...
runTask.cancel()
try await runTask.value // Teardown has completed before this returns.
```

Surface errors from `run()` in the app instead of silently ignoring them.
Cancellation returns normally. Calling `run()` concurrently throws
`alreadyRunning`; calling it again after it returns starts a fresh run.

## Messages and state

Subscribe before starting a run. Every stream access creates an independent
subscription, and subscriptions survive reconnects and run cycles.

```swift
struct Greeting: VMMessage {
    static let messageID = "greeting.v1"
    var text: String
}

let greetings = connection.messages(of: Greeting.self)
let state = connection.state

// Run these observers alongside run(), in tasks owned by your app:
for await message in greetings {
    print(message.payload.text, message.receivedAt)
}

// From another task:
try await connection.send(Greeting(text: "Hello"))
```

Messages are JSON-encoded `Codable & Sendable` values. The default `messageID`
is the unqualified type name; override it for compatibility across renames.
There is no sender or destination ID: the connection identifies the VM.

`state` immediately yields the current state, then changes: `.stopped`,
`.connecting`, `.connected`, or `.disconnected`. `await connection.currentState`
provides a snapshot. State snapshots may coalesce when an observer is slow.

Messages with no subscriber are retained for ten seconds, consumed by the next
subscriber, and never replayed to subsequent subscribers. Requests have a
separate buffer. Each namespace allows 64 pending types, 64 values per type, and a total
512 KiB payload budget, evicting the oldest first. Set
`VMConfiguration(undeliveredMessageRetention: .zero)` to disable retention.
Buffered undelivered values are cleared on disconnect. Values already yielded
to an application subscription remain that application's responsibility.
Slow application subscribers must keep up with their message streams; use
bulk streams for sustained byte traffic.

## Request and reply

```swift
struct GetStatus: VMMessage {}
struct Status: VMMessage { var version: String }

let requests = connection.requests(of: GetStatus.self)
for await request in requests {
    try await request.reply(Status(version: "1.0"))
}

// On the other side:
let status = try await connection.send(
    GetStatus(), expecting: Status.self, timeout: .seconds(5)
)
```

Requests correlate independently, even when sent concurrently. The default
reply timeout is ten seconds. Reply types are checked by `messageID` before
JSON decoding. A reply handle permits one attempt, even if that attempt fails.
Cancellation abandons the pending request. Late responses are discarded.
Disconnect immediately fails pending requests and invalidates reply handles.

## Files and generated streams

Ordinary messages are limited to 8 MiB of encoded payload. Bulk transfers use
64 KiB chunks, a 1 MiB receive window per transfer, and fair multiplexing with
ordinary messages. Each direction permits at most 32 transfers/offers at once.

Subscribe to offers before the run:

```swift
let offers = connection.bulkTransferOffers
for await offer in offers {
    // Choose a complete destination; never use received names as paths.
    let destination = downloads.appendingPathComponent(UUID().uuidString)
    let transfer = try await offer.accept(to: destination, overwrite: false)
    let receipt = try await transfer.waitForCompletion()
    print(receipt.byteCount)
}

// On the sending side:
let transfer = try await connection.sendFile(
    at: archiveURL,
    metadata: BulkTransferMetadata(name: "archive.zip", contentType: "application/zip")
)
let receipt = try await transfer.waitForCompletion()
```

Receivers must explicitly accept or reject offers, which expire after 30
seconds. Each offer subscription buffers its latest 32 offers. File receivers write a temporary sibling, validate byte count and
SHA-256, then install the result. Existing destinations are preserved unless
`overwrite` is true; failed transfers remove their temporary files.

For generated bytes, use
`.stream(byteCount: expectedCount, chunks: yourAsyncThrowingStream)` with
`sendBulkTransfer(_:metadata:)`. The producer must supply exactly that many
bytes and bound its own upstream buffering. The library pulls and transmits
chunks under receiver credit. `offer.acceptAsStream()` returns `(transfer,
bytes)`; iterate `bytes` with one consumer and await transfer completion to
check integrity. Abandoning the byte iterator cancels the transfer.

Transfer handles expose a replaying `progress` stream plus `pause()`, `resume()`,
`cancel()`, and `waitForCompletion()`. Pausing applies within one physical
connection and may allow already-in-flight bytes to arrive. Cancelling a
completion waiter cancels only that wait; call `cancel()` to cancel the transfer.

## Reconnection and VM lifetime

The guest retries transient failures with exponential backoff from 500 ms to
5 seconds. A successful handshake resets backoff. Connect attempts and
handshakes have a ten-second timeout. Incompatible protocols and permanent
socket setup failures end the guest run. The host keeps listening after a
failed candidate or a disconnect. A valid replacement connection supersedes
the old connection after its handshake; incomplete candidates do not interrupt
an established session.

Sends while disconnected fail immediately. VMBridge does not queue outgoing
messages, replay them after reconnection, or resume interrupted transfers.
Subscribe to state and restore application-specific state explicitly.

Cancel and await the host run before stopping or replacing a VM/socket device.
Pausing a VM leaves the connection open; request deadlines continue to elapse.
There is no heartbeat or silent-peer timeout. The package uses the local
hypervisor-mediated channel without application-layer encryption, and validates
frames and transfer bounds even on this local channel.

VirtualBuddy's existing VirtualWormhole uses a serial channel and another wire
format. Adopting VMBridge requires a separate migration of VM configuration and
application services, including any replay or guest-relay policies.

## Testing

The tests use connected socket pairs and injected host adapters; no VM is
needed. See [VM smoke testing](Documentation/VMIntegration.md) for real Virtio
socket validation, and [protocol notes](Documentation/Protocol.md) for framing,
limits, and failure behavior. [Examples](Examples) compile as a separate package
using only the public API.
