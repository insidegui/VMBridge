# VMBridge protocol version 1

A physical connection begins with guest HELLO and host ACCEPT, both carrying
JSON `{ "protocolName": "VMBridge", "version": 1, "role": "guest" | "host" }`.
The remote role must be complementary and the version/name must match. No
application data is accepted before the handshake. There is no identity,
discovery, cryptographic negotiation, or legacy compatibility.

Frames have a nine-byte header: four ASCII bytes `VMB1`, a one-byte type, and a
four-byte unsigned big-endian payload length. Unknown types, invalid magic,
excessive lengths, malformed established envelopes, and EOF inside a frame
close that physical connection. Lengths are checked before waiting for the
payload. Socket reads pull at most 64 KiB without an intermediate receive queue.

Message envelopes contain a version byte, a UInt16 big-endian UTF-8 message ID
length, the ID, and JSON payload. Request envelopes prefix the message ID with
a 16-byte correlation UUID; responses additionally include a success status
byte. Request and response version is 1, and response status 0 means success.
A response's message ID must match the pending request's expected reply type.
The JSON body may not exceed 8 MiB. A request handle is bound to its physical
session, so an old handle cannot address a replacement connection.

Bulk control envelopes use bounded JSON, with transfer UUID and version 1.
Chunks use a version byte, transfer UUID, UInt64 big-endian byte offset, and up
to 64 KiB of data. Control payloads are limited to 64 KiB; ordinary frames allow
8 MiB plus 66,000 bytes of envelope headroom. Handshake frames are limited to
1 KiB. The receiver validates contiguous offsets, declared length, and SHA-256.
The receiver grants credit by acknowledging consumed offsets, with at most
1 MiB outstanding per transfer. Credit violations fail the affected transfer.
Malformed bulk records that cannot identify a transfer are discarded; transfer
validation failures with a known ID fail that transfer. At most 32 outgoing and
32 incoming/offered transfers may exist at once.

Control writes are prioritized. Ordinary writes alternate with transfer chunks,
and different transfers' chunks are selected round-robin. Control responses are
queued without awaiting socket writability so simultaneous transfers do not
block both read loops on full output buffers. The control queue is capped at
1 MiB and 1,024 entries; overload closes the connection. Chunk producers await
write completion and receiver credit. Once a frame starts writing, cancelling
its caller cannot truncate the frame; the write finishes or the socket closes.

No wire operation replays across a physical reconnection. Offers, callbacks,
requests, timeouts, temporary files, and unfinished transfer operations are
scoped to a physical session. The application's typed subscriptions remain
registered on the logical VMConnection through reconnections.
