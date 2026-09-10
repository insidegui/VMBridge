# Real VM integration and smoke testing

## Host setup

Use an existing macOS VM configuration and append a
`VZVirtioSocketDeviceConfiguration` to `configuration.socketDevices` before
creating the VM. Create the VM on the main queue and obtain its
`VZVirtioSocketDevice`. On MainActor, create `VMConnection.host(socketDevice:port:)`.
Reserve the selected port exclusively for that connection. Start the connection
run alongside the VM lifetime and keep its task so teardown can be awaited.

The host app needs its normal Virtualization entitlement and VM setup. The
VMBridge host adapter does not create a VM, start it, or configure guest images.
Its listener and accepted VZ connection objects stay on MainActor. Each accepted
socket is duplicated for I/O; the adapter retains the original connection until
the physical link closes. Host run cancellation closes both copies and removes
the listener before returning.

## Guest setup

Build the guest example from `Examples` and copy the executable to the guest.
Run it with the same port as the host, for example `VMBridgeGuestExample 51780`.
The guest uses Darwin AF_VSOCK and connects to VMADDR_CID_HOST. It requires no
IP address, Bonjour declaration, Bluetooth declaration, or working guest IP
network. The host examples target demonstrates the corresponding public API.

## Smoke-test procedure

1. Subscribe to connection state and requests on the host, then start its run
   and boot the VM. Start the guest example. Both endpoints must become connected;
   the guest sends an EchoRequest and verifies the returned text.
2. Disable the VM's IP network and repeat an echo exchange. Virtio sockets must
   continue to work.
3. Exchange a file larger than 8 MiB in each direction. Verify the received bytes
   or SHA-256, and send requests while transfers are active. Test rejecting an
   offer and cancelling a transfer; inspect for leftover `.partial` files.
4. Quit and restart the guest process. The host run and its subscriptions must
   stay alive, accept the replacement, and handle another request. Old request
   handles and incomplete transfers must fail rather than address the new guest.
5. Pause and resume the VM. The listener stays installed. Requests made while
   paused may time out; after resume, new requests must work. No heartbeat should
   force an otherwise healthy paused VM to reconnect.
6. Cancel and await the host run. The guest must report disconnection and retry
   with capped backoff. Restart the host run and confirm reconnection. Stop the
   guest run while it is retrying and verify prompt termination.
7. Cancel/await the host run before stopping or replacing the VM/socket device.
   Check that no old listener, duplicate descriptor, temporary file, or pending
   operation survives teardown. Repeat with two VMs, maintaining separate
   VMConnection instances and using the same port on their separate devices.

The automated suite validates stream-socket semantics and adapter ownership
without booting a VM. This smoke procedure is the remaining hardware/runtime
validation; passing socket-pair tests alone does not prove AF_VSOCK behavior in
a booted macOS guest.

## VirtualBuddy migration boundary

VirtualWormhole currently uses a Virtio serial port and its own packet format.
A later adoption change must add the socket device, migrate services to the
new message API, and decide host/guest upgrade compatibility. Application replay,
clipboard relay between VMs, and persisted VM identities stay in VirtualBuddy.
