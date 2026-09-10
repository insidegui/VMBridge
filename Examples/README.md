# Public API examples

Run `swift build` in this directory to compile both examples against the parent
package. Neither example uses package-scoped interfaces or test imports.

The `VMBridgeHostExample` library provides `runEchoHost(socketDevice:port:)`.
Call it in a task from an existing main-queue VM controller whose configuration
includes a Virtio socket device. Cancel/await the task before VM teardown.

Build `VMBridgeGuestExample`, copy the executable into a macOS VM, and run
`VMBridgeGuestExample 51780` while the host listens on port 51780. The guest
connects, exchanges an echo request/reply, cancels its run, and exits. Launch it
again to exercise guest process replacement without restarting the host.

See the parent package's real-VM smoke procedure for file transfer and lifecycle
validation. This example package deliberately does not create a VM or bundle
an OS image.
