# Windows Subsystem for Linux (WSL)

WSL enables running a complete GNU/Linux environment natively within Windows 10, providing Linux compatibility without the performance overhead of traditional virtualization.

To install WSL, follow Microsoft official instructions: <https://learn.microsoft.com/en-us/windows/wsl/install>.

> **Important**: WSL Version 2 is the only version supported.

Under this option you can build Erigon as you would on a regular Linux distribution (see detailed instructions [here](../installation/linux-and-macos.md)).

You can also point your data to any of the mounted Windows partitions ( e.g. `/mnt/c/[...]`, `/mnt/d/[...]` etc..) but be aware that performance will be affected: this is due to the fact that these mount points use `DrvFS`, which is a network file system, and additionally MDBX locks the db for exclusive access, meaning that only one process at a time can access the data.

> ⚠️ **Warning**: The remote db RPCdaemon is an experimental feature and is not recommended, it is extremely slow. It is highly preferable to use the embedded RPCdaemon.

This has implications for running `rpcdaemon`, which must be configured as a remote DB, even if it is running on the same machine. If your data is hosted on the native Linux filesystem instead, there are no restrictions. Also note that the default WSL2 environment has its own IP address, which does not match the network interface of the Windows host: take this into account when configuring NAT on port 30303 on your router.
