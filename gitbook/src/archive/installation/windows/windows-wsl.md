---
description: Erigon Setup on Windows Subsystem for Linux
---

# Windows Subsystem for Linux (WSL)

WSL enables you to run a complete GNU/Linux environment natively within Windows, offering Linux compatibility without the performance and resource overhead of traditional virtual machines.

### Installation and Version

* Official Installation: Follow Microsoft's official guide to install WSL: [https://learn.microsoft.com/en-us/windows/wsl/install](https://learn.microsoft.com/en-us/windows/wsl/install)
* Required Version: WSL Version 2 is the only version supported by Erigon.

### Building Erigon

Once WSL 2 is set up, you can build and run Erigon exactly as you would on a regular Linux distribution.

{% content-ref url="../linux-and-macos/" %}
[linux-and-macos](../linux-and-macos/)
{% endcontent-ref %}

### Performance and Data Storage

The location of your Erigon data directory (`datadir`) is the most crucial factor for performance in WSL.

| **Data Location**                                                         | **Performance & Configuration**                                                                                                                                                                                                              |
| ------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Recommended: Native Linux Filesystem (e.g., in your Linux home directory) | Optimal Performance. Erigon runs without restrictions, and the embedded RPC daemon works efficiently.                                                                                                                                        |
| Avoid: Mounted Windows Partitions (e.g., `/mnt/c/`, `/mnt/d/`)            | Significantly Affected Performance. This is due to: \<ul>\<li>The mounted drives using DrvFS (a slower network file system).\</li>\<li>The MDBX database locking the data for exclusive access, limiting simultaneous processes.\</li>\</ul> |

### RPC Daemon Configuration

The choice of data location directly impacts how you must configure the RPC daemon:

| **Scenario**                                  | **RPC Daemon Requirement**                                                               |
| --------------------------------------------- | ---------------------------------------------------------------------------------------- |
| Data on Native Linux Filesystem (Recommended) | Use the Embedded RPC Daemon. This is the highly preferred and most efficient method.     |
| Data on Mounted Windows Partition             | The `rpcdaemon` must be configured as a remote DB even when running on the same machine. |

{% hint style="warning" %}
⚠️ Warning: The remote DB RPC daemon is an experimental feature, is not recommended, and is extremely slow. Always aim to use the embedded RPC daemon by keeping your data on the native Linux filesystem.
{% endhint %}

### Networking Notes

Be aware that the default WSL 2 environment uses its own internal IP address, which is distinct from the IP address of your Windows host machine.

If you need to connect to Erigon from an external network (e.g., opening a port on your home router for peering on port `30303`), you must account for this separate WSL 2 IP address when configuring NAT on your router.
