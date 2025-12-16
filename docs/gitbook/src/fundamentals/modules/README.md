---
description: 'Erigon''s Modular Architecture: Running Components as Separate Processes'
---

# Modules

Erigon is by default an "all-in-one" binary solution, but it's possible start any internal component as a separated processes:

* [RPCDaemon](rpc-daemon.md), the JSON RPC layer. (Most battle-tested external component)
* [TxPool](txpool.md), the transaction pool
* [Sentry](sentry.md), the p2p layer
* [Downloader](downloader.md), the history download layer (we don't recommend run it externally)
* [Caplin](../caplin.md), the embedded Consensus Layer

This may be for security, scalability, decentralisation, resource limitation, custom implementation, or any other reason you/your team deems appropriate. See the appropriate section to understand how to start each service separately.

{% hint style="success" %}
Don't start services as separated processes unless you have clear reason for it.
{% endhint %}
