---
title: "Troubleshooting"
description: "Step-by-step diagnostic guides for the most frequent runtime and sync issues."
sidebar_position: 3
---

# Troubleshooting

## Resilience and Data Integrity

Erigon is highly resilient and uses a fully-transactional database. This design makes it safe against hard termination (`kill -9`) and power outages. The database ensures users never see "partial writes," meaning all data changes are atomic (all-or-nothing), and all RPC methods operate within Read-Only Transactions, guaranteeing a consistent data view.

**Protect Against Hardware Failure**: True data corruption is typically only caused by hardware failures (like disk or RAM failure). We strongly recommend using ECC memory, disk RAID, and performing regular backups to mitigate these risks.

When an issue arises, follow these steps to methodically diagnose and resolve the problem.

1. **Check Hardware Requirements:** The most common cause of issues is insufficient disk or RAM. Ensure your system meets the recommended [Hardware Requirements](/get-started/hardware-requirements). Note that Erigon is very adaptive—adding more RAM to the server will make Erigon faster without requiring any setting changes.
2. **Inspect Erigon Logs:** The logs are your best friend. Use `tail -f erigon.log` or `journalctl` to see real-time output and identify error messages or warnings. See [Logs](/fundamentals/logs) for log configuration options.
3. **Verify Sync Status:** Use `curl localhost:8545 \-X POST \-H "Content-Type: application/json" \--data '{"jsonrpc":"2.0","method":"eth\_syncing","params":\[\],"id":1}'` to check if the node is actively syncing.
4. **Monitor System Resources:** Use `htop`, `top`, or `iostat` to monitor CPU, RAM, and disk I/O. This can help you identify a performance bottleneck. For a full monitoring dashboard, see [Creating a Dashboard](/fundamentals/creating-a-dashboard).
5. **Look for OOM-kill events:** After an unexpected crash, always check your system logs for an "Out of Memory" killer event. This confirms if a memory issue caused the crash.
6. **Perform a Simple Restart**: If the node is stalled, simply restart the service using `systemctl restart erigon`. Erigon's transactional database is designed to handle interruption gracefully.
7. **Disable RPC/CL during Initial Sync**: If you are stalling during snapshot sync, try restarting without the [RPC daemon](/fundamentals/modules/rpc-daemon) or consensus client to reduce concurrent disk access, which is a bottleneck during the slowest stage (Blocks Execution).
8. **Check for Disk Space:** Regularly check your disk usage. A full disk will cause performance degradation and can lead to a node crash. See [Optimizing Storage](/fundamentals/optimizing-storage) for tips on reducing disk usage.
9. **Verify Network Time:** Ensure your system's clock is synchronized. Incorrect time can cause issues with block propagation.
10. **Check P2P Peer Connections:** Use `net\_peerCount` or similar RPC methods to check if you have a healthy number of peers. A low count may indicate a network problem. See [Default Ports](/fundamentals/default-ports) to verify firewall rules allow P2P traffic.
11. **Review Firewall Rules:** Confirm that your firewall is not blocking inbound or outbound traffic on the required P2P and RPC [ports](/fundamentals/default-ports).
12. **Double-Check Configuration Flags:** Review all your command-line flags for typos or incorrect values. A single misplaced character can cause a cryptic error. See the [CLI Reference](/fundamentals/configuring-erigon) for the full flag list.
13. **Check for Snapshot File Issues:** For version upgrades, a known issue with snapshot filenames can cause problems. Use snapshot [upgrade](/get-started/installation/upgrading) and repair options.
14. **Correct File Ownership:** If using a dedicated user or Docker, confirm that the user has full read/write access to the datadir. See the [Docker Compose](/fundamentals/docker-compose) guide for container-specific permission tips.
15. **Adjust RPC Timeouts:** If specific RPC requests are timing out, try increasing the timeout values to allow more time for heavy requests to complete. See the [RPC & API flags](/fundamentals/configuring-erigon#rpc--api) in the CLI Reference.
16. **Check for `DB.read.concurrency` issues:** If you have high RPC traffic and low TPS, try reducing the `--DB.read.concurrency` flag. See [Configuring Erigon](/fundamentals/configuring-erigon) for all database-related flags.
17. **Report a Bug:** If all else fails, open a detailed bug report on GitHub with logs, version info, and a clear description of the problem.
18. **Engage with the Community:** The Erigon Discord server is an invaluable resource for seeking help from core developers and experienced users.

## Collecting Diagnostics for Bug Reports

Before opening a GitHub issue, gather the following information to help the team reproduce and fix your problem faster.

**Dump goroutine stacks** (sends `SIGUSR1` to the running process — safe, non-destructive):

```bash
kill -SIGUSR1 $(pidof erigon)
# Stack traces are printed to the erigon log / stdout
```

If the node is wedged and you are ready to lose it, `kill -SIGABRT <pid>` dumps the stacks and terminates that process in one step. Pass an explicit PID rather than `$(pidof erigon)`: `SIGABRT` is destructive, and on a host running more than one instance `pidof` would abort all of them.

**Capture a CPU or heap profile via pprof** (requires `--pprof` flag at startup — default address `localhost:6060`; override with `--pprof.addr` and `--pprof.port`):

```bash
# CPU profile — 30-second sample
curl -o cpu.pprof http://localhost:6060/debug/pprof/profile?seconds=30

# Heap profile
curl -o heap.pprof http://localhost:6060/debug/pprof/heap

# Inspect locally
go tool pprof -http=:8080 cpu.pprof
```

Attach the `.pprof` files and the goroutine dump to your GitHub issue.

## Hetzner Cloud / Dedicated Server Firewall Note

Hetzner applies a stateless firewall at the network edge. Ensure the following ports are open for **both TCP and UDP, inbound and outbound**:

| Purpose        | Port  | Protocol |
| -------------- | ----- | -------- |
| P2P (Ethereum) | 30303 | TCP+UDP  |
| P2P (Caplin)   | 9000  | TCP+UDP  |

Without these, the node may appear to have peers (via the cloud dashboard) but will suffer poor block propagation. Configure the firewall in the Hetzner Cloud Console under **Firewalls** or via `hcloud firewall`.

A public-facing Erigon node should also not attempt to peer with IPv4 ranges that are reserved for special use. Blocking them is worth doing explicitly on Hetzner, whose abuse and netscan detection flags outbound dials to reserved ranges — that is what prompted this note, rather than their firewall filtering the traffic. Do not apply this to a node using `--caplin.local-discovery`, which deliberately peers over private IPs. The authoritative list is the [IANA IPv4 Special-Purpose Address Registry](https://www.iana.org/assignments/iana-ipv4-special-registry/iana-ipv4-special-registry.xhtml) ([RFC 6890](https://datatracker.ietf.org/doc/html/rfc6890)); the ranges below are the ones commonly blocked for an Ethereum node (`100.64.0.0/10` comes from [RFC 6598](https://datatracker.ietf.org/doc/html/rfc6598), and `192.88.99.0/24` has since been deprecated by [RFC 7526](https://datatracker.ietf.org/doc/html/rfc7526) — blocking it remains correct):

```text
0.0.0.0/8             "This" Network                                RFC 1122, Section 3.2.1.3
10.0.0.0/8            Private-Use Networks                          RFC 1918
100.64.0.0/10         Carrier-Grade NAT (CGN)                       RFC 6598, Section 7
127.0.0.0/8           Loopback                                      RFC 1122, Section 3.2.1.3
169.254.0.0/16        Link Local                                    RFC 3927
172.16.0.0/12         Private-Use Networks                          RFC 1918
192.0.0.0/24          IETF Protocol Assignments                     RFC 5736
192.0.2.0/24          TEST-NET-1                                    RFC 5737
192.88.99.0/24        6to4 Relay Anycast                            RFC 3068
192.168.0.0/16        Private-Use Networks                          RFC 1918
198.18.0.0/15         Network Interconnect Device Benchmark Testing RFC 2544
198.51.100.0/24       TEST-NET-2                                    RFC 5737
203.0.113.0/24        TEST-NET-3                                    RFC 5737
224.0.0.0/4           Multicast                                     RFC 3171
240.0.0.0/4           Reserved for Future Use                       RFC 1112, Section 4
255.255.255.255/32    Limited Broadcast                             RFC 919 and RFC 922, Section 7
```

The same list expressed in [iptables syntax](https://ethereum.stackexchange.com/questions/6386/how-to-prevent-being-blacklisted-for-running-an-ethereum-client/13068#13068).

