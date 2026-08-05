---
title: "How to run a Gnosis Chain node"
description: "Run a full Gnosis Chain node with Erigon's embedded consensus layer or an external CL client."
sidebar_position: 2
---


# How to run a Gnosis Chain node

## 1. Prerequisites Check

1. Confirm your machine meets the necessary [Hardware Requirements](/get-started/hardware-requirements) based on your desired pruning mode.
2. **Install Docker**:
   * For Linux, install [Docker Engine](https://docs.docker.com/engine/install).
   * For macOS or Windows, install [Docker Desktop](https://docs.docker.com/desktop/).

## 2. Configure and Launch Erigon

Follow these steps to configure and launch the All-in-One Client. Erigon uses its embedded Consensus Layer (Caplin) by default, so you don't need a separate Consensus Client (CL).

### **A. Create the Configuration File**

Create a new file named `docker-compose.yml` in a directory where you want to manage your Erigon setup, and paste the following content into it:

```yaml
services:
  erigon:
    image: erigontech/erigon:v{ERIGON_VERSION}
    container_name: erigon-node
    restart: always
    command:
      # --- Basic Configuration ---
      - --chain=gnosis
      # Must match the container side of the volume mount below.
      - --datadir=/var/lib/erigon
      - --http.addr=0.0.0.0
      - --http.api=eth,web3,net,debug,trace,txpool
      # --- Reachability (recommended) ---
      # Lets other nodes dial you. Set EXTERNAL_IP to your public IP, or drop
      # both lines to run outbound-only with fewer peers.
      - --nat=extip:${EXTERNAL_IP}
      - --caplin.nat=extip:${EXTERNAL_IP}
      # --- Pruning Mode (Optional) ---
      # To change Pruning Mode, uncomment the line below:
      # - --prune.mode=archive
      # or
      # - --prune.mode=minimal
    ports:
      - "127.0.0.1:8545:8545" # RPC, reachable from this machine only
      - "30303:30303/tcp"     # execution-layer p2p
      - "30303:30303/udp"     # execution-layer discovery
      - "42069:42069/tcp"     # snapshot downloader (BitTorrent)
      - "42069:42069/udp"
      - "4000:4000/udp"       # Caplin consensus-layer discovery
      - "4001:4001/tcp"       # Caplin consensus-layer p2p
    volumes:
      # *** IMPORTANT: CHANGE THIS PATH! ***
      # Replace the path below with an actual directory on your machine
      # where you want the blockchain data stored (e.g., /mnt/ssd/erigon-data)
      - /path/to/erigon/data:/var/lib/erigon
```

Set `EXTERNAL_IP` before starting, either in a `.env` file next to `docker-compose.yml`:

```bash
echo "EXTERNAL_IP=$(curl -s https://api.ipify.org)" > .env
```

or by exporting it in your shell. Forward the `30303`, `42069`, `4000` and `4001`
ports on your router as well, otherwise peers still cannot reach you.

:::warning
⚠️ **Action Required**: the volume path should be changed to suit your setup — replace `/path/to/erigon/data` with a valid, empty directory on your machine where you want Erigon to store its files.

The container runs as UID/GID `1000`, so the directory must be writable by that
user — `sudo chown -R 1000:1000 /path/to/erigon/data`. See
[Permission denied inside Docker](/help-center/common-errors-and-solutions) if
you hit a `permission denied` error on startup.
:::

### **B. Launch the Node and Monitor Progress**

Open your terminal in the directory where you saved `docker-compose.yml`. To start the node and immediately see the sync process type:

```bash
docker compose up
```

That keeps the logs in the foreground, which is what you want the first time.
Once you are happy with the configuration, start it detached so the node keeps
running after you close the terminal:

```bash
docker compose up -d
```

and follow the logs when you need them with `docker compose logs -f`.

## Flag explanation

* `--chain=gnosis` specifies to run on Gnosis Chain, use `--chain=chiado` for Chiado testnet
* Add `--prune.mode=minimal` to run minimal [Pruning Mode](/fundamentals/pruning-modes) or `--prune.mode=archive` to run an archive node
* `--datadir=/var/lib/erigon` has to name the same path as the container side of the volume mount. Without it Erigon writes to its default location inside the container instead of your mounted directory, and the data ends up in an anonymous Docker volume rather than where you pointed it
* `--http.addr=0.0.0.0 --http.api=eth,web3,net,debug,trace,txpool` to use RPC and e.g. be able to connect your [web3 wallet](/fundamentals/web3-wallet). `0.0.0.0` is the address *inside* the container, which is what lets Docker forward to it at all; the `127.0.0.1:8545:8545` port mapping is what keeps it off your LAN. Change that mapping to `8545:8545` only if you intend to expose RPC to other machines
* `--nat=extip:<your public IP>` and `--caplin.nat=extip:<your public IP>` advertise a reachable address so other nodes can dial you — the first for the execution layer, the second for Caplin. Without them, and without the forwarded ports, the node still syncs but only through connections it opens itself
* `--torrent.download.rate` is deliberately not set above, because its default of `512mb` (megabytes per second) is already the maximum this recipe would ask for. During initial sync Erigon uses the full allowance, which is what you want on a dedicated machine. Add the flag only to **lower** the cap if you share the machine with other work (e.g. `--torrent.download.rate=128mb`), or set `--torrent.download.rate=Inf` to remove the limit entirely.

When you get familiar with running Erigon from CLI you may also consider [staking](/staking/caplin) and/or run a Gnosis node with an [external Consensus Layer](/get-started/easy-nodes/how-to-run-a-gnosis-chain-node/gnosis-with-an-external-cl).

:::tip
Press `Ctrl+C` in your terminal to stop Erigon.
:::

## Host networking as an alternative

Publishing each port individually is explicit, but Docker's NAT is also what
makes `--nat=extip:...` necessary in the first place. Adding `network_mode:
host` to the service instead gives the container the host's network stack
directly, so peers see the host's own address and there is nothing to map:

```yaml
services:
  erigon:
    image: erigontech/erigon:v{ERIGON_VERSION}
    network_mode: host
    command:
      - --chain=gnosis
      - --datadir=/var/lib/erigon
      # With host networking these bind on the host directly, so keep the
      # local-only services on loopback rather than 0.0.0.0.
      - --http.addr=127.0.0.1
      - --http.api=eth,web3,net,debug,trace,txpool
      - --authrpc.addr=127.0.0.1
      - --private.api.addr=127.0.0.1:9090
    volumes:
      - /path/to/erigon/data:/var/lib/erigon
```

Two things change with it, and both matter:

* the `ports:` section is ignored — port publishing does not apply, so the
  firewall on the host is what governs access;
* every listener binds on the host, so anything you do not want exposed must be
  bound explicitly to `127.0.0.1`. That is why RPC, the Engine API and the
  internal gRPC address are pinned to loopback above. `--nat` and `--caplin.nat`
  are no longer needed for the container, though you still need the p2p ports
  forwarded on your router.

Host networking is Linux-only in practice; on Docker Desktop for macOS and
Windows it does not behave the same way, so keep the published-ports form there.

Additional flags can be added to [configure](/fundamentals/configuring-erigon/) Erigon with several options.
