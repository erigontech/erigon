# Multiple instances / One machine

Erigon supports running multiple instances on the same machine by configuring distinct ports and data directories for each instance. Multiple instances are fully supported but require careful configuration to avoid port conflicts and resource contention. The modular architecture allows for flexible deployment patterns, from fully integrated instances to distributed service architectures. The primary consideration is the performance impact from shared disk access, especially during initial synchronization phases.

## Required Configuration Flags

To avoid conflicts between instances, you must define **6 essential flags** for each instance:

- `--datadir` - Separate data directory for each instance
- `--port` - P2P networking port (default: `30303`)
- `--http.port` - HTTP JSON-RPC port (default: `8545`)
- `--authrpc.port` - Engine API port (default: `8551`)
- `--torrent.port` - BitTorrent protocol port (default: `42069`)
- `--private.api.addr` - Internal gRPC API address (default: `127.0.0.1:9090`)

## Example Configuration

Here's how to run mainnet and sepolia instances simultaneously:

```bash
# Mainnet instance
./build/bin/erigon \
  --datadir="<your_mainnet_data_path>" \
  --chain=mainnet \
  --port=30303 \
  --http.port=8545 \
  --authrpc.port=8551 \
  --torrent.port=42069 \
  --private.api.addr=127.0.0.1:9090 \
  --http --ws \
  --http.api=eth,debug,net,trace,web3,erigon

# Sepolia instance
./build/bin/erigon \
  --datadir="<your_sepolia_data_path>" \
  --chain=sepolia \
  --port=30304 \
  --http.port=8546 \
  --authrpc.port=8552 \
  --torrent.port=42068 \
  --private.api.addr=127.0.0.1:9091 \
  --http --ws \
  --http.api=eth,debug,net,trace,web3,erigon
```

## Docker Compose Multi-Instance Setup

For containerized deployments, the docker-compose configuration shows how services can be orchestrated with proper port isolation:

The compose file demonstrates the port allocation strategy:
- **9090-9094**: Internal gRPC services (execution, sentry, consensus, downloader, txpool)
- **8545, 8551**: External HTTP APIs
- **30303, 42069**: P2P networking ports

## Best Practices

### 1. Resource Management

**Memory Considerations:**
Erigon uses memory-mapped files (MDBX) where the OS manages page cache. Multiple instances will share the same page cache efficiently, but be aware that:

- Each instance uses ~4GB RAM during genesis sync and ~1GB during normal operation
- OS page cache can utilize unlimited memory and is shared between instances
- Memory usage shown by `htop` includes OS page cache and may appear inflated

> ⚠️ **Disk Performance Warning:** Multiple instances accessing the same disk concurrently will impact performance due to increased random disk access. This is particularly problematic during the "Blocks Execution stage" which performs many random reads. **Avoid running multiple genesis syncs on the same disk.**

### 2. Database Configuration

For multiple instances, consider adjusting database parameters to reduce resource contention:

```bash
# Reduce memory-mapped database growth to minimize disk churn
--db.growth.step=32MB
--db.size.limit=512MB
```

### 3. Network Port Management

**Default Port Allocation:**

| Component | Default Port | Protocol | Purpose |
|-----------|--------------|----------|---------|
| Engine | 9090 | TCP | gRPC Server (Private) |
| Engine | 42069 | TCP/UDP | BitTorrent (Public) |
| Engine | 8551 | TCP | Engine API (Private) |
| Sentry | 30303/30304 | TCP/UDP | P2P Peering (Public) |
| RPCDaemon | 8545 | TCP | HTTP/WebSocket (Private) |

### 4. Service Separation

Erigon supports modular deployment where components can run as separate processes:

For multiple instances, you can:
- Run each instance with integrated services (default)
- Separate heavy components like `downloader` or `rpcdaemon` to dedicated processes
- Use the `--private.api.addr` flag for inter-service communication

### 5. Monitoring and Logging

Configure separate log directories for each instance:

```bash
# Instance 1
--log.dir.path=/logs/mainnet

# Instance 2  
--log.dir.path=/logs/sepolia
```

For Prometheus monitoring, each instance should expose metrics on different ports.

## Performance Optimization

### Cloud Storage Considerations

If using network-attached storage, apply these optimizations:

```bash
# Reduce disk latency impact
export ERIGON_SNAPSHOT_MADV_RND=false
--db.pagesize=64kb

# For Polygon networks
--sync.loop.block.limit=10000
```

### Memory Locking for Performance

For production setups with sufficient RAM, you can lock critical data in memory: 

```bash
# Lock domain snapshots in RAM
vmtouch -vdlw /mnt/erigon/snapshots/domain/*bt
ls /mnt/erigon/snapshots/domain/*.kv | parallel vmtouch -vdlw
```


**File:** README.md (L678-685)
```markdown
vmtouch -vdlw /mnt/erigon/snapshots/domain/*bt
ls /mnt/erigon/snapshots/domain/*.kv | parallel vmtouch -vdlw
```

# if it failing with "can't allocate memory", try: 
sync && sudo sysctl vm.drop_caches=3
echo 1 > /proc/sys/vm/compact_memory
```
```

**File:** README.md (L769-774)
```markdown
**Warning:** Multiple instances of Erigon on same machine will touch Disk concurrently, it impacts performance - one of
main Erigon optimizations: "reduce Disk random access".
"Blocks Execution stage" still does many random reads - this is reason why it's slowest stage. We do not recommend
running multiple genesis syncs on same Disk. If genesis sync passed, then it's fine to run multiple Erigon instances on
same Disk.

```

**File:** README.md (L782-791)
```markdown
What can do:

- reduce disk latency (not throughput, not iops)
    - use latency-critical cloud-drives
    - or attached-NVMe (at least for initial sync)
- increase RAM
- if you throw enough RAM, then can set env variable `ERIGON_SNAPSHOT_MADV_RND=false`
- Use `--db.pagesize=64kb` (less fragmentation, more IO)
- Or use Erigon3 (it also sensitive for disk-latency - but it will download 99% of history)

```

**File:** docker-compose.yml (L9-12)
```yaml
# Ports: `9090` execution engine (private api), `9091` sentry, `9092` consensus engine, `9093` snapshot downloader, `9094` TxPool
# Ports: `8545` json rpc, `8551` consensus json rpc, `30303` eth p2p protocol, `42069` bittorrent protocol,

# Connections: erigon -> (sentries, downloader), rpcdaemon -> (erigon, txpool), txpool -> erigon
```

**File:** docs/programmers_guide/db_faq.md (L34-42)
```markdown
### How RAM used

Erigon will use all available RAM, but this RAM will not belong to Erigon’s process. OS will own all this
memory. And OS will maintain hot part of DB in RAM. If OS will need RAM for other programs or for second Erigon instance
OS will manage all the work. This called PageCache. Erigon itself using under 2Gb. So, Erigon will benefit from more
RAM and will use all RAM without re-configuration. Same PageCache can be used by other processes if they run on same
machine by just opening same DB file. For example if RPCDaemon started with —datadir option - it will open db of
Erigon and will use same PageCache (if data A already in RAM because it’s hot and RPCDaemon read it - then it read it
from RAM not from Disk). Shared memory.
```

**File:** cmd/devnet/devnet/node.go (L190-194)
```go
	// These are set to prevent disk and page size churn which can be excessive
	// when running multiple nodes
	// MdbxGrowthStep impacts disk usage, MdbxDBSizeLimit impacts page file usage
	n.nodeCfg.MdbxGrowthStep = 32 * datasize.MB
	n.nodeCfg.MdbxDBSizeLimit = 512 * datasize.MB
```

**File:** tests/automated-testing/docker-compose.yml (L9-24)
```yaml
      --datadir=/home/erigon/.local/share/erigon --chain=dev --private.api.addr=0.0.0.0:9090 --mine --log.dir.path=/logs/node1
    ports:
      - "8551:8551"
    volumes:
      - datadir:/home/erigon/.local/share/erigon
      - ./logdir:/logs
    user: ${DOCKER_UID}:${DOCKER_GID}
    restart: unless-stopped
    mem_swappiness: 0

  erigon-node2:
    profiles:
      - second
    image: erigontech/erigon:$ERIGON_TAG
    command: |
      --datadir=/home/erigon/.local/share/erigon --chain=dev --private.api.addr=0.0.0.0:9090 --staticpeers=$ENODE --log.dir.path=/logs/node2
```

**File:** cmd/prometheus/prometheus.yml (L11-24)
```yaml
      - targets:
          - erigon:6060 # If Erigon runned by default docker-compose, then it's available on `erigon` host.
          - erigon:6061
          - erigon:6062
          - 46.149.164.51:6060
          - host.docker.internal:6060 # this is how docker-for-mac allow to access host machine
          - host.docker.internal:6061
          - host.docker.internal:6062
          - 192.168.255.134:6060
          - 192.168.255.134:6061
          - 192.168.255.134:6062
          - 192.168.255.138:6060
          - 192.168.255.138:6061
          - 192.168.255.138:6062
```
