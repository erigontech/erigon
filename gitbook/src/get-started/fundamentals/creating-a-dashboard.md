---
description: Enhancing Erigon Node Monitoring with Prometheus and Grafana
---

# Creating a dashboard

Erigon provides robust, built-in support for monitoring using the Prometheus and Grafana stack. This setup offers comprehensive visibility into node performance, storage usage, and network activity, including relevant metrics for the integrated Consensus Layer (Caplin).

#### Prerequisites

* Docker and Docker Compose installed
* Erigon node running
* Basic understanding of Prometheus and Grafana

#### Step 1: Enable Metrics in Erigon

You must first enable metrics collection in your running Erigon instance.

```sh
./erigon --metrics --datadir=/your/data/dir
```

To specify a custom address for metrics, use the `--metrics.addr` flag.

#### Step 2: Configure Prometheus Targets

The Erigon codebase includes a default configuration file for Prometheus.

1. Copy the default configuration: `./cmd/prometheus/prometheus.yml`
2. Edit the file to include the endpoints of your running Erigon instance(s).
3. Save the modified configuration file.

#### Step 3: Launch Monitoring Stack

Erigon provides a simple Docker Compose setup to launch the monitoring services.

```sh
docker compose up -d prometheus grafana
```

Alternatively, use the built-in $$ $\text{make}$ $$ target: $$ $\text{make prometheus}$ $$

```sh
make prometheus
```

#### Step 4: Access Grafana Dashboard

Once the containers are running, access the Grafana interface at $$ $\text{localhost:3000}$ $$.

* Default credentials: `admin/admin`

#### Step 5: Utilize Pre-configured Dashboards

Erigon comes with comprehensive, pre-built dashboards that you can find in `./cmd/prometheus/readme.md` .

The `erigon.json` dashboard is the recommended high-level board for most users, tracking critical performance and resource metrics:

* **Performance**: Block Execution Speed, Processing Times (validation and execution latencies).
* **Storage & Growth**: Monitor chaindata and snapshot sizes.
* **Network Activity**: Gossip bandwidth and P2P metrics.
* **State Management**: Domain operations and pruning statistics.

#### Step 6: Memory Usage Monitoring (Important Note)

Standard OS tools like `htop`$$ $\text{htop}$ $$ can be misleading for Erigon's memory usage because its database (MDBX) uses `MemoryMap`. The OS manages the OS Page Cache, which is shared and automatically freed when needed.

The dedicated panels in the `erigon.json` dashboard track accurate Go memory statistics. Erigon's application typically uses around 1GB during normal operation, while the OS Page Cache handles the bulk of data access memory efficiently.

#### Step 7: Environment and Custom Configuration

You can customize the setup using environment variables:

| **Variable**                                                        | **Description**                                                      |
| ------------------------------------------------------------------- | -------------------------------------------------------------------- |
| `XDG_DATA_HOME`$$ $\text{XDG\_DATA\_HOME}$ $$                       | Changes default database folder location.                            |
| $$ $\text{ERIGON\_PROMETHEUS\_CONFIG}$ $$`ERIGON_PROMETHEUS_CONFIG` | Path to a custom $$ $\text{prometheus.yml}$ $$`prometheus.yml` file. |
| `ERIGON_GRAFANA_CONFIG`$$ $\text{ERIGON\_GRAFANA\_CONFIG}$ $$       | Path to a custom `grafana.ini`$$ $\text{grafana.ini}$ $$ file.       |

Example with a Custom Prometheus Configuration:

{% code overflow="wrap" %}
```sh
ERIGON_PROMETHEUS_CONFIG=/path/to/custom/prometheus.yml docker compose up prometheus grafana
```
{% endcode %}

#### Troubleshooting

* Ensure Erigon is running with the `--metrics` flag enabled.
* Verify Prometheus can reach your Erigon metrics endpoint (default port varies).
* Check Docker container logs if services fail to start.
* Confirm firewall settings allow access to monitoring ports.

#### For Developers

For developers, the `erigon_internals.json` dashboard offers a low-level, complex view of the node for in-depth debugging (not recommended for typical users). Custom metrics can be added by searching for `grpc_prometheus.Register` within the codebase.
