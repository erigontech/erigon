---
description: Enhancing Erigon Node Monitoring with Prometheus and Grafana
metaLinks:
  alternates:
    - >-
      https://app.gitbook.com/s/3DGBf2RdbfoitX1XMgq0/fundamentals/creating-a-dashboard
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

To specify a custom address and port for metrics, use the `--metrics.addr` and `--metrics.port` flags (default port: `6061`).

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

Alternatively, use the built-in `make` target:

```sh
make prometheus
```

#### Step 4: Access Grafana Dashboard

Once the containers are running, access the Grafana interface at `localhost:3000`.

* Default credentials: `admin/admin`

#### Step 5: Utilize Pre-configured Dashboards

Erigon comes with pre-built dashboards located in `./cmd/prometheus/dashboards/`. See `./cmd/prometheus/Readme.md` for details.

**`erigon.json`** is the recommended dashboard for most users. It contains the following sections:

* **Blockchain**: Block execution speed and processing times.
* **Block consume delay**: Latency between block production and consumption.
* **RPC**: Request rates and response times for the JSON-RPC interface.
* **Private api** (collapsed): Internal gRPC API metrics.

**`erigon_internals.json`** is a low-level dashboard used by the Erigon development team for deep internal debugging. It is exported from Erigon's own Grafana Cloud instance and requires a pre-release Grafana build — it is _not recommended_ for typical users or self-hosted setups.

#### Step 6: Memory Usage Monitoring (Important Note)

Standard OS tools like `htop` can be misleading for Erigon's memory usage because its database (MDBX) uses `MemoryMap`. The OS manages the OS Page Cache, which is shared and automatically freed when needed.

The dedicated panels in the `erigon.json` dashboard track accurate Go memory statistics. Erigon's application typically uses around 1GB during normal operation, while the OS Page Cache handles the bulk of data access memory efficiently.

#### Step 7: Environment and Custom Configuration

You can customize the setup using environment variables:

| **Variable**               | **Description**                           |
| -------------------------- | ----------------------------------------- |
| `XDG_DATA_HOME`            | Changes default database folder location.  |
| `ERIGON_PROMETHEUS_CONFIG` | Path to a custom `prometheus.yml` file.    |
| `ERIGON_GRAFANA_CONFIG`    | Path to a custom `grafana.ini` file.       |
| `ERIGON_GRAFANA_DASHBOARD` | Path to a custom dashboards directory.     |

Example with a Custom Prometheus Configuration:

{% code overflow="wrap" %}
```sh
ERIGON_PROMETHEUS_CONFIG=/path/to/custom/prometheus.yml docker compose up prometheus grafana
```
{% endcode %}

#### Troubleshooting

* Ensure Erigon is running with the `--metrics` flag enabled.
* Verify Prometheus can reach your Erigon metrics endpoint (default port: `6061`).
* Check Docker container logs if services fail to start.
* Confirm firewall settings allow access to monitoring ports.

#### For Developers

Custom metrics can be added by searching for `grpc_prometheus.Register` within the codebase.
