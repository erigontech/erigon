# Creating a dashboard

You can set up monitoring for Erigon using Grafana and Prometheus. Erigon has built-in support for this monitoring stack with pre-configured dashboards and metrics collection. This guide will walk you through setting up a complete monitoring dashboard for Erigon using Prometheus and Grafana, leveraging the built-in monitoring tools provided in the Erigon codebase.

The monitoring setup leverages Erigon's built-in metrics system and provides comprehensive visibility into node performance, storage usage, and network activity. The pre-configured dashboards are actively maintained and include metrics for both execution and consensus layer operations when running with Caplin.

## Prerequisites

- Docker and Docker Compose installed
- Erigon node running
- Basic understanding of Prometheus and Grafana

## Step 1: Enable Metrics in Erigon

First, you need to enable metrics collection in your Erigon instance by adding the `--metrics` flag.

```bash
./erigon --metrics --datadir=/your/data/dir
```

If you need to specify a custom metrics address, use `--metrics.addr`.

## Step 2: Configure Prometheus Targets

Add your Erigon hosts to the Prometheus configuration file:

1. Copy the default configuration: `./cmd/prometheus/prometheus.yml`
2. Edit the file to include your Erigon instance endpoints
3. Save the modified configuration file

## Step 3: Launch Monitoring Stack

Erigon provides a simple Docker Compose setup for the monitoring stack:

```bash
docker compose up -d prometheus grafana
```

Alternatively, you can use the make target [3](#0-2) :

```bash
make prometheus
```

## Step 4: Access Grafana Dashboard

Once the containers are running, access Grafana at [localhost:3000](http://localhost:3000).

**Default credentials:** `admin/admin`

## Step 5: Pre-configured Dashboards

Erigon comes with comprehensive pre-built dashboards that monitor various aspects of the node:

### Main Dashboard Features
The `erigon_internals.json` dashboard [5](#0-4)  includes panels for:

- **Storage Monitoring**: Snapshots, chaindata, and temp directory sizes
- **Block Processing**: Block importing latency and execution times
- **Network Activity**: Gossip bandwidth and P2P metrics
- **State Management**: Domain operations and pruning statistics

### Key Metrics to Monitor

1. **Block Execution Speed**
2. **Storage Growth** - Monitor chaindata and snapshot sizes
3. **Processing Times** - Track validation and execution latencies

## Step 6: Environment Configuration

You can customize the setup using environment variables:

- `XDG_DATA_HOME`: Changes default database folder location
- `ERIGON_PROMETHEUS_CONFIG`: Path to custom prometheus.yml file  
- `ERIGON_GRAFANA_CONFIG`: Path to custom grafana.ini file

Example with custom configuration:
```bash
ERIGON_PROMETHEUS_CONFIG=/path/to/custom/prometheus.yml docker compose up prometheus grafana
```

## Step 7: Adding Custom Hosts

To monitor multiple Erigon instances:

1. Copy `./cmd/prometheus/prometheus.yml`
2. Add your additional Erigon hosts to the targets
3. Use the custom config: `ERIGON_PROMETHEUS_CONFIG=/new/location/prometheus.yml docker compose up prometheus grafana`

## Step 8: Memory Usage Monitoring

The dashboard includes proper memory usage tracking that accounts for OS page cache. This is important because standard tools like `htop` can be misleading for Erigon memory usage.

## Troubleshooting

- Ensure Erigon is running with `--metrics` flag enabled
- Verify Prometheus can reach your Erigon metrics endpoint (default port varies)
- Check Docker container logs if services fail to start
- Confirm firewall settings allow access to monitoring ports

## Developers

For developers wanting to add custom metrics, examples can be found in the codebase, and gRPC metrics are available by searching for `grpc_prometheus.Register` in the code.

<!--
### Citations

**File:** cmd/prometheus/Readme.md (L1-1)
```markdown
Add flag `--metrics` to Erigon or any other process (add `--metrics.addr` if need).
```

**File:** cmd/prometheus/Readme.md (L3-3)
```markdown
Add hosts to collecting metrics in: `./cmd/prometheus/prometheus.yml`
```

**File:** cmd/prometheus/Readme.md (L5-5)
```markdown
Run Grafana and Prometheus: `docker compose up -d prometheus grafana` or `make prometheus`
```

**File:** cmd/prometheus/Readme.md (L7-7)
```markdown
Go to: [localhost:3000](localhost:3000), admin/admin
```

**File:** cmd/prometheus/Readme.md (L9-13)
```markdown
Env variables:

- `XDG_DATA_HOME` re-defines default prometheus and grafana databases folder.
- `ERIGON_PROMETHEUS_CONFIG` path to custom `prometheus.yml` file. Default is: `./cmd/prometheus/prometheus.yml`
- `ERIGON_GRAFANA_CONFIG` path to custom `grafana.ini file`. Default is: `./cmd/prometheus/grafana.ini`
```

**File:** cmd/prometheus/Readme.md (L15-16)
```markdown
To add custom Erigon host: copy `./cmd/prometheus/prometheus.yml`, modify, pass new location by:
`ERIGON_PROMETHEUS_CONFIG=/new/location/prometheus.yml docker compose up prometheus grafana`
```

**File:** cmd/prometheus/Readme.md (L29-31)
```markdown
See example: `ethdb/object_db.go:dbGetTimer`

```

**File:** cmd/prometheus/dashboards/erigon_internals.json (L1-60)
```json
{
  "__inputs": [
    {
      "name": "DS_GRAFANACLOUD-ERIGONOVHMONITORING-PROM",
      "label": "grafanacloud-erigonovhmonitoring-prom",
      "description": "",
      "type": "datasource",
      "pluginId": "prometheus",
      "pluginName": "Prometheus"
    }
  ],
  "__elements": {},
  "__requires": [
    {
      "type": "panel",
      "id": "barchart",
      "name": "Bar chart",
      "version": ""
    },
    {
      "type": "panel",
      "id": "bargauge",
      "name": "Bar gauge",
      "version": ""
    },
    {
      "type": "panel",
      "id": "gauge",
      "name": "Gauge",
      "version": ""
    },
    {
      "type": "grafana",
      "id": "grafana",
      "name": "Grafana",
      "version": "12.1.0-91295"
    },
    {
      "type": "panel",
      "id": "piechart",
      "name": "Pie chart",
      "version": ""
    },
    {
      "type": "datasource",
      "id": "prometheus",
      "name": "Prometheus",
      "version": "1.0.0"
    },
    {
      "type": "panel",
      "id": "stat",
      "name": "Stat",
      "version": ""
    },
    {
      "type": "panel",
      "id": "timeseries",
      "name": "Time series",
      "version": ""
```

**File:** cmd/prometheus/dashboards/erigon_internals.json (L185-195)
```json
              "expr": "devops_erigon_dirs_size_total{directory=\"/erigon-data/snapshots\", instance=~\"$nodo\"}",
              "legendFormat": "__auto",
              "range": true,
              "refId": "A",
              "datasource": {
                "type": "prometheus",
                "uid": "${DS_GRAFANACLOUD-ERIGONOVHMONITORING-PROM}"
              }
            }
          ],
          "title": "<datadir>/SNAPSHOTS TOTAL SIZE (GB)",
```

**File:** cmd/prometheus/dashboards/erigon_internals.json (L282-292)
```json
              "expr": "devops_erigon_dirs_size_total{directory=\"/erigon-data/chaindata\", instance=~\"$nodo\"}",
              "legendFormat": "__auto",
              "range": true,
              "refId": "A",
              "datasource": {
                "type": "prometheus",
                "uid": "${DS_GRAFANACLOUD-ERIGONOVHMONITORING-PROM}"
              }
            }
          ],
          "title": "<datadir>/CHAINDATA size (GB)",
```

**File:** cmd/prometheus/dashboards/erigon_internals.json (L1867-1877)
```json
              "expr": "block_importing_latency{instance=~\"$instance\"}",
              "legendFormat": "{{label_name}} {{instance}}",
              "range": true,
              "refId": "A",
              "datasource": {
                "type": "prometheus",
                "uid": "${DS_GRAFANACLOUD-ERIGONOVHMONITORING-PROM}"
              }
            }
          ],
          "title": "Block importing latency",
```

**File:** cmd/prometheus/dashboards/erigon_internals.json (L2583-2593)
```json
              "expr": "execution_time{instance=~\"$instance\"}",
              "legendFormat": "{{label_name}} {{instance}}",
              "range": true,
              "refId": "A",
              "datasource": {
                "type": "prometheus",
                "uid": "${DS_GRAFANACLOUD-ERIGONOVHMONITORING-PROM}"
              }
            }
          ],
          "title": "ValidateChain: time spent",
```

**File:** cmd/prometheus/dashboards/erigon_internals.json (L3153-3166)
```json
              "expr": "rate(gossip_topics_seen_beacon_block{instance=~\"$instance\"}[$__rate_interval])/125 > 0  ",
              "fullMetaSearch": false,
              "includeNullMetadata": true,
              "legendFormat": "kb/s {{instance}}",
              "range": true,
              "refId": "A",
              "useBackend": false,
              "datasource": {
                "type": "prometheus",
                "uid": "${DS_GRAFANACLOUD-ERIGONOVHMONITORING-PROM}"
              }
            }
          ],
          "title": "Beacon Block Gossip bandwidth",
```

**File:** cmd/prometheus/dashboards/erigon_internals.json (L4324-4345)
```json
              "expr": "domain_running_files_building{instance=~\"$instance\"}",
              "hide": false,
              "instant": false,
              "legendFormat": "running files building: {{instance}}",
              "range": true,
              "refId": "E"
            },
            {
              "datasource": {
                "type": "prometheus",
                "uid": "${DS_GRAFANACLOUD-ERIGONOVHMONITORING-PROM}"
              },
              "editorMode": "code",
              "expr": "domain_wal_flushes{instance=~\"$instance\"}",
              "hide": false,
              "instant": false,
              "legendFormat": "WAL flushes {{instance}}",
              "range": true,
              "refId": "F"
            }
          ],
          "title": "State: running collate/merge/prune",
```

**File:** cmd/prometheus/dashboards/erigon_internals.json (L5716-5725)
```json
              "expr": "chain_execution_seconds{quantile=\"$quantile\",instance=~\"$instance\"}",
              "format": "time_series",
              "interval": "",
              "intervalFactor": 1,
              "legendFormat": "execution: {{instance}}",
              "range": true,
              "refId": "A"
            }
          ],
          "title": "Block Execution speed ",
```

**File:** README.md (L443-443)
```markdown
`docker compose up prometheus grafana`, [detailed docs](./cmd/prometheus/Readme.md).
```

**File:** README.md (L751-755)
```markdown
`htop` on column `res` shows memory of "App + OS used to hold page cache for given App", but it's not informative,
because if `htop` says that app using 90% of memory you still can run 3 more instances of app on the same machine -
because most of that `90%` is "OS pages cache".
OS automatically frees this cache any time it needs memory. Smaller "page cache size" may not impact performance of
Erigon at all.
```

**File:** README.md (L762-763)
```markdown
- `Prometheus` dashboard shows memory of Go app without OS pages cache (`make prometheus`, open in
  browser `localhost:3000`, credentials `admin/admin`)
```
-->