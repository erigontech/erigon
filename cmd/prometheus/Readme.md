Add flag `--metrics` to Erigon or any other process (add `--metrics.addr` if need).

Add hosts to collecting metrics in: `./cmd/prometheus/prometheus.yml`

Run Grafana and Prometheus: `docker compose up -d prometheus grafana` or `make prometheus`

Go to: [localhost:3000](localhost:3000), admin/admin

Env variables:

- `XDG_DATA_HOME` re-defines default prometheus and grafana databases folder.
- `ERIGON_PROMETHEUS_CONFIG` path to custom `prometheus.yml` file. Default is: `./cmd/prometheus/prometheus.yml`
- `ERIGON_GRAFANA_CONFIG` path to custom `grafana.ini file`. Default is: `./cmd/prometheus/grafana.ini`

To add custom Erigon host: copy `./cmd/prometheus/prometheus.yml`, modify, pass new location by:
`ERIGON_PROMETHEUS_CONFIG=/new/location/prometheus.yml docker-compose up prometheus grafana`

## For developers

#### How to update dashboards

1. Edit dashboard right in Grafana UI as you need. Save.
2. Go to "Dashboard Settings" -> "JSON Model" -> Copy json representation of dashboard.
3. Go to file `./cmd/prometheus/dashboards/erigon.json` and past json there.
4. Commit and push. Done. 

#### How to add new metrics

See example: `ethdb/object_db.go:dbGetTimer`

For gRPC metrics search in code: `grpc_prometheus.Register`
