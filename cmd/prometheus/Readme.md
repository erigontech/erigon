Run Grafana and Prometheus: `docker-compose up prometheus grafana`

Go to: [localhost:3000](localhost:3000), admin/admin

List of hosts and ports to collecting metrics is in: `./cmd/prometheus/prometheus.yml`

Env variables:
- `XDG_DATA_HOME` re-defines default prometheus and grafana databases folder. 
- `TG_PROMETHEUS_CONFIG` path to custom `prometheus.yml` file. Default is: `./cmd/prometheus/prometheus.yml`
- `TG_GRAFANA_CONFIG` path to custom `grafana.ini file`. Default is: `./cmd/prometheus/grafana.ini`

To add custom TG host: copy `./cmd/prometheus/prometheus.yml`, modify, pass new location by:
`TG_PROMETHEUS_CONFIG=/new/location/prometheus.yml docker-compose up prometheus grafana`


