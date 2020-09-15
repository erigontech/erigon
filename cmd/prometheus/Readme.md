Build: `docker-compose build --parallel`

Run only Prometheus: `docker-compose up prometheus grafana`

Run with TurboGeth, RestApi and DebugUI: `XDG_DATA_HOME=/path/to/geth/data/dir docker-compose up`

Grafana: [localhost:3000](localhost:3000), admin/admin
DebugUI: [localhost:3001](localhost:3001)

