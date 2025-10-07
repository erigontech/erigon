# Compare Executions

1. sync a minimal node...once synced kill the process

2. in `cmd/scripts/exec_bench/sample.yml` (or create a copy so you're not bothered by git), update the source_dir (from 1.)

3. create directory and set permissions:

```bash
# Create directories with correct ownership 
mkdir -p ~/.local/share/erigon-prometheus 
mkdir -p ~/.local/share/erigon-grafana # Set permissions 
sudo chown -R 1000:1000 ~/.local/share/erigon-prometheus 
sudo chown -R 472:0 ~/.local/share/erigon-grafana 
sudo chmod -R 755 ~/.local/share/erigon-prometheus 
sudo chmod -R 755 ~/.local/share/erigon-grafana
```

4. start prometheus and grafana

```bash
ERIGON_GRAFANA_DASHBOARD=/home/erigon/repo/erigon/cmd/scripts/exec_bench/dashboard ERIGON_PROMETHEUS_CONFIG=/home/erigon/repo/erigon/cmd/scripts/exec_bench/prometheus-docker.yml docker compose up -d prometheus grafana

## check logs:
docker compose logs -f

## can do port forwarding if needed
ssh -f -N -L 3000:localhost:3000 username@server
```

5. run the script:

```bash
./cmd/scripts/exec_bench/exec_bench.sh ./cmd/scripts/exec_bench/sample.yml
```

---

## updating the dashboard

- goto dashboard Share -> Export
- "Export for sharing externally" should be off
- then save the json in ./cmd/scripts/exec_bench/dashboard/exec-bench.json
- then run:
```bash
jq '.panels[].datasource = {"name": "Prometheus", "type": "prometheus"} |
    .panels[].targets[]?.datasource = {"name": "Prometheus", "type": "prometheus"}' \
    cmd/scripts/exec_bench/dashboard/exec-bench.json > output.json && mv output.json cmd/scripts/exec_bench/dashboard/exec-bench.json
```
