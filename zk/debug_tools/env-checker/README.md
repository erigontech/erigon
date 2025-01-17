# Env Checker

This Go command reads a JSON file containing environment configurations, checks the status of nodes in each group by making JSON-RPC requests, and reports whether all groups are up or if any group is down.

## How to Run

2. Run the command with the `envFile` flag:
```bash
go run zk/debug_tools/env-checker/main.go -envFile=path/to/envs.json
```
    