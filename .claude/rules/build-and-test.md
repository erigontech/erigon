# Build & Test

**Requirements**: Go 1.24+, GCC 10+ or Clang, 32GB+ RAM, SSD/NVMe storage

```bash
make erigon              # Build main binary (./build/bin/erigon)
make integration         # Build integration test binary
make lint                # Run golangci-lint + mod tidy check
make test-short          # Quick unit tests (-short -failfast)
make test-all            # Full test suite
make gen                 # Generate all auto-generated code (mocks, grpc, etc.)
```

Run specific tests:
```bash
go test ./execution/stagedsync/...
go test -run TestName ./path/to/package/...
```

Before committing, always verify changes with: `make lint && make erigon integration`
