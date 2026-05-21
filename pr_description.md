# execution/tests, genesiswrite: isolate per-file DB and reduce genesis mmap to fix SIGBUS crash

## What was failing

The `tests-mac-linux (ubuntu-24.04)` CI job crashed with **SIGBUS (bus error)** during `TestState` in the `execution/tests` package. The crash occurred inside `GenesisToBlock` → `mdbx_env_open` when opening a temporary MDBX environment.

**Root cause (two contributing factors):**

1. **2 TB mmap reservation per `GenesisToBlock` call** (`execution/state/genesiswrite/genesis_write.go:338`): Each call to `GenesisToBlock` opened a temporary MDBX database with a 2 TB virtual address space reservation. When multiple parallel test subtests called `GenesisToBlock` concurrently, the combined reservations exhausted the process's virtual address space (~128 TB on x86-64), causing SIGBUS (sigcode=2 — nonexistent physical address) during `mdbx_env_open`.

2. **Shared database across parallel test files** (`execution/tests/state_test.go`): Both `TestState` and `TestStateCornerCases` created a single `temporaltest.NewTestDB` and `datadir.Dirs` for the entire test, then shared them across hundreds of parallel file-level subtests (each file runs via `t.Parallel()`). This caused state corruption through the shared aggregator, producing "Wrong trie root" errors visible in the CI log, and amplified the SIGBUS by re-running failed tests through `withTrace`.

## What was changed

### `execution/state/genesiswrite/genesis_write.go` (production code)

Reduced `genesisMapSize` from `2 * datasize.TB` to `16 * datasize.GB` on non-Windows platforms (line 338). 16 GB is still more than sufficient for any practical genesis block allocation, while allowing 100+ concurrent MDBX environments to coexist within the process's virtual address space. This matches the value already used on `main`.

### `execution/tests/state_test.go` (test infrastructure)

Moved `datadir.New(t.TempDir())` and `temporaltest.NewTestDB(t, dirs)` from the top-level test function into the `st.walk` callback, so each JSON test file gets its own isolated database and data directory. This prevents state corruption between parallel file subtests and eliminates the "Wrong trie root" errors. This matches the pattern already used on `main` in `runStateTests`.

## How the fix was verified

- `TestStateCornerCases`: 10/10 consecutive runs pass
- `TestState` (the failing test): 3/3 consecutive runs pass (each taking ~5-6 minutes, running the full EEST state test suite)
- Both tests run together: pass
- `go vet` clean on both modified packages
