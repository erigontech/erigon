# Agent Task

This folder is being worked on by an automated agent.

## Specification

# Spec: Move commitment-prefix to snapshots subcommand

GitHub issue: https://github.com/erigontech/erigon/issues/14616

## Context
There's a standalone binary `cmd/commitment-prefix/main.go` (479 lines) that analyzes commitment domain .kv files — produces charts of branch data distribution by prefix depth. It was deleted from main but the issue asks to resurrect it as a subcommand under `erigon snapshots commitment-analysis`.

The original code is preserved at `cmd/commitment-prefix-original.go` in the working directory for reference.

## What To Do

### 1. Create the subcommand

Add `commitment-analysis` as a subcommand of `erigon snapshots` in `cmd/utils/app/snapshots_cmd.go`:

```go
{
    Name:  "commitment-analysis",
    Usage: "Analyze commitment domain branch data distribution by prefix depth",
    Action: func(c *cli.Context) error { ... },
    Flags: []cli.Flag{...},
}
```

### 2. Rewrite to use standard database/aggregator patterns

The original code opens .kv files directly via `seg.NewDecompressor`. Rewrite to:
- Use `--datadir` flag (standard pattern from other subcommands)
- Open database/aggregator in the same manner as other snapshots subcommands (look at `doLS`, `doRetire`, etc. for patterns)
- Find commitment domain .kv files through the aggregator, not manual filepath walking

### 3. Keep the analysis logic

The core analysis logic (prefix counting, depth analysis, chart generation) should be preserved from the original. Key functions to port:
- `proceedFiles` — concurrent file processing
- `analyzeBranches` — branch data parsing and prefix analysis  
- Chart generation via go-echarts (bar charts, heatmaps)
- State printing (`--state` flag)

### 4. CLI flags mapping

Original flags → new CLI flags:
- `-output` → `--output` (output directory for charts)
- `-j` → `--concurrency` (worker count)
- `-trie` → `--trie` (hex/bin variant)
- `-compression` → `--compression` (none/k/v/kv)
- `-state` → `--state` (print file state)
- `-depth` → `--depth` (prefix depth)
- Positional args (file paths) → either `--datadir` auto-discovery or positional

### 5. Delete old binary

Remove `cmd/commitment-prefix/` directory if it still exists (it was already deleted but ensure no references remain).

## Approach

- Read `cmd/commitment-prefix-original.go` for the full original implementation
- Read `cmd/utils/app/snapshots_cmd.go` for patterns of how other subcommands are structured
- Read how `doLS` or similar opens the database/aggregator
- Port the code, adapting to cli.Command patterns
- Go binary: `/usr/local/go/bin/go`
- Build: `go build ./cmd/erigon/...`
- Test: `go build ./cmd/...`

## What NOT To Do
- Don't change the analysis algorithm
- Don't remove go-echarts dependency
- Don't modify other subcommands
- Don't add new dependencies beyond what's already in go.mod


## Success Criteria (Objective)

# Objective: commitment-analysis subcommand

## Success Criteria

### Must Pass
1. `go build ./cmd/erigon/...` — compiles clean
2. `go build ./cmd/...` — all binaries compile
3. New subcommand visible: `erigon snapshots commitment-analysis --help` shows usage
4. All original functionality preserved (charts, state printing, depth analysis)
5. Uses --datadir pattern consistent with other snapshots subcommands
6. `cmd/commitment-prefix/` directory does not exist

### Quality Checks
- Follows existing code patterns in snapshots_cmd.go
- Database/aggregator opened same way as other subcommands
- CLI flags documented with usage strings
- No hardcoded paths
- Error handling consistent with codebase

### Code Quality
- No magic strings — use constants where appropriate
- Error handling: wrap errors with context
- Comments on non-obvious decisions
- Consistent naming with codebase conventions
- gofmt clean

### Verification Commands
```bash
/usr/local/go/bin/go build ./cmd/erigon/...
/usr/local/go/bin/go build ./cmd/...
```

### Red Flags (auto-fail)
- Changing existing subcommands
- Adding new dependencies not in go.mod
- Leaving cmd/commitment-prefix/ directory
- Breaking compilation of any existing binary


## Important Notes

- A **strict verifier agent** will independently check your work when you are done.
- The verifier has no access to your session — it only reads the actual files.
- Claims you make that are not backed by real file changes will be caught.
- Do not leave TODOs, stubs, or placeholder code. Every criterion must be fully met.
- Run tests / build commands to confirm your work is correct before finishing.
