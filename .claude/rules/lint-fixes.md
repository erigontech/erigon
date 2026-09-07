# Lint Fix Reference

Common `make lint` categories and fixes:

- **ruleguard (defer tx.Rollback/cursor.Close):** Error check must come *before* `defer tx.Rollback()`. Never remove an explicit `.Close()` or `.Rollback()` — add `defer` as a safety net alongside it.
- **prealloc:** Pre-allocate slices when length is known from a range.
- **unslice:** Remove redundant `[:]` on variables already slices.
- **newDeref:** Replace `*new(T)` with `T{}`.
- **appendCombine:** Combine consecutive `append` calls into one.
- **rangeExprCopy:** Use `&x` in `range` to avoid copying large arrays.
- **dupArg:** For intentional `x.Equal(x)` self-equality tests, suppress with `//nolint:gocritic`.
- **Loop ruleguard in benchmarks:** For `BeginRw`/`BeginRo` inside loops where `defer` doesn't apply, suppress with `//nolint:gocritic`.

A finding printed with a path outside the repo (e.g. `../erigon.worktrees/<name>/...`) is usually the repo's own file: golangci-lint's cache is shared across git worktrees and keyed by file content, so it can replay an issue under a sibling worktree's path. Such paths also escape path-based exclusions in `.golangci.yml`, so baselined findings can resurface. Remedy: `go tool golangci-lint cache clean` and re-run, or set `GOLANGCI_LINT_CACHE` to a per-worktree directory.
