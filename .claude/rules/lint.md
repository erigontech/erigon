# Lint

Always run `make lint` after making code changes and before committing. Fix any linter errors before proceeding.

The linter is non-deterministic in which files it scans — new issues may appear on subsequent runs. Run lint repeatedly until clean.

Common lint categories and fixes:
- **ruleguard (defer tx.Rollback/cursor.Close):** The error check must come *before* `defer tx.Rollback()`. Never remove an explicit `.Close()` or `.Rollback()` — add `defer` as a safety net alongside it, since the timing of the explicit call may matter.
- **prealloc:** Pre-allocate slices when the length is known from a range.
- **unslice:** Remove redundant `[:]` on variables that are already slices.
- **newDeref:** Replace `*new(T)` with `T{}`.
- **appendCombine:** Combine consecutive `append` calls into one.
- **rangeExprCopy:** Use `&x` in `range` to avoid copying large arrays.
- **dupArg:** For intentional `x.Equal(x)` self-equality tests, suppress with `//nolint:gocritic`.
- **Loop ruleguard in benchmarks:** For `BeginRw`/`BeginRo` inside loops where `defer` doesn't apply, suppress with `//nolint:gocritic`.
