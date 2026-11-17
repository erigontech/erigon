# Erigon Agent Guidelines

Use B2-level English in code, code-comments, github. Use programming terminology freely and exactly

## Ponytail

Use [Ponytail](https://github.com/DietrichGebert/ponytail) for coding tasks to keep changes simple.

- Reuse an existing installation. Otherwise, follow the upstream [installation instructions](https://github.com/DietrichGebert/ponytail#install) and [portability guide](https://github.com/DietrichGebert/ponytail/blob/main/docs/agent-portability.md) for your agent and version. Prefer user-local setup, complete prerequisites and activation steps, and verify it is loaded.
- If native integration is unavailable, clone Ponytail outside the Erigon checkout and read `skills/ponytail/SKILL.md` from that checkout at the start of each coding session. Preserve existing agent configuration and this `AGENTS.md`.
- Before handing off code changes, invoke `ponytail-review` using your agent's syntax, or read and apply `skills/ponytail-review/SKILL.md` from the Ponytail checkout. Review the current diff for: delete (dead code/speculative feature), stdlib (reinvented standard library), native (dependency doing what the platform does), yagni (abstraction with one implementation), shrink (same logic, fewer lines), reuse (duplicates a helper already in this repo), bugs, security, load, tests, speed, bloat

Explicit task requirements and Erigon's guidelines take precedence, including TDD, test coverage, and comment rules. Keep downloaded files and personal settings out of commits.

Build: `make erigon`. Regenerate code: `make gen`.

### Local verification

Before committing, pushing, or opening/updating a PR, select checks for the change:

- **Go changes:** run relevant tests and full configured lint on affected packages and callers
- **Imports or dependencies:** also run `go mod tidy -diff` in each affected module and resolve any reported changes
- **Command wiring, build configuration, native code, or runtime testing:** build the affected executable with its Makefile target
- **Broad changes or linter configuration/rules:** run `make lintci` for full repository lint and the module check
- **Documentation-only changes (no `.go` files):** check the diff and relevant links or document tooling; Go lint and builds are not required.

Example targets for the checks above: `make lint`, `make test-short`. Run affected long tests without `-short`.

## Architecture Overview

- Erigon is an Ethereum execution client
- Data flow: `db -> snapshots`
- `snapshots` are immutable
- `Unwind` beyond data in snapshots not allowed

## Key Directories

| Directory | Purpose | Component Docs |
|-----------|---------|----------------|
| `cmd/` | Entry points: erigon, rpcdaemon, caplin, sentry, downloader | - |
| `execution/commitment/` | Sequential and parallel commitment tries | [parallel-patricia-hashed.md](docs/design/parallel-patricia-hashed.md) |
| `execution/stagedsync/` | Staged sync pipeline | [agents.md](execution/stagedsync/agents.md) |
| `db/` | Storage: MDBX, snapshots, ETL | [agents.md](db/agents.md) |
| `cl/` | Consensus layer (Caplin) | [agents.md](cl/agents.md) |
| `p2p/` | P2P networking (DevP2P) | [agents.md](p2p/agents.md) |
| `rpc/jsonrpc/` | JSON-RPC API | - |

## Running

```bash
./build/bin/erigon --datadir=./data --chain=mainnet
./build/bin/erigon --datadir=dev --chain=dev  # PoS dev mode
```

## Test-Driven Development

When fixing bugs or adding new features, follow the test-driven development (TDD) cycle: **Red → Green → Refactor**.

1. **Red** — write a failing test that specifies the desired behavior. Confirm it fails *for the right reason* (the behavior is missing or wrong), not because of a typo, missing import, or wrong setup.
2. **Green** — write the minimum production code needed to make the test pass. Do not write code the current failing test does not demand.
3. **Refactor** — clean up code and tests with the suite staying green. Skipping this step is how technical debt accumulates.

Before adding a test: Inspect the relevant existing tests - extend them 

Before changing a bound or constant: a deliberate tolerance and an off-by-one are indistinguishable at the call site. Before changing a bound, threshold or rounding expression, read the code that *produces* the value being compared against and confirm the intended relationship.

### For bug fixes

Reproduce the bug as a failing test **before** touching the fix. This proves three things at once: (a) the bug exists, (b) the agent/contributor understands it, and (c) the fix actually addresses it — the test flips `red → green` when the fix lands.

### For new features

- Drive the API shape from how the test wants to call it (outside-in)
- Start with the simplest meaningful behavior
- Add edge cases as separate `Red → Green` cycle

### When pragmatism applies

- Pure refactors with no behavior change — existing tests are the safety net; do not write new tests just to satisfy the cycle.
- Exploratory spikes — throw the spike away and TDD the real implementation.
- Mechanical, trivial changes. Feature removals

## Test skips

A failing test is a real failure to be diagnosed and fixed. Skipping it converts a loud "this is broken" signal into silence, then into a later surprise for whoever removes the skip and finds the bug still there.

**Automated agents must never, on their own initiative, add a skip to hide unsupported behavior, a failure, or a flake** — not with a "the user can review it" framing, not as an `AskUserQuestion` option, not as a "tactical unblock" in a text answer, not behind a conditional or env-var gate

When an agent encounters a failing test:
- Investigate: read logs, reproduce locally, narrow to a minimal repro
- Fix the underlying bug
- If genuinely unfixable in-session: (a) escalate with the findings, or (b) report a tracked blocker — never (c) skip it

Removing tests from CI matrix: require own PR with clear reasons

### Valid reasons a skip may exist

1. **External test suites we import where we can't pass all the tests** — usually a feature we haven't implemented yet (e.g. an upstream Ethereum spec test). The skip documents the gap rather than hiding a regression.

2. **Flaky tests** — partially valid, with very low (not zero) tolerance. The rule is **reproduce locally and fix**; a skip is a last resort only after serious root-cause analysis (not "I ran it three times and it passed"), with that investigation attached to the tracking issue. 

3. **Long-running tests excluded from short mode** — a test too slow or resource-heavy for the quick `-short` suite may use the canonical guard `if testing.Short() { t.Skip("...") }` (message such as `"long-running test"`). It must still run in the full (non-`-short`) suite, the guard must depend only on `testing.Short()`. Never add the guard in response to a failure, hang, or flake.

For the first two reasons, the skip carries an inline comment linking the tracking issue, which is closed by removing the skip — not by closing it with the skip still in place.

## Conventions

Commit messages: prefix with package(s) modified: `eth, rpc: make trace configs optional`

Don't sign commits, pr's, issues, comments.

Follow [Local verification](#local-verification) before commits, pushes, and PR updates.

## Code Style

### Comments

**Default: no comment.** Clear names and small focused functions read on their own. Before adding a comment, ask whether renaming, extracting a helper, or restructuring would remove the need

A comment may be warranted for: a non-obvious invariant the types don't enforce; a workaround for a bug in a dependency or the runtime (link the issue/commit); a surprising edge case a reader would otherwise miss; a performance-sensitive choice where the obvious implementation would be wrong.

- Concise, but err on the side of clarity: cut comments a sentence or two - if it doesn't harm clarity
- High-level, not scenario-specific: explain the invariant or gotcha in general terms
- Free of forensic detail: Strip dates, devnet/branch names, PR/issue/review references, incident anecdotes, callsite lists, scope or limitation narration (`// forward-only`, `// safety net`). That history belongs in the commit message and PR description
- Stated once: "The Why" lives at its canonical place — the field, type or function it belongs to; other sites refer to the source of truth
- Not a restatement of the code

## Code Reviews

Keep review feedback focused by PR scope - real unrelated bugs must be fixed in follow-up PR. Do not require unrelated pre-existing issues to be fixed in the same change 

### Pull Request scope

Keep each PR focused on one coherent unit of work. Judge scope by the concepts changed, not only by the number of lines or files

Large change: use stacked PRs

Benchmarks: keep in `xyz_bench_test.go` file instead of `xyz_test.go`

To refer to a numbered list item, point, step, or nit in PR descriptions, issue descriptions, or comments, write it in words — "point 1", "item 1", "the first nit". Use `#N` only as a reference to a GitHub issue or PR number; GitHub auto-links it, so a bare `#1` would point at an unrelated PR.

## Manual Workflows

When manually dispatching a workflow that is not part of the PR's automatic check list, add a comment on the PR explaining which workflow was dispatched, why it was chosen, and include a direct link to the workflow run.

### Backport PRs to release branches

When opening a PR against `release/3.4` (or another release branch):

- **Title**: `[r3.4] db/version: enforce upper-bound file version check`
- **Body**: keep it short: `Cherry-pick of #<N> to release/3.4.` When release-branch adjustments were needed (different APIs, skipped tests, etc.), add a `## r3.4-specific adaptations` section listing the deltas
- **Branch name**: `cp/<PR#>-to-3.4`
- **Base branch**: `release/3.4`.

## Workflows

Make sure all scripts and shell code used from GitHub workflows is cross platform, for macOS, Windows and Linux.

Read [`CI-GUIDELINES.md`](CI-GUIDELINES.md) for guidelines before making changes to workflows.

## Go Test Caching

**When a test reads a data file at runtime** (via `os.Open`, `os.ReadFile`, `os.Stat`, etc.) that lives outside a `testdata/` directory, it must be added to the `git restore-mtime` pattern list in `setup-erigon/action.yml`. Otherwise that package's test results will never be cached in CI.
