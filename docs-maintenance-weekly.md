# Erigon Docs — Weekly Maintenance Procedure

This document describes the weekly procedure for keeping the [docs.erigon.tech](https://docs.erigon.tech) documentation up to date with the erigontech/erigon repository.

The routine runs every **Tuesday at 9:00 AM** and produces a report of what needs to be updated, followed by a PR if changes are required.

---

## Overview

The procedure consists of 4 steps:

1. **Review merged PRs** from the past 7 days for documentation-relevant changes
2. **Verify findings** against the live docs site
3. **Check the `#Options` section** against the latest stable release
4. **Check for broken links** on docs.erigon.tech

---

## Step 1 — Review merged PRs (last 7 days)

### Fetch merged PRs

```bash
gh pr list --repo erigontech/erigon --state merged --limit 100 \
  --json number,title,body,mergedAt,url
```

Filter for PRs merged in the last 7 days.

### What to look for

For each PR that looks relevant, inspect it with:

```bash
gh pr view <NUMBER> --repo erigontech/erigon --json title,body,files
```

**Include** PRs that introduce:
- New or removed CLI flags
- New commands or subcommands
- New ports or network interfaces
- New environment variables
- Changed default values for existing flags
- Breaking changes visible to the user
- Behaviour changes that affect node operators

**Exclude** PRs that are:
- Internal bug fixes, tests, CI/CD, refactoring
- Changes to `eth_*` APIs (documented externally at [ethereum.github.io](https://ethereum.github.io/execution-apis))
- Fork activation dates for specific networks
- New EIP implementations

### Reading flag names correctly

When a PR adds or modifies a CLI flag, **always read `flags.go`** (or the relevant `flags.go` file in the PR diff) to get the exact flag name. Do not rely on the PR title — it is often informal (e.g. a PR titled "add off-mcp flag" may actually introduce `--mcp.disable`).

```bash
gh pr view <NUMBER> --repo erigontech/erigon --json files
# then read the flags.go file in the diff
```

### New RPC namespaces

If a PR introduces a new RPC namespace (e.g. `testing_*`, `kimiko_*`), **note it in the report** but do not create documentation for it unless explicitly requested.

---

## Step 2 — Verify findings against docs.erigon.tech

For each candidate change identified in Step 1, visit the relevant page on docs.erigon.tech to confirm the change is **not already documented**. Only include items in the report if they are genuinely missing or incorrect in the live docs.

Key pages to check:
- [fundamentals/configuring-erigon](https://docs.erigon.tech/fundamentals/configuring-erigon)
- [fundamentals/logs](https://docs.erigon.tech/fundamentals/logs)
- [fundamentals/security](https://docs.erigon.tech/fundamentals/security)
- [fundamentals/mcp](https://docs.erigon.tech/fundamentals/mcp)
- [fundamentals/default-ports](https://docs.erigon.tech/fundamentals/default-ports)

---

## Step 3 — Check the `#Options` section

The `#Options` section of `fundamentals/configuring-erigon` must reflect the **exact output** of `erigon --help` from the latest stable release.

### Find the latest stable release

```bash
curl -s https://api.github.com/repos/erigontech/erigon/releases/latest \
  | python3 -c "import sys,json; r=json.load(sys.stdin); print(r['tag_name'])"
```

### Download and run the binary

```bash
# Find the Linux amd64 asset URL
curl -s https://api.github.com/repos/erigontech/erigon/releases/tags/<TAG> \
  | python3 -c "
import sys,json
for a in json.load(sys.stdin)['assets']:
    if 'linux' in a['name'] and 'amd64' in a['name'] and a['name'].endswith('.tar.gz'):
        print(a['browser_download_url'])
" | head -1

# Download, extract and run
curl -L -o erigon.tar.gz <URL>
tar xzf erigon.tar.gz
chmod +x erigon
./erigon --help
```

Alternatively, if the repo is already cloned locally:

```bash
git fetch --tags
git checkout <TAG>
make erigon
./build/bin/erigon --help
```

### Update the docs

The output of `--help` must be placed verbatim in:

```
docs/gitbook/src/fundamentals/configuring-erigon/README.md
```

inside the existing Gitbook code block, after the line `./build/bin/erigon --help`:

```markdown
{% code overflow="wrap" fullWidth="true" %}
```
<exact output of ./build/bin/erigon --help>
```
{% endcode %}
```

**Do not reformat, summarize or convert to a table.** Copy the output as-is. Do not touch any Gitbook tags (`{% code %}`, `{% endcode %}`, `{% embed %}`).

---

## Step 4 — Check for broken links

### External links

For each page on docs.erigon.tech, check that all external links return a valid HTTP response (2xx or 3xx).

Known categories of links that may break:
- Links to third-party documentation (Lighthouse, Prysm, Otterscan, etc.) — these projects occasionally move or restructure their docs
- Links to specific GitHub file paths at fixed commits

### Internal links

Check all internal links (links pointing to other pages within docs.erigon.tech), including anchor links (e.g. `/fundamentals/configuring-erigon#options`).

> **Note:** The v2 legacy section (`/erigon/v2/`) is not actively maintained. Broken links found there do not require action.

### Automated check (recommended)

```bash
# Using lychee (fast link checker)
lychee --no-progress --format markdown https://docs.erigon.tech

# Or using linkchecker
linkchecker https://docs.erigon.tech
```

---

## Step 5 — Report and PR

### Weekly report format

Send a report with the following sections:

**A) Documentation changes needed**
For each item: exact flag/feature name, affected docs page, description of the change, reference PR.

**B) New RPC namespaces to note**
List only — no action required unless explicitly requested.

**C) `#Options` section**
State whether it is up to date or list flags that are missing/changed/removed.

**D) Broken links**
List broken links with: URL, page where it was found, suggested replacement.

**E) Recommendation**
Open a documentation PR: yes/no and why.

---

## Opening a PR

When changes are needed, open a PR against the `main` branch of `erigontech/erigon`.

### Setup

```bash
git clone https://github.com/erigontech/erigon.git
cd erigon
git checkout -b docs/MONTH-YEAR-wWW-documentation-update
# e.g. docs/march-2026-w11-documentation-update
```

### PR title convention

```
docs: <Month> <Year> w<WW> documentation update
```

Example: `docs: March 2026 w11 documentation update`

### PR body template

```markdown
## Summary

- <bullet point per change>

## Related PRs
- #XXXXX — description

🤖 Generated with [Claude Code](https://claude.com/claude-code)
```

### Commit and push

```bash
git add <changed files>
git commit -m "docs: <description of change>"
git push origin <branch-name>

# Create PR via GitHub API or gh CLI
gh pr create --repo erigontech/erigon \
  --title "docs: March 2026 w11 documentation update" \
  --body "..."
```

---

## Key files reference

| What | Path in repo |
|------|-------------|
| CLI flag definitions | `cmd/utils/flags.go` |
| Configuring Erigon / #Options | `docs/gitbook/src/fundamentals/configuring-erigon/README.md` |
| Log documentation | `docs/gitbook/src/fundamentals/logs.md` |
| Security documentation | `docs/gitbook/src/fundamentals/security.md` |
| MCP documentation | `docs/gitbook/src/fundamentals/mcp.md` |
| Default ports | `docs/gitbook/src/fundamentals/default-ports.md` |
| OTS API | `docs/gitbook/src/interacting-with-erigon/ots.md` |
| Staking / external CL | `docs/gitbook/src/staking/external-consensus-client-as-validator.md` |

---

## What NOT to document

| Topic | Reason |
|-------|--------|
| `eth_*` API methods | Documented externally at [ethereum.github.io](https://ethereum.github.io/execution-apis) |
| Fork activation dates | Managed separately, not in the main docs |
| EIP support | Not documented in the user-facing docs |
| v2 legacy section | Not actively maintained |
| New RPC namespaces | Noted in report only, not documented unless requested |

---

## Changelog

| Date | What changed |
|------|-------------|
| 2026-03-12 w11 | First PR: removed `(experimental)` from `--prune.include-commitment-history`, log dir permissions, MCP binary in releases, `#Options` updated to v3.3.9, typo fixes, broken external links fixed (Lighthouse, Otterscan, Prysm). PR [#19859](https://github.com/erigontech/erigon/pull/19859) |
