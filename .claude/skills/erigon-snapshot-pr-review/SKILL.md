---
name: erigon-snapshot-pr-review
description: Triage all open PRs on erigontech/erigon-snapshot. Auto-approve and squash-merge routine automation PRs that only extend the step range (new snapshots, or smaller ranges merged into bigger ones) with no gaps and no hash changes on existing files. Report anything else as unusual, unmerged. Use when asked to review, triage, or clear out open erigon-snapshot PRs.
argument-hint: "[--dry-run] [PR numbers...]"
allowed-tools: Bash
---

# erigon-snapshot PR Review

Sweep the open PRs on `erigontech/erigon-snapshot` (a separate repo from `erigon` — everything below uses `--repo erigontech/erigon-snapshot`). For each PR, decide between two outcomes only:

- **Routine → approve + squash-merge.** The PR only extends coverage (new snapshot files, or several smaller ranges merged into one bigger one) with no gaps and no changed hashes on files that already existed.
- **Unusual → leave alone, report it.** Anything that doesn't cleanly fit the routine case.

There is no partial-credit path — if a PR is ambiguous or the checks below can't be run cleanly, it goes in the unusual bucket.

If `$ARGUMENTS` contains `--dry-run`, do every check below but skip the actual `gh pr review --approve` / `gh pr merge` calls — report what *would* happen instead. If `$ARGUMENTS` lists specific PR numbers, restrict the sweep to those; otherwise sweep all open PRs.

## Step 0: Fetch the classifier script once

This repo's own `.claude/skills/summarize-changes/analyze_diff.py` already implements the exact classification needed (hash changes, range merges, version upgrades/downgrades, new-data frontier, unexpected deletions, version conflicts). Reuse it rather than re-deriving the logic. Pull a fresh copy at the start of the run (it lives in the target repo, not this one, so don't rely on a stale local copy):

```bash
gh api repos/erigontech/erigon-snapshot/contents/.claude/skills/summarize-changes/analyze_diff.py \
  -H "Accept: application/vnd.github.raw" > /tmp/erigon-snapshot-review/analyze_diff.py
```

(create `/tmp/erigon-snapshot-review/` first if needed; use the session scratchpad dir instead of `/tmp` if the environment gives you one).

## Step 1: List open PRs

```bash
gh pr list --repo erigontech/erigon-snapshot --state open --draft=false \
  --json number,title,url,author,labels,headRefName,isDraft,createdAt \
  --limit 100
```

Sort by `number` ascending and process oldest first — merging one PR can change the mergeability of a later one touching the same `.toml` file, so always re-check state immediately before acting on a given PR rather than trusting the list snapshot.

If there are no open PRs, report that and stop.

## Step 2: Per-PR automation gate

Before any diff analysis, a PR must satisfy **all** of these or it goes straight to the unusual bucket with reason "not a routine automation PR":

1. Title starts with `[automation]`.
2. Has the `automation` label.
3. Author is the snapshot-automation bot.
4. The PR's changed-files list (`gh pr diff <n> --repo erigontech/erigon-snapshot --name-only`) is **exactly one file**, a `<chain>.toml` at repo root (e.g. `mainnet.toml`, `chiado.toml`). Any PR touching `go.mod`, `embed.go`, a workflow file, docs, or more than one toml file is unusual by definition — routine snapshot updates touch a single chain's toml.

## Step 3: Diff classification

For PRs that pass Step 2, reproduce what `summarize-changes` does to gather inputs, then classify:

```bash
gh pr diff <n> --repo erigontech/erigon-snapshot > /tmp/erigon-snapshot-review/pr_<n>_diff.txt

HEAD_REF=$(gh pr view <n> --repo erigontech/erigon-snapshot --json headRefOid -q .headRefOid)
TOML_FILE=$(gh pr diff <n> --repo erigontech/erigon-snapshot --name-only)
gh api --method GET "repos/erigontech/erigon-snapshot/contents/$TOML_FILE" \
  -H "Accept: application/vnd.github.raw" -f ref="$HEAD_REF" > /tmp/erigon-snapshot-review/pr_<n>_toml.txt

python3 /tmp/erigon-snapshot-review/analyze_diff.py \
  /tmp/erigon-snapshot-review/pr_<n>_diff.txt /tmp/erigon-snapshot-review/pr_<n>_toml.txt
```

Read the `=== TOTALS ===` line and the per-section counts. Trust the script's numbers over any visual impression of the diff — do not eyeball hash strings yourself.

## Step 4: Mergeability + CI gate

```bash
gh pr view <n> --repo erigontech/erigon-snapshot \
  --json mergeable,mergeStateStatus,statusCheckRollup,reviewDecision
```

Required for the routine path:
- `mergeable` is `MERGEABLE` (not `CONFLICTING`/`UNKNOWN` — if `UNKNOWN`, re-poll once after a few seconds since GitHub computes it lazily; if still `UNKNOWN`, treat as unusual rather than guessing).
- Every entry in `statusCheckRollup` has `conclusion: SUCCESS` (the `CI` / `tests` check in particular). Any `FAILURE`, `IN_PROGRESS`, `QUEUED`, or missing check → unusual, not merged. Do not wait around for pending CI; just report it as pending and move to the next PR.

## Step 5: Decide

Merge (routine) only if **all** of these hold:
- Step 2 automation gate passed.
- From Step 3: `hash_changes` count is 0, `unexpected` (deletions) count is 0, `version conflicts` count is 0, and `VERSION DOWNGRADES` count is 0.
- From Step 4: mergeable and CI green.

Everything else → unusual, unmerged, with the specific reason(s) carried into the final report (which check failed, and the concrete numbers/files behind it — not just "looked wrong").

Note what "routine" *does* include, since these are expected and not blockers: range merges (several old ranges replaced by one bigger range, same hash lineage), version upgrades (e.g. v1.1 → v1.2, not a downgrade), and new frontier files extending the range past the previous top block. That's exactly the "step range extended, no gaps, no hash changes on existing files" case from the task.

## Step 6: Act

For a routine PR (skip both calls if `--dry-run`):

```bash
gh pr review <n> --repo erigontech/erigon-snapshot --approve \
  --body "Routine snapshot update: range extended, no gaps, no hash changes on existing files. Auto-approved."

gh pr merge <n> --repo erigontech/erigon-snapshot --squash --delete-branch
```

(the repo only allows squash merges — don't pass `--merge` or `--rebase`.)

If `gh pr merge` fails for any reason (race with another merge, branch protection, etc.), do not retry blindly — move it to the unusual bucket with the error message.

For an unusual PR: take no action on GitHub. Just record it for the report.

## Step 7: Report

After sweeping all PRs (or the ones in `$ARGUMENTS`), report back in this shape:

```
### erigon-snapshot PR review

✅ Merged (N)
- #1507 [automation] Updated v34 mainnet.toml up to 25.974M

🚨 Needs attention (N)
- #1234 <title> — <url>
  reason: <specific reason, e.g. "2 hash changes on existing files: domain/v2.0-accounts.0-32.kv, ...">
```

If run with `--dry-run`, label the first section "Would merge" instead of "Merged". If nothing is open or nothing needed action, say so plainly instead of printing empty sections.
