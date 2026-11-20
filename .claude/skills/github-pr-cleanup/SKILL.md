---
name: github-pr-cleanup
description: Clean up local worktrees and branches for merged GitHub PRs, and optionally cherry-pick to release branches. Use this skill whenever the user gives you a GitHub PR link and asks to clean it up, close it out, remove its worktree, or handle post-merge tasks. Also use it when the user says things like "PR is merged, clean up", "remove worktree for this PR", or "handle cherry-pick for this PR".
allowed-tools: Bash
---

# GitHub PR Cleanup

Clean up local git state after a PR is merged, and handle cherry-pick instructions if present in the PR body.

## Input

The user provides a GitHub PR URL (e.g., `https://github.com/erigontech/erigon/pull/19785`). Extract the repo owner/name and PR number from it.

## Step 1: Check PR Status

```bash
gh pr view <number> --repo <owner>/<repo> --json state,mergedAt,mergeCommit,headRefName,baseRefName,title,body,number
```

If the PR is not in the `MERGED` state, tell the user "PR #<number> is not merged yet" and stop — do nothing else.

## Step 2: Identify the Local Worktree

The PR's `headRefName` is the branch used for the PR. Find the local worktree that tracks this branch:

```bash
git worktree list --porcelain
```

Parse the porcelain output to find the worktree whose `branch` field matches `refs/heads/<headRefName>`. If no matching worktree is found, skip worktree removal and tell the user (the branch may still exist without a worktree).

## Step 3: Remove the Worktree

If a matching worktree was found, remove it:

```bash
git worktree remove <worktree-path>
```

If it fails due to uncommitted changes, add `--force` and warn the user that uncommitted work was discarded.

Report the removed worktree path to the user.

## Step 4: Remove the Local Branch

Delete the local branch that the PR used:

```bash
git branch -D <headRefName>
```

Report the deleted branch name.

## Step 5: Fetch the Target Branch and Find the Squash Commit

The PR's `baseRefName` is the branch the PR was merged into (e.g., `main`). The `mergeCommit.oid` from the `gh pr view` output is the squash commit SHA.

Fetch the target branch to make sure we have the latest:

```bash
git fetch origin <baseRefName>
```

**YubiKey note:** This triggers an SSH operation — remind the user to press their YubiKey.

Verify the squash commit exists locally:

```bash
git cat-file -t <mergeCommit.oid>
```

Report the squash commit SHA and its one-line summary to the user.

## Step 6: Check for Cherry-Pick Instructions

Parse the PR `body` for cherry-pick instructions. Look for lines matching patterns like:

- `- [ ] Cherry-pick merge commit to <target>`
- `- [ ] Cherry-pick to <target>`
- `- [x] Cherry-pick merge commit to <target>` (already done — skip)

The target is typically a branch name in backticks, e.g., `` `release/3.4` ``.

If no cherry-pick instruction is found, or if all cherry-pick items are already checked (`[x]`), report that there's nothing more to do and stop.

If an unchecked cherry-pick instruction is found, proceed to Step 7.

## Step 7: Cherry-Pick to Target Branch

Given:
- `squash_commit` = the merge commit SHA from Step 5
- `cherry_pick_target` = the branch extracted from the cherry-pick instruction (e.g., `release/3.4`)
- `pr_number` = the original PR number
- `pr_title` = the original PR title

### 7a: Fetch the cherry-pick target branch

```bash
git fetch origin <cherry_pick_target>
```

**YubiKey note:** Remind the user to press their YubiKey.

### 7b: Create an ephemeral branch

The branch name format is `cherry-pick-PR<number>-to-<target-branch-name>` where slashes in the target are replaced with dashes:

```bash
# Example: cherry-pick-PR19785-to-release-3.4
git checkout -b cherry-pick-PR<number>-to-<sanitized-target> origin/<cherry_pick_target>
```

### 7c: Cherry-pick the squash commit

```bash
git cherry-pick <squash_commit>
```

If the cherry-pick has conflicts, stop and tell the user — do not attempt to resolve conflicts automatically.

### 7d: Push the ephemeral branch

```bash
git push -u origin cherry-pick-PR<number>-to-<sanitized-target>
```

**YubiKey note:** Remind the user to press their YubiKey.

### 7e: Create the cherry-pick PR

The PR title format prepends `[cherry-pick]` and the release tag to the original title.

For example, if the target is `release/3.4` and the original title is `snapcfg: load preverified hashes for single chain only`, the cherry-pick PR title becomes:
`[cherry-pick] [r3.4] snapcfg: load preverified hashes for single chain only`

The `[rX.Y]` tag is derived from the target branch name by stripping the `release/` prefix.

```bash
gh pr create \
  --base <cherry_pick_target> \
  --head cherry-pick-PR<number>-to-<sanitized-target> \
  --title "[cherry-pick] [r<version>] <original-title>" \
  --body "$(cat <<'EOF'
Cherry-pick of #<pr_number> (<squash_commit_short>) to <cherry_pick_target>.
EOF
)"
```

Report the new PR URL to the user.

### 7f: Update the original PR

Mark the cherry-pick task as done in the original PR body and reference the new cherry-pick PR. Use `gh pr edit` to update the body — replace the unchecked cherry-pick item:

```
- [ ] Cherry-pick merge commit to `release/3.4`
```

with a checked item referencing the new PR:

```
- [x] Cherry-pick merge commit to `release/3.4` — #<cherry-pick-pr-number>
```

```bash
gh pr edit <original_pr_number> --repo <owner>/<repo> --body "<updated-body>"
```

### 7g: Clean up the ephemeral branch locally

Switch back to the main worktree's branch and delete the ephemeral branch:

```bash
git checkout <previous-branch>
git branch -D cherry-pick-PR<number>-to-<sanitized-target>
```

## Summary

At the end, print a summary of everything that was done:

- Worktree removed: `<path>` (or "no worktree found")
- Branch deleted: `<headRefName>`
- Squash commit: `<short-sha> <one-line-summary>`
- Cherry-pick PR created: `<url>` (or "no cherry-pick needed")
- Original PR updated: yes/no
