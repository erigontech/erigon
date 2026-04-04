---
name: erigon-cherry-pick
description: Erigon cherry pick PR's from one long-living git branch to another
allowed-tools: Bash, Read, Write, Edit, Glob, Git
---

Pick PR's from git branch `A` to `B`. Create separated PR's on github. Don't put your name into this PR's and don't sign
commits. Don't need much description - just refer original PR.

For example: from `release/3.4` to `main`