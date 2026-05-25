# Branch Naming Conventions

## Release branches

- `release/3.3` — Stable 3.3.x
- `release/3.4` — Stable 3.4.x (current)
- `main` — Next feature release (3.5)

## Developer branches

The repo-wide convention is `<github-username>/<short-desc>`. Case style of the description varies by individual — kebab-case (`alice-bob/fix-foo-bar`) and snake_case (`alice_bob/fix_foo_bar`) are both in use. Pick one and stay consistent within your own branches.

Examples seen on `origin`:

- `yperbasis/overlay-flush-append`
- `mh/parallel-exec-heuristic-removal`
- `lupin012/fix_vrs_null_mainnet`
- `awskii/eest-witness-newmain`

When creating a branch for someone else, sample their recent branches first (`git for-each-ref refs/remotes/origin --sort=-committerdate | grep <username>/`) and match their style — do not impose a different one.

### Optional variants

Some contributors append a release-number suffix to mark which release the branch targets — Alex's convention is `alex/<short_desc>_<release_num>[_suffix]` (e.g. `alex/seg_header_meta2_34`, `alex/all7_34_dbg`). Release number is two digits without the dot: `34` = release/3.4, `35` = main, `33` = release/3.3. Suffixes: `_dbg` (debug), `_auto` (automated), `_<n>` (revision). This is one contributor's personal scheme — do not apply it to others.

Non-personal prefixes that show up for cross-cutting work:

- `feature/<username>/<desc>` — feature-branch namespace
- `cp/<pr-number>-to-<target>` — cherry-pick branches (e.g. `cp/21328-to-main`)
- `ci-fix/<desc>`, `wip/<desc>` — purpose-prefixed when ownership is shared or unclear

## Choosing a base branch

- Bug for current stable release → base on `release/3.4`
- New feature → base on `main`
- Backport / cherry-pick → branch off the target release branch, prefix the PR title with `[rX.Y]` (dominant form `[r3.4]`)
