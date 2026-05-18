# Branch Naming Conventions

## Release branches

- `release/3.3` — Stable 3.3.x
- `release/3.4` — Stable 3.4.x (current)
- `main` — Next feature release (3.5)

## Developer branches (alex's convention)

Format: `alex/<short_desc>_<release_num>[_suffix]`

Release number is two digits without the dot: `34` = release/3.4, `35` = main, `33` = release/3.3.

Examples: `alex/seg_header_meta2_34`, `alex/integ_trie_root_35`, `alex/all7_34_dbg`

Suffixes: `_dbg` (debug), `_auto` (automated), `_<n>` (revision). Short descs use underscores.

## Choosing a base branch

- Bug for current stable release → base on `release/3.4`
- New feature → base on `main`
- Backport / cherry-pick → branch off target release branch, prefix PR title with `[rX.Y]`
