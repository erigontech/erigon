# Task: Gather trie tumblers/config into single TrieConfig struct (issue #20553)

## Objective
Audit `execution/commitment/` and consolidate all scattered tumbler variables,
feature flags, constants and knobs into a single `TrieConfig` struct in a new `config.go`.

## Scope
- metrics toggles
- memoization switches
- key referencing params
- deferred hash derivation flags
- deferred branch write settings

## Goal
Clean, documented config surface passed into `HexPatriciaHashed` and related types.
Migration path for existing callers included.

## Branch
`awskii/commitment-config`

## Steps
1. Audit `execution/commitment/` — find all scattered config knobs:
   - fields set via setter methods (SetTrace, EnableWarmupCache, etc.)
   - package-level vars / constants used as tunables
   - constructor params that are really config
   - flags threaded through multiple layers

2. Design `TrieConfig` struct in new `config.go`:
   - Group into logical sections (metrics, memoization, deferred, etc.)
   - Sensible zero-value defaults where possible
   - Document each field

3. Wire `TrieConfig` into `HexPatriciaHashed` and callers:
   - Replace setter methods with config fields
   - Update `NewHexPatriciaHashed` / factory functions
   - Migration path: keep setters as deprecated shims if needed for callers outside the package

4. Write/update tests to exercise config paths

5. Run: `go test ./execution/commitment/... -count=1`

## Output
- `execution/commitment/config.go` — new file with TrieConfig struct
- Modified `hex_patricia_hashed.go`, `commitment.go`, etc.
- Plan saved to `docs/plans/YYYYMMDD-commitment-config.md`
