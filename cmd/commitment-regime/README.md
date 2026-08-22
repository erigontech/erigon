# commitment-regime

Reports whether a commitment domain `.kv` file stores **plain** full keys or
**referenced** (shortened file-offset) keys in its branch values.

It reads every key/value pair in order and stops at the first branch that
carries a shortened key:

- `referenced` — at least one shortened key found (prints the pair index where).
- `plain` — every pair scanned, all keys are full account (20B) / storage (52B) keys.

A malformed branch or corrupt word stream is reported conservatively as
`referenced`. Files are opened read-only, so a running node need not be stopped.

## Run

```sh
go run ./cmd/commitment-regime <path/to/vX.Y-commitment.A-B.kv> [more.kv ...]
```

or build once and point it at a datadir:

```sh
go build -o /tmp/commitment-regime ./cmd/commitment-regime
/tmp/commitment-regime <datadir>/snapshots/domain/*commitment*.kv
```

Example output:

```
v1.1-commitment.0-512.kv      referenced (first shortened key at pair 4 of 4)
v2.1-commitment.576-640.kv    plain (scanned all 57763381 pairs)
```
