# Publisher restart chain.toml stuck in .part state

Bug surfaced 2026-05-04 during §5c testing on hoodi. Publisher-side
restart-correctness issue affecting the bootstrap-publisher role.

## Symptom

After a publisher restart with an existing `chain.toml.torrent` on
disk but no `chain.toml` file (or a chain.toml whose content
disagrees with the torrent), the torrent client picks up the
torrent metadata, starts downloading from peers, writes incomplete
content to `chain.toml.part`, and never finalises the file.
The publisher logs "[chaintoml] re-published existing chain.toml
ENR entry" but the published infohash points at content the
publisher itself doesn't fully have.

Downstream effect: V2 consumer nodes connecting to the publisher
see the chain.toml ENR entry, but their torrent fetch never
completes (publisher cannot seed bytes it doesn't have);
manifestReady times out at 5 min and the consumer falls back to
preverified.

## Sequence of events that triggers it

1. Publisher run #1 generates `chain.toml` from inventory and
   `chain.toml.torrent` from that file. Successfully serves both.
2. Publisher run #1 ends; publisher restarted later.
3. Between runs, `chain.toml` is deleted (or replaced by content
   that doesn't match the .torrent). On the test rig this happened
   when a peer's old chain.toml content was being re-downloaded
   into the publisher's `chain.toml.part`.
4. Publisher run #2 starts. The downloader's startup path adds
   `chain.toml.torrent` to the torrent client (via either
   `seedChainTomlTorrent` or general torrent-client init). The
   torrent client sees no matching local file → starts a download.
5. The 30-second-delayed `PublishLocalChainToml` goroutine
   eventually runs `PublishChainToml` and saves a fresh chain.toml.
   But the torrent client still holds the OLD .torrent's infohash
   and is mid-download into `.part`.
6. The freshly-saved chain.toml has different content than the old
   .torrent's hash → torrent client renames it back to `.part` and
   continues trying to download.

The window between (4) and (5) — about 30 s — is where the conflict
takes root. Once the torrent client is in `.part` state, it stays
there.

## Reproduction

  1. Run a publisher with `--snap.bootstrap-from-preverified` until
     it has chain.toml + chain.toml.torrent on disk.
  2. Stop the publisher.
  3. Delete `chain.toml` (keep the .torrent).
  4. Restart the publisher.
  5. Observe: `chain.toml.part` is created, never finalises.

The simpler reproduction is:
  1. Stop publisher.
  2. Delete `chain.toml` AND `chain.toml.torrent`.
  3. Restart.
  4. Observe: fresh files are produced cleanly. (Workaround.)

## Fix candidates

### Option A — synchronous publish before torrent client init

`PublishLocalChainToml` runs at line backend.go:586 inside a 30-s
goroutine. Move it to run *synchronously before* the torrent client
is set up to seed/recheck existing torrents on disk. Then the
torrent client always sees a fresh chain.toml + chain.toml.torrent
pair that match.

  - Trade-off: blocking startup on chain.toml regeneration. Should
    be fast (single TOML serialise + torrent build) but adds a
    sync point.

### Option B — explicit cleanup before publish

Before calling `PublishChainToml`, the downloader removes
`chain.toml.part` and any existing `chain.toml.torrent` if the
local chain.toml file is missing or mismatched. This forces the
torrent client to start from a known-clean state.

  - Trade-off: deletion of a `.torrent` orphan means peers' ENR
    references the old infohash for a few seconds until the new
    one publishes. Acceptable.

### Option C — torrent client treats missing file as "not yet generated"

Teach the torrent client (or its caller) that for chain.toml
specifically, missing local content means "publisher will generate
it shortly", not "download from peers". Skip `t.DownloadAll()` for
chain.toml when this node is a publisher
(`--snap.bootstrap-from-preverified`).

  - Trade-off: special-cases chain.toml in the torrent client
    layer. Couples the layers. Less clean than B.

### Recommendation

Option B is the smallest-blast-radius fix: clean up stale state in
the publisher's `PublishLocalChainToml` path before saving the new
file. ~5 lines of os.Remove. No layering crossings, no startup
blocking change.

Option A is the more robust long-term fix because it eliminates
the race window entirely — but it touches startup ordering which is
a riskier change.

## Severity

Medium-high for **public publishers** at PR landing time. Erigon
Tech's snapshotter restarts will hit this if it ever ends up with
chain.toml missing. Mitigations until fix lands:

  - Operators document the workaround in the publisher runbook
    (delete chain.toml.* on stop / before restart).
  - The 30-second goroutine eventually stabilises in many cases
    (the second invocation may be enough to overwrite); not
    guaranteed.

Low for **consumer nodes** — they don't generate chain.toml; this
bug only affects bootstrap publishers.

## Sequencing

Lands as a small follow-up commit/PR. Not blocking the V2
delivery PR — the workaround is operational and the bug only
surfaces under restart with stale state. New publishers come up
clean.
