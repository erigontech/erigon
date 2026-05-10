// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package downloader

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"sort"

	"github.com/erigontech/erigon/db/snaptype"
	snapshotinv "github.com/erigontech/erigon/node/components/storage/snapshot"
	"github.com/pelletier/go-toml/v2"
)

// ChainTomlVersion is the format version for chain.toml.
// V1 (implicit): flat key-value map of filename → torrent hash.
// V2 (explicit): versioned format with [blocks], [meta], [salt],
// [domains.*], [[caplin]] sections.
const ChainTomlV2Version = 2

// Wire-level kind names for DomainFileEntry.Kind. The on-disk format
// always uses these explicit strings; an empty Kind from an older
// publisher is normalized to KindKVName by the parser.
const (
	KindKVName      = "kv"
	KindHistoryName = "history"
	KindIdxName     = "idx"
)

// ChainTomlV2 is the structured representation of a V2 chain.toml manifest.
type ChainTomlV2 struct {
	Version int `toml:"version"`

	// UCANHash is the torrent infohash of the snapshotauth delegation
	// attestation paired with this manifest generation
	// (chain.ucan.<seq>.bin). Hex-encoded 20 bytes; empty when this
	// node is not running with attestation. Verifiers configured with
	// a non-nil TrustConfig MUST reject peers whose manifest carries
	// no UCANHash; trust-everyone deployments ignore the field.
	UCANHash string `toml:"ucan_hash,omitempty"`

	// Block snapshot files — deterministic, all nodes produce identical files.
	Blocks map[string]string `toml:"blocks,omitempty"`

	// Chain-config metadata files (erigondb.toml). Single flat map.
	Meta map[string]string `toml:"meta,omitempty"`

	// Hash-derivation salts (salt-blocks.txt, salt-state.txt). Separated
	// from meta because semantics differ — salts feed the deterministic
	// rebuild of accessors, meta is human-edited config.
	Salt map[string]string `toml:"salt,omitempty"`

	// Per-domain state snapshot sections (covers kv + history + idx kinds).
	Domains map[string]*DomainManifest `toml:"domains,omitempty"`

	// Caplin beacon archive files (caplin/v*.seg).
	Caplin []CaplinFileEntry `toml:"caplin,omitempty"`
}

// DomainManifest describes the available files for a single state domain.
// Files may be of mixed kind (kv, history, idx); coverage is computed
// from the kv kind only — that's the canonical primary that defines the
// domain's step coverage.
type DomainManifest struct {
	// Coverage is the [from, to) step range covered by kv files. Other
	// kinds align to the same ranges but coverage is reported on kv.
	Coverage [2]uint64 `toml:"coverage"`

	// Files lists the individual snapshot files with their step ranges,
	// kinds, hashes, and trust.
	Files []DomainFileEntry `toml:"files,omitempty"`
}

// DomainFileEntry is a single file in a domain manifest.
type DomainFileEntry struct {
	Name string `toml:"name"`
	// Range is the [from, to) step range this file covers.
	Range [2]uint64 `toml:"range"`
	// Kind is "kv" (default when empty for back-compat), "history" (.v),
	// or "idx" (.ef).
	Kind  string `toml:"kind,omitempty"`
	Hash  string `toml:"hash"`
	Trust string `toml:"trust"`

	// ProofRoot is the hex-encoded state-trie root recorded inside this
	// file (currently populated for commitment-domain primaries — the
	// `state` key carries this for every step boundary regardless of
	// pruning mode). Empty when the file records no root.
	//
	// Closes the cryptographic chain that ties state to blocks: a
	// consumer can verify ProofRoot against its own block header's
	// stateRoot for AtBlock — using only block headers, no snapshot
	// data — before downloading the file.
	ProofRoot string `toml:"proof_root,omitempty"`

	// AtBlock is the block whose execution produced ProofRoot. Together
	// with AtTxNum it positions the proof in the chain timeline.
	AtBlock uint64 `toml:"at_block,omitempty"`

	// AtTxNum is the txnum at which ProofRoot was recorded.
	AtTxNum uint64 `toml:"at_tx_num,omitempty"`

	// IsPartialBlock is true when AtTxNum < the block's max txnum — the
	// file's last record is mid-block. Consumers MUST NOT treat such
	// files as a valid snapshot-tip state. (Phase 5 gap-f wall lived
	// exactly here: a partial-block commitment loaded as if it were a
	// tip, intermediate txns of the boundary block unapplied, exec
	// failed at the first transaction with a stale account.)
	IsPartialBlock bool `toml:"is_partial_block,omitempty"`
}

// CaplinFileEntry is a single caplin beacon-archive file. Same shape as
// the (currently flat-map) Blocks section, promoted to a typed list so
// it can grow a Kind field if blob/state archives appear.
type CaplinFileEntry struct {
	Name  string `toml:"name"`
	Hash  string `toml:"hash"`
	Trust string `toml:"trust,omitempty"`
}

// GenerateV2 builds a V2 chain.toml from a snapshot inventory.
//
// Domain files are filtered by canonicity (power-of-2 aligned, see
// isCanonicalFile). Non-canonical files (merge backlog) are excluded —
// they'll appear in a future publication once merges catch up. The
// canonicity filter applies to every kind; a non-canonical .v won't be
// advertised even if the matching .kv is canonical.
//
// Block, caplin, meta, and salt files are emitted as-is (no canonicity
// filter — their alignment is implicit).
func GenerateV2(inv *snapshotinv.Inventory) *ChainTomlV2 {
	manifest := &ChainTomlV2{
		Version: ChainTomlV2Version,
		Blocks:  make(map[string]string),
		Meta:    make(map[string]string),
		Salt:    make(map[string]string),
		Domains: make(map[string]*DomainManifest),
	}

	// Block files.
	for _, f := range inv.BlockFiles() {
		if f.TorrentHash != [20]byte{} {
			manifest.Blocks[f.Name] = fmt.Sprintf("%x", f.TorrentHash)
		}
	}

	// Meta files (chain config).
	for _, f := range inv.MetaFiles() {
		if f.TorrentHash != [20]byte{} {
			manifest.Meta[f.Name] = fmt.Sprintf("%x", f.TorrentHash)
		}
	}

	// Salt files (hash-derivation seeds).
	for _, f := range inv.SaltFiles() {
		if f.TorrentHash != [20]byte{} {
			manifest.Salt[f.Name] = fmt.Sprintf("%x", f.TorrentHash)
		}
	}

	// Caplin beacon archive — sorted by name for deterministic output.
	caplin := inv.CaplinFiles()
	sort.Slice(caplin, func(i, j int) bool { return caplin[i].Name < caplin[j].Name })
	for _, f := range caplin {
		if f.TorrentHash == [20]byte{} {
			continue
		}
		manifest.Caplin = append(manifest.Caplin, CaplinFileEntry{
			Name:  f.Name,
			Hash:  fmt.Sprintf("%x", f.TorrentHash),
			Trust: f.Trust.String(),
		})
	}

	// Domain files — kv + history + idx, all canonical.
	for _, domain := range inv.Domains() {
		files := inv.LocalFiles(domain)
		if len(files) == 0 {
			continue
		}

		dm := &DomainManifest{}

		// Only include files at canonical boundaries with a torrent hash.
		// Coverage is computed from kv files only — that's the canonical
		// primary defining the domain's step coverage.
		for _, f := range files {
			r := f.Range()
			if !isCanonicalFile(r) {
				continue
			}
			if f.TorrentHash == [20]byte{} {
				continue // no torrent hash yet
			}
			// Always emit an explicit kind so the on-disk file is
			// unambiguous; the parser still tolerates an empty kind
			// from older publishers (defaults it to "kv").
			kind := string(f.Kind)
			if kind == "" {
				kind = KindKVName
			}
			entry := DomainFileEntry{
				Name:  f.Name,
				Range: [2]uint64{r.From, r.To},
				Kind:  kind,
				Hash:  fmt.Sprintf("%x", f.TorrentHash),
				Trust: f.Trust.String(),
			}
			// Carry the per-file step-header anchors when the validator
			// has populated them. Zero ProofRoot means no validator has
			// recorded a root for this file — emit nothing rather than
			// a bogus all-zero hash.
			if f.ProofRoot != ([32]byte{}) {
				entry.ProofRoot = fmt.Sprintf("%x", f.ProofRoot)
				entry.AtBlock = f.AtBlock
				entry.AtTxNum = f.AtTxNum
				entry.IsPartialBlock = f.IsPartialBlock
			}
			dm.Files = append(dm.Files, entry)
		}

		// Sort files deterministically: by range[0], then range[1], then kind.
		// Mixing kinds with overlapping ranges (kv + history + idx all on
		// [0, 128)) needs a tiebreak so the output is byte-stable.
		sort.Slice(dm.Files, func(i, j int) bool {
			a, b := dm.Files[i], dm.Files[j]
			if a.Range[0] != b.Range[0] {
				return a.Range[0] < b.Range[0]
			}
			if a.Range[1] != b.Range[1] {
				return a.Range[1] < b.Range[1]
			}
			return a.Kind < b.Kind
		})

		// Coverage from kv-kind entries only.
		var kvFiles []DomainFileEntry
		for _, f := range dm.Files {
			if f.Kind == "" || f.Kind == KindKVName {
				kvFiles = append(kvFiles, f)
			}
		}
		if len(kvFiles) > 0 {
			dm.Coverage = [2]uint64{
				kvFiles[0].Range[0],
				kvFiles[len(kvFiles)-1].Range[1],
			}
		}

		if len(dm.Files) > 0 {
			manifest.Domains[string(domain)] = dm
		}
	}

	return manifest
}

// isCanonicalFile checks if a single file is at a canonical (power-of-2 aligned) boundary.
func isCanonicalFile(r snapshotinv.StepRange) bool {
	size := r.Len()
	if size == 0 {
		return false
	}
	// Power of 2 check.
	if size&(size-1) != 0 {
		return false
	}
	// Alignment check: From must be divisible by size.
	if r.From%size != 0 {
		return false
	}
	return true
}

// MarshalV2 serializes a V2 manifest to deterministic TOML bytes.
func MarshalV2(manifest *ChainTomlV2) ([]byte, error) {
	var buf bytes.Buffer
	enc := toml.NewEncoder(&buf)
	if err := enc.Encode(manifest); err != nil {
		return nil, fmt.Errorf("encoding chain.toml V2: %w", err)
	}
	return buf.Bytes(), nil
}

// ParseV2 parses V2 chain.toml bytes into the structured representation.
// Returns an error if the version field is not 2.
//
// Tolerates older publishers that didn't emit per-file Kind by defaulting
// empty Kind to KindKVName — the original V2 only carried kv files in
// the domains section.
func ParseV2(data []byte) (*ChainTomlV2, error) {
	var manifest ChainTomlV2
	if err := toml.Unmarshal(data, &manifest); err != nil {
		return nil, fmt.Errorf("parsing chain.toml V2: %w", err)
	}
	if manifest.Version != ChainTomlV2Version {
		return nil, fmt.Errorf("expected chain.toml version %d, got %d", ChainTomlV2Version, manifest.Version)
	}
	for _, dm := range manifest.Domains {
		if dm == nil {
			continue
		}
		for i := range dm.Files {
			if dm.Files[i].Kind == "" {
				dm.Files[i].Kind = KindKVName
			}
		}
	}
	return &manifest, nil
}

// DetectVersion reads the version field from chain.toml bytes without full parsing.
// Returns 1 for V1 (no version field) or the version number for V2+.
func DetectVersion(data []byte) int {
	var header struct {
		Version int `toml:"version"`
	}
	if err := toml.Unmarshal(data, &header); err != nil {
		return 1 // parse error → assume V1
	}
	if header.Version == 0 {
		return 1 // no version field → V1
	}
	return header.Version
}

// HeaderStateRootFn looks up the consensus block-header stateRoot for
// a given block number. Used by ApplyV2AnchorsToInventory's optional
// cross-check: if provided, every entry's ProofRoot must equal the
// stateRoot the chain committed at AtBlock. Pass nil to skip the
// cross-check (still applies anchors). Returning an error from the
// callback is treated as a lookup failure — the entry is skipped, not
// rejected, since the consumer may not yet have the header for AtBlock.
type HeaderStateRootFn func(blockNum uint64) (root [32]byte, err error)

// ApplyV2AnchorsToInventory walks the manifest's domain files and stamps
// step-header anchors (ProofRoot/AtBlock/AtTxNum/IsPartialBlock) onto
// matching FileEntries in inv via SetAnchors. Returns the count of
// entries updated.
//
// When headerRoot is non-nil, every entry's ProofRoot is verified
// against the stateRoot the chain header records for AtBlock. A
// mismatch is reported via the returned mismatches list — the entry's
// anchors are NOT applied. Lookup errors (chain not yet at AtBlock) are
// silent: the anchors are skipped this pass and can be re-applied later.
//
// Files in Blocks/Meta/Salt/Caplin sections carry no anchors today, so
// they're untouched. Anchors in V2 are emitted only on domain files
// (currently the commitment domain — that's where the state-key sits).
//
// MismatchedAnchor names a file whose published ProofRoot disagrees
// with the chain header — pinpoints where the publisher's manifest
// claim diverges from consensus.
func ApplyV2AnchorsToInventory(inv *snapshotinv.Inventory, manifest *ChainTomlV2, headerRoot HeaderStateRootFn) (applied int, mismatches []MismatchedAnchor, err error) {
	if inv == nil || manifest == nil {
		return 0, nil, nil
	}
	for _, dm := range manifest.Domains {
		if dm == nil {
			continue
		}
		for _, f := range dm.Files {
			if f.ProofRoot == "" {
				continue
			}
			rootBytes, decErr := hex.DecodeString(f.ProofRoot)
			if decErr != nil {
				return applied, mismatches, fmt.Errorf("decoding ProofRoot for %s: %w", f.Name, decErr)
			}
			if len(rootBytes) != 32 {
				return applied, mismatches, fmt.Errorf("ProofRoot for %s is %d bytes, want 32", f.Name, len(rootBytes))
			}
			var root [32]byte
			copy(root[:], rootBytes)

			if headerRoot != nil {
				chainRoot, lookupErr := headerRoot(f.AtBlock)
				if lookupErr == nil && chainRoot != root {
					mismatches = append(mismatches, MismatchedAnchor{
						Name:       f.Name,
						AtBlock:    f.AtBlock,
						Manifest:   root,
						ChainState: chainRoot,
					})
					continue
				}
			}

			if inv.SetAnchors(f.Name, root, f.AtBlock, f.AtTxNum, f.IsPartialBlock) {
				applied++
			}
		}
	}
	return applied, mismatches, nil
}

// MismatchedAnchor reports a file whose manifest-published ProofRoot
// disagrees with the chain header's stateRoot for AtBlock. The
// publisher's claim cannot be trusted for this file; consumer must not
// advertise it.
type MismatchedAnchor struct {
	Name       string
	AtBlock    uint64
	Manifest   [32]byte
	ChainState [32]byte
}

// PartialBlockWithoutCoverage names a manifest entry that the publisher
// flagged as partial-block (IsPartialBlock=true) but for which the
// same manifest lacks any block .seg covering AtBlock. The consumer
// cannot replay-verify across the partial boundary without that block
// data, so the file must NOT be treated as a usable snapshot tip
// until the manifest catches up — typically the publisher's next
// republish will include the missing block .seg once its lifecycle
// completes.
type PartialBlockWithoutCoverage struct {
	Name    string
	AtBlock uint64
}

// FindPartialBlockCommitmentsWithoutCoverage scans the manifest for
// partial-block commitments whose AtBlock is NOT covered by any block
// .seg in the same manifest. Mirror of the publisher-side
// blockSegAdvertisableForBlock check (commitment_validator.go), but
// against a discovered manifest instead of the local lifecycle: the
// consumer trusts a partial-block commitment as a usable snapshot tip
// only when the publisher has bundled the matching block data in the
// same manifest generation. Files in the returned list represent
// "abnormal" manifest states the consumer should refuse to load —
// either the publisher is mid-republish (transient) or its lifecycle
// is in an unusual state (operational signal).
//
// Block file ranges are parsed from m.Blocks names (e.g.
// "v1.1-025040-025050-headers.seg" → [25_040_000, 25_050_000)).
//
// Pure function; no inventory or chain-reader needed. Empty result
// means the manifest is self-consistent for partial-block tips.
func FindPartialBlockCommitmentsWithoutCoverage(m *ChainTomlV2) []PartialBlockWithoutCoverage {
	if m == nil {
		return nil
	}
	type blockRange struct{ from, to uint64 }
	var ranges []blockRange
	for name := range m.Blocks {
		// ParseFileName populates From/To even when ok=false (the ok
		// signal requires the file's Type to be registered via
		// freezeblocks init). For range-only logic we tolerate
		// ok=false as long as From < To.
		info, _, _ := snaptype.ParseFileName("", name)
		if info.From >= info.To {
			continue
		}
		ranges = append(ranges, blockRange{from: info.From, to: info.To})
	}

	var out []PartialBlockWithoutCoverage
	for _, dm := range m.Domains {
		if dm == nil {
			continue
		}
		for _, f := range dm.Files {
			if !f.IsPartialBlock {
				continue
			}
			covered := false
			for _, r := range ranges {
				if f.AtBlock >= r.from && f.AtBlock < r.to {
					covered = true
					break
				}
			}
			if !covered {
				out = append(out, PartialBlockWithoutCoverage{Name: f.Name, AtBlock: f.AtBlock})
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// DomainCoverage extracts the step ranges from a V2 domain manifest.
func (dm *DomainManifest) StepRanges() snapshotinv.StepRanges {
	ranges := make(snapshotinv.StepRanges, 0, len(dm.Files))
	for _, f := range dm.Files {
		ranges = append(ranges, snapshotinv.StepRange{From: f.Range[0], To: f.Range[1]})
	}
	return ranges.Normalize()
}

// FilesAtTrust returns only the files at or above the given trust level.
func (dm *DomainManifest) FilesAtTrust(minTrust snapshotinv.TrustLevel) []DomainFileEntry {
	var result []DomainFileEntry
	for _, f := range dm.Files {
		t, err := snapshotinv.ParseTrustLevel(f.Trust)
		if err != nil {
			continue
		}
		if t.Satisfies(minTrust) {
			result = append(result, f)
		}
	}
	return result
}
