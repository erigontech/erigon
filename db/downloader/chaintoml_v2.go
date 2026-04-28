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
	"fmt"
	"sort"

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
			dm.Files = append(dm.Files, DomainFileEntry{
				Name:  f.Name,
				Range: [2]uint64{r.From, r.To},
				Kind:  kind,
				Hash:  fmt.Sprintf("%x", f.TorrentHash),
				Trust: f.Trust.String(),
			})
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
