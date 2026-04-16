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
// V2 (explicit): versioned format with [blocks], [meta], [domains.*] sections.
const ChainTomlV2Version = 2

// ChainTomlV2 is the structured representation of a V2 chain.toml manifest.
type ChainTomlV2 struct {
	Version int `toml:"version"`

	// Block snapshot files — deterministic, all nodes produce identical files.
	Blocks map[string]string `toml:"blocks,omitempty"`

	// Metadata files (erigondb.toml, salt files).
	Meta map[string]string `toml:"meta,omitempty"`

	// Per-domain state snapshot sections.
	Domains map[string]*DomainManifest `toml:"domains,omitempty"`
}

// DomainManifest describes the available files for a single state domain.
type DomainManifest struct {
	// Coverage is the [from, to) step range covered by all files combined.
	Coverage [2]uint64 `toml:"coverage"`

	// Files lists the individual snapshot files with their step ranges, hashes, and trust.
	Files []DomainFileEntry `toml:"files,omitempty"`
}

// DomainFileEntry is a single file in a domain manifest.
type DomainFileEntry struct {
	Name  string    `toml:"name"`
	Range [2]uint64 `toml:"range"`
	Hash  string    `toml:"hash"`
	Trust string    `toml:"trust"`
}

// GenerateV2 builds a V2 chain.toml from a snapshot inventory.
//
// Only files that pass the IsCanonical check are included in the domains section.
// Non-canonical files (merge backlog) are excluded — they'll appear in a future
// publication once merges catch up.
//
// Block snapshot files and metadata are included as-is (they're deterministic).
func GenerateV2(inv *snapshotinv.Inventory) *ChainTomlV2 {
	manifest := &ChainTomlV2{
		Version: ChainTomlV2Version,
		Blocks:  make(map[string]string),
		Meta:    make(map[string]string),
		Domains: make(map[string]*DomainManifest),
	}

	// Block files.
	for _, f := range inv.BlockFiles() {
		if f.TorrentHash != [20]byte{} {
			manifest.Blocks[f.Name] = fmt.Sprintf("%x", f.TorrentHash)
		}
	}

	// Domain files — only canonical files.
	for _, domain := range inv.Domains() {
		files := inv.LocalFiles(domain)
		if len(files) == 0 {
			continue
		}

		// Build the layout from local files and check canonicity.
		var layout snapshotinv.StepRanges
		for _, f := range files {
			layout = append(layout, f.Range())
		}
		layout = layout.Normalize()

		dm := &DomainManifest{}

		// Only include files at canonical boundaries with a torrent hash.
		// Coverage is computed from what's actually listed, not from all
		// local files — avoids advertising coverage for uncanonical/unhashed files.
		for _, f := range files {
			r := f.Range()
			if !isCanonicalFile(r) {
				continue
			}
			if f.TorrentHash == [20]byte{} {
				continue // no torrent hash yet
			}
			dm.Files = append(dm.Files, DomainFileEntry{
				Name:  f.Name,
				Range: [2]uint64{r.From, r.To},
				Hash:  fmt.Sprintf("%x", f.TorrentHash),
				Trust: f.Trust.String(),
			})
		}

		// Sort files by range for deterministic output.
		sort.Slice(dm.Files, func(i, j int) bool {
			return dm.Files[i].Range[0] < dm.Files[j].Range[0]
		})

		// Compute coverage from the published file list (not all local files).
		if len(dm.Files) > 0 {
			dm.Coverage = [2]uint64{
				dm.Files[0].Range[0],
				dm.Files[len(dm.Files)-1].Range[1],
			}
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
func ParseV2(data []byte) (*ChainTomlV2, error) {
	var manifest ChainTomlV2
	if err := toml.Unmarshal(data, &manifest); err != nil {
		return nil, fmt.Errorf("parsing chain.toml V2: %w", err)
	}
	if manifest.Version != ChainTomlV2Version {
		return nil, fmt.Errorf("expected chain.toml version %d, got %d", ChainTomlV2Version, manifest.Version)
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
