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

// Parser for the preverified.toml file shipped alongside an Erigon snapshot
// dir. The file is a flat TOML map of `'<rel-path>' = '<40-char hex>'`,
// authoritative for what should be on disk for a given chain. Tests use it
// as the open-loop reference: any swarm replication must converge on this
// exact (filename → infohash) set.

package harness

import (
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/pelletier/go-toml/v2"

	"github.com/erigontech/erigon/node/components/storage/snapshot"
)

// PreverifiedRole classifies a preverified.toml entry by what role it
// plays in the snapshot archive. Only the first three roles get
// transferred over P2P; derived files are rebuilt locally from the
// primaries via BuildMissedAccessors / BuildMissedIndices.
type PreverifiedRole int

const (
	RoleUnknown PreverifiedRole = iota
	RoleBlockSeg                 // top-level v*.seg (headers, bodies, transactions)
	RoleDomainKV                 // domain/v*-<domain>.<from>-<to>.kv
	RoleDomainHistory            // history/v*-<domain>.<from>-<to>.v
	RoleDomainIdx                // idx/v*-<domain>.<from>-<to>.ef
	RoleCaplinSeg                // caplin/v*-beaconblocks.seg
	RoleMeta                     // erigondb.toml
	RoleSalt                     // salt-*.txt
	RoleDerivedAccessor          // .vi, .efi, .kvi, .kvei, .bt — rebuilt locally
	RoleDerivedBlockIdx          // top-level .idx — rebuilt locally
	RoleDerivedCaplinIdx         // caplin/*.idx — rebuilt locally
)

// IsPrimary reports whether the role is one a peer must transfer over P2P
// (vs locally rebuildable). Returned roles include the kind that V2
// advertises in its [blocks], [meta], [salt], [domains.*], [[caplin]]
// sections.
func (r PreverifiedRole) IsPrimary() bool {
	switch r {
	case RoleBlockSeg, RoleDomainKV, RoleDomainHistory, RoleDomainIdx,
		RoleCaplinSeg, RoleMeta, RoleSalt:
		return true
	}
	return false
}

// String returns a short tag suitable for log lines; not part of any wire
// format.
func (r PreverifiedRole) String() string {
	switch r {
	case RoleBlockSeg:
		return "block.seg"
	case RoleDomainKV:
		return "domain.kv"
	case RoleDomainHistory:
		return "domain.v"
	case RoleDomainIdx:
		return "domain.ef"
	case RoleCaplinSeg:
		return "caplin.seg"
	case RoleMeta:
		return "meta"
	case RoleSalt:
		return "salt"
	case RoleDerivedAccessor:
		return "derived.accessor"
	case RoleDerivedBlockIdx:
		return "derived.block.idx"
	case RoleDerivedCaplinIdx:
		return "derived.caplin.idx"
	}
	return "unknown"
}

// PreverifiedEntry is one parsed line from preverified.toml augmented
// with our role classification and (when populated) the matching
// snapshot.FileKind for V2 wire purposes.
type PreverifiedEntry struct {
	// RelPath is the path relative to the snapshots dir (e.g.
	// "accessor/v1.1-code.0-128.vi" or "v1.1-000000-000100-headers.seg").
	RelPath string

	// Role is the classification used to decide whether the entry
	// participates in P2P transfer.
	Role PreverifiedRole

	// InfoHash is the canonical 20-byte BitTorrent info-hash from
	// preverified.toml — the open-loop reference every peer's local
	// hash computation must match.
	InfoHash [20]byte

	// Kind is the snapshot.FileKind a primary entry maps to. Empty for
	// derived / unknown roles.
	Kind snapshot.FileKind

	// Domain is set for domain primaries (kv, history, idx).
	Domain snapshot.Domain

	// FromStep / ToStep set for entries with step-range semantics
	// (domain primaries). Zero otherwise.
	FromStep uint64
	ToStep   uint64
}

// LoadPreverified reads <snapDir>/preverified.toml, parses it, and
// returns the entries sorted by RelPath. Malformed hashes are skipped
// with a returned-error count rather than aborting — the file is
// machine-generated but not all entries are equally trustworthy and a
// single bad line shouldn't sink the test.
func LoadPreverified(snapDir string) ([]PreverifiedEntry, error) {
	path := filepath.Join(snapDir, "preverified.toml")
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading %s: %w", path, err)
	}
	var raw map[string]string
	if err := toml.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("parsing %s: %w", path, err)
	}

	out := make([]PreverifiedEntry, 0, len(raw))
	for relPath, hashStr := range raw {
		hash, ok := decodeHash20(hashStr)
		if !ok {
			continue
		}
		entry := PreverifiedEntry{
			RelPath:  relPath,
			InfoHash: hash,
		}
		classifyEntry(&entry)
		out = append(out, entry)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].RelPath < out[j].RelPath })
	return out, nil
}

// stateDomainNameRE matches v<X>.<Y>-<domain>.<from>-<to>.{kv,kvi,kvei,bt,v,ef,vi,efi}
var stateDomainNameRE = regexp.MustCompile(`^v\d+\.\d+-([a-z]+)\.(\d+)-(\d+)\.([a-z]+)$`)

// blockFileNameRE matches v<X>.<Y>-<from>-<to>-<role>.{seg,idx}
// Caplin role names are CamelCase (BlockRoot, ActiveValidatorIndicies);
// EL block role names are lowercase (headers, bodies, transactions). The
// role segment accepts both.
var blockFileNameRE = regexp.MustCompile(`^v\d+\.\d+-(\d+)-(\d+)-[A-Za-z][A-Za-z0-9\-]*\.(seg|idx)$`)

// classifyEntry sets Role, Kind, Domain, FromStep, ToStep on a freshly
// parsed entry from preverified.toml. The classification is filename-shape
// based — preverified.toml has no metadata beyond name + hash, so we
// recover everything from the relative path.
func classifyEntry(e *PreverifiedEntry) {
	dir, base := splitRel(e.RelPath)

	// Top-level config + salt files first (no extension parsing needed).
	if dir == "" {
		switch base {
		case "erigondb.toml":
			e.Role = RoleMeta
			e.Kind = snapshot.KindMeta
			return
		case "salt-blocks.txt", "salt-state.txt":
			e.Role = RoleSalt
			e.Kind = snapshot.KindSalt
			return
		}
	}

	// Block-shaped names: v<X>.<Y>-<from>-<to>-<role>.{seg,idx}
	if m := blockFileNameRE.FindStringSubmatch(base); m != nil {
		from, _ := strconv.ParseUint(m[1], 10, 64)
		to, _ := strconv.ParseUint(m[2], 10, 64)
		ext := m[3]
		switch dir {
		case "":
			if ext == "seg" {
				e.Role = RoleBlockSeg
			} else {
				e.Role = RoleDerivedBlockIdx
			}
		case "caplin":
			if ext == "seg" {
				e.Role = RoleCaplinSeg
				e.Kind = snapshot.KindCaplin
			} else {
				e.Role = RoleDerivedCaplinIdx
			}
		}
		e.FromStep = from
		e.ToStep = to
		return
	}

	// State-domain-shaped names: v<X>.<Y>-<domain>.<from>-<to>.<ext>
	if m := stateDomainNameRE.FindStringSubmatch(base); m != nil {
		domainName := m[1]
		from, _ := strconv.ParseUint(m[2], 10, 64)
		to, _ := strconv.ParseUint(m[3], 10, 64)
		ext := m[4]

		e.FromStep = from
		e.ToStep = to
		e.Domain = snapshot.Domain(domainName)

		switch dir {
		case "domain":
			switch ext {
			case "kv":
				e.Role = RoleDomainKV
				e.Kind = snapshot.KindKV
			case "kvi", "kvei", "bt":
				e.Role = RoleDerivedAccessor
			}
		case "history":
			if ext == "v" {
				e.Role = RoleDomainHistory
				e.Kind = snapshot.KindHistory
			}
		case "idx":
			if ext == "ef" {
				e.Role = RoleDomainIdx
				e.Kind = snapshot.KindIdx
			}
		case "accessor":
			if ext == "vi" || ext == "efi" {
				e.Role = RoleDerivedAccessor
			}
		}
	}
}

// splitRel separates "<dir>/<base>" into (dir, base). Returns ("", base)
// when there is no separator.
func splitRel(relPath string) (string, string) {
	idx := strings.IndexByte(relPath, '/')
	if idx < 0 {
		return "", relPath
	}
	return relPath[:idx], relPath[idx+1:]
}

// decodeHash20 parses a 40-char hex string into a fixed-size info-hash.
// Tolerant only of valid input — silent skip is the caller's job.
func decodeHash20(s string) ([20]byte, bool) {
	var out [20]byte
	if len(s) != 40 {
		return out, false
	}
	b, err := hex.DecodeString(s)
	if err != nil {
		return out, false
	}
	copy(out[:], b)
	return out, true
}
