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

package snapshot

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/erigontech/erigon/db/snaptype"
)

// ValidateMetadata is the Tier-0 per-file invariant check, applied
// at AddFile time. Files that fail this check never enter the
// inventory — by construction the inventory only ever holds entries
// with consistent metadata.
//
// This is the same shape of check that the validation package
// exposes as Validator types (NameNotEmpty, RangeOrdering,
// KindConsistencyFromName) for operator-composable chains; the
// inventory enforces them as invariants regardless of what an
// operator chain does, because malformed entries break grouping,
// dispatch, and serialisation downstream.
//
// Kind handling note: KindKV is the zero value of FileKind, so an
// entry with Kind=="" is ambiguous — either explicitly KV or simply
// not-yet-set. AddFile resolves this ambiguity by auto-deriving Kind
// from the name pattern when entry.Kind is zero (see InferKind +
// AddFile). ValidateMetadata only flags inconsistency for explicit
// non-zero Kinds that disagree with the name.
//
// Returns nil on accept; a descriptive error on reject.
func ValidateMetadata(entry *FileEntry) error {
	if entry == nil {
		return fmt.Errorf("nil FileEntry")
	}
	if entry.Name == "" {
		return fmt.Errorf("empty Name")
	}
	// Range ordering: zero-zero is allowed (non-stepped files); any
	// other non-strict ordering is a producer bug.
	if !(entry.FromStep == 0 && entry.ToStep == 0) {
		if entry.FromStep >= entry.ToStep {
			return fmt.Errorf("FromStep=%d must be strictly less than ToStep=%d (empty or inverted range)",
				entry.FromStep, entry.ToStep)
		}
	}
	// Kind consistency: only enforced when Kind is explicitly set
	// (non-zero). A zero-value Kind is treated as "derive from name"
	// and AddFile fills it in.
	if entry.Kind != "" {
		if expected, ok := InferKind(entry.Name); ok && entry.Kind != expected {
			return fmt.Errorf("name %q implies Kind=%q but entry has Kind=%q",
				entry.Name, expected, entry.Kind)
		}
	}
	return nil
}

// InferKind returns the FileKind implied by the name's pattern.
// Returns ok=false for unrecognised patterns (the caller treats
// unknown as "can't speak to this file"). Used by AddFile to
// auto-fill Kind when the caller didn't set one explicitly, and by
// ValidateMetadata to check consistency when Kind is explicit.
func InferKind(name string) (FileKind, bool) {
	return inferKindFromName(name)
}

// PopulateFromName fills in step / block range, Domain, and Kind on
// a FileEntry by parsing its Name. The single seam every caller goes
// through to derive metadata from a snapshot file name.
//
// The two range axes are mutually exclusive: state files populate
// FromStep/ToStep + Domain; block files populate FromBlock/ToBlock
// (no Domain). A block file's step range is unknown until a
// commitment-derived (step, block) binding maps it — until then,
// FromStep/ToStep stay zero, which truthfully reflects "we don't
// know the step yet". Block files beyond the last validated step
// legitimately sit in this state until either a peer's commitment
// validates or this node's own retire produces one.
//
// Caller-facing contract:
//   - Name must already be set on the entry.
//   - Existing non-zero Domain / Kind / FromStep / ToStep / FromBlock
//     / ToBlock on the entry are preserved.
//   - On unrecognised name patterns the entry is left alone.
//
// Returns true when at least one field was populated.
func PopulateFromName(entry *FileEntry) bool {
	if entry == nil || entry.Name == "" {
		return false
	}
	info, _, _ := snaptype.ParseFileName("", entry.Name)
	populated := false

	// Determine the kind first — drives which range axis to populate.
	if entry.Kind == "" {
		if k, ok := InferKind(entry.Name); ok {
			entry.Kind = k
			populated = true
		}
	}
	if entry.Domain == "" {
		if domain := domainFromTypeString(info.TypeString); domain != "" {
			entry.Domain = domain
			populated = true
		}
	}

	// State files (have a Domain, derived from typeString):
	// populate the STEP axis. snaptype's parser yields step units
	// directly for these.
	if entry.Domain != "" {
		if entry.FromStep == 0 && entry.ToStep == 0 && info.To > 0 {
			entry.FromStep = info.From
			entry.ToStep = info.To
			populated = true
		}
		return populated
	}

	// Block files (no Domain, but parser succeeded with non-zero
	// range): populate the BLOCK axis. snaptype's parser yields
	// block units (multiplied ×1000 internally) for these.
	// FromStep/ToStep stay zero — explicitly "step unknown" until a
	// commitment binding establishes it.
	if info.To > 0 && entry.FromBlock == 0 && entry.ToBlock == 0 {
		entry.FromBlock = info.From
		entry.ToBlock = info.To
		populated = true
	}
	return populated
}

// PathForName returns the on-disk path for a snapshot file, given the
// snap-dir root and the file's basename. Encodes Erigon's canonical
// snapshot layout:
//
//   - .seg, .idx, .torrent, salt-*.txt, erigondb.toml → top-level
//   - .kv (state primaries: accounts, storage, code, commitment) → domain/
//   - .v (history primaries) → history/
//   - .ef (inverted-index primaries) → idx/
//   - .kvi, .bt (state accessors) → accessor/
//
// The single seam every caller — both discovery (driver.discoverNewFiles)
// and presence-checking validators (AllFilesPresent) — should go
// through. Without a single seam the layout is encoded in two places
// and they drift (the 2026-05-06 bug: discoverNewFiles scanned subdirs
// but AllFilesPresent stat-ed top-level only, so state files were
// "discovered" but never marked present).
//
// Unrecognised names fall back to top-level (the safest assumption —
// matches the pre-V2 era and any future singleton files).
func PathForName(snapDir, name string) string {
	return filepath.Join(snapDir, RelPathForName(name))
}

// RelPathForName returns the snap-dir-relative path for a file given
// either its basename or its already-relative form. Idempotent: callers
// can pass "foo.kv" (basename) or "domain/foo.kv" (already relative)
// and get back "domain/foo.kv". The form the downloader's Seed /
// Delete calls expect.
//
// Idempotency matters because the inventory mixes the two forms today:
// the disk-scan path adds basenames, the legacy retire OnFilesChange
// path adds subdir-prefixed relative paths. Both end up in the same
// ChangeSet stream — without idempotency, double-prefixing happens for
// the latter (idx/idx/foo.ef) and the downloader fails to find the file.
//
// Bare basename (top-level) is returned for file kinds with no subdir.
func RelPathForName(name string) string {
	sub := SubdirForName(name)
	if sub == "" {
		return name
	}
	if strings.HasPrefix(name, sub+"/") || strings.HasPrefix(name, sub+string(filepath.Separator)) {
		return name
	}
	return filepath.Join(sub, name)
}

// SubdirForName returns the subdirectory (relative to snap-dir) where
// a snapshot file with the given basename lives. "" means top-level.
//
// Layout (verified against an Erigon mainnet snapshots/ directory):
//   - domain/    → .kv (primary), .kvi/.kvei/.bt (in-domain accessors)
//   - history/   → .v
//   - idx/       → .ef
//   - accessor/  → .vi
//
// Note: the .kvi/.kvei/.bt accessor files live ALONGSIDE their .kv
// primary in domain/, not in a separate accessor/ tree — Erigon
// puts only .vi files (history accessors) in accessor/.
//
// Public so the lifecycle driver and the bridge subscriber can
// converge on the same layout without each maintaining its own
// switch — see PathForName / RelPathForName for the composed forms
// most callers want.
func SubdirForName(name string) string {
	switch {
	case strings.HasSuffix(name, ".kv"),
		strings.HasSuffix(name, ".kvi"),
		strings.HasSuffix(name, ".kvei"),
		strings.HasSuffix(name, ".bt"):
		return "domain"
	case strings.HasSuffix(name, ".v"):
		return "history"
	case strings.HasSuffix(name, ".ef"):
		return "idx"
	case strings.HasSuffix(name, ".vi"):
		return "accessor"
	default:
		return ""
	}
}

// domainFromTypeString maps a snaptype TypeString (like "accounts",
// "accountsHistory", "storage") to the FileEntry's Domain. History /
// Idx suffixes are stripped because Erigon's inventory groups
// primary + history + idx files for the same state slice under the
// same Domain — they're step-siblings of one retire cycle.
//
// Returns "" for typeStrings that don't map to any state Domain
// (e.g. "headers", "bodies" — block files have empty Domain).
func domainFromTypeString(typeString string) Domain {
	if typeString == "" {
		return ""
	}
	base := typeString
	for _, suffix := range []string{"History", "Idx"} {
		if strings.HasSuffix(base, suffix) {
			base = base[:len(base)-len(suffix)]
			break
		}
	}
	switch base {
	case "accounts":
		return DomainAccounts
	case "storage":
		return DomainStorage
	case "code":
		return DomainCode
	case "commitment":
		return DomainCommitment
	case "receipt":
		return DomainReceipt
	}
	return ""
}

// inferKindFromName maps a snapshot file's name to the Kind the
// snapshot subsystem would assign to it. Returns ok=false for
// unrecognised patterns; the caller treats unknown as "can't speak
// to this file" rather than reject. Mirrors the table in
// validation/builtins.go (KindConsistencyFromName) — kept here to
// avoid a snapshot→validation import cycle.
func inferKindFromName(name string) (FileKind, bool) {
	if name == "" {
		return "", false
	}
	if name == "erigondb.toml" {
		return KindMeta, true
	}
	if strings.HasPrefix(name, "salt-") && strings.HasSuffix(name, ".txt") {
		return KindSalt, true
	}
	switch {
	case strings.HasSuffix(name, ".kv"):
		return KindKV, true
	case strings.HasSuffix(name, ".v"):
		return KindHistory, true
	case strings.HasSuffix(name, ".ef"):
		return KindIdx, true
	case strings.HasSuffix(name, ".seg"):
		if strings.HasPrefix(name, "caplin/") {
			return KindCaplin, true
		}
		return KindKV, true
	}
	return "", false
}
