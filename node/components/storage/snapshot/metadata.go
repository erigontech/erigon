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

// PopulateFromName fills in FromStep, ToStep, Domain (and Kind, if
// not set) on a FileEntry by parsing its Name. The single seam every
// caller goes through to derive step + domain metadata from a
// snapshot file name; centralises the parsing + (eventually)
// block→txnum→step unit conversion.
//
// Today the implementation calls snaptype.ParseFileName and copies
// the result. Block files end up with FromStep/ToStep in
// block-number units (multiplied by 1000 by ParseFileName), state
// files in step-number units. Sibling grouping works in either
// unit because step-siblings share the same name pattern and thus
// the same parsed numbers. Cross-kind comparisons (block vs state)
// are NOT meaningful in current units — proper block→txnum→step
// conversion (using the chain's actual block→txnum mapping + step
// size) is follow-up work that lives behind this seam.
//
// Caller-facing contract:
//   - Name must already be set on the entry.
//   - Existing non-zero Domain / Kind / FromStep / ToStep on the
//     entry are preserved.
//   - On unrecognised name patterns the entry is left alone.
//
// Returns true when at least one field was populated.
func PopulateFromName(entry *FileEntry) bool {
	if entry == nil || entry.Name == "" {
		return false
	}
	info, _, _ := snaptype.ParseFileName("", entry.Name)
	populated := false
	if entry.FromStep == 0 && entry.ToStep == 0 && info.To > 0 {
		entry.FromStep = info.From
		entry.ToStep = info.To
		populated = true
	}
	if entry.Domain == "" {
		if domain := domainFromTypeString(info.TypeString); domain != "" {
			entry.Domain = domain
			populated = true
		}
	}
	if entry.Kind == "" {
		if k, ok := InferKind(entry.Name); ok {
			entry.Kind = k
			populated = true
		}
	}
	return populated
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
