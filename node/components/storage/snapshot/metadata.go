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
