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

package validation

import (
	"fmt"
	"strings"

	"github.com/erigontech/erigon/node/components/storage/snapshot"
)

// NameNotEmpty rejects FileEntry values whose Name is the empty
// string. A trivial sanity check that catches buggy manifest
// generation or in-process corruption — a file with no name has no
// way to be served and should never reach the inventory.
type NameNotEmpty struct{}

// Name implements Validator.
func (NameNotEmpty) Name() string { return "name_not_empty" }

// Validate implements Validator.
func (NameNotEmpty) Validate(file *snapshot.FileEntry, _ ContentSource) error {
	if file == nil {
		return fmt.Errorf("nil file entry")
	}
	if file.Name == "" {
		return fmt.Errorf("file entry has empty Name")
	}
	return nil
}

// RangeOrdering rejects FileEntry values whose FromStep > ToStep.
// Step ranges are half-open [from, to); zero-zero is allowed (matches
// files that don't carry step semantics — caplin, meta, salt).
//
// Equal non-zero from/to is rejected because a [N, N) range is empty
// and a file claiming to cover an empty range is a producer bug.
type RangeOrdering struct{}

// Name implements Validator.
func (RangeOrdering) Name() string { return "range_ordering" }

// Validate implements Validator.
func (RangeOrdering) Validate(file *snapshot.FileEntry, _ ContentSource) error {
	if file == nil {
		return fmt.Errorf("nil file entry")
	}
	if file.FromStep == 0 && file.ToStep == 0 {
		// Step-less file (caplin, meta, salt). Permitted.
		return nil
	}
	if file.FromStep >= file.ToStep {
		return fmt.Errorf("FromStep=%d must be strictly less than ToStep=%d (empty or inverted range)",
			file.FromStep, file.ToStep)
	}
	return nil
}

// KindConsistencyFromName rejects FileEntry values whose declared
// Kind disagrees with what the Name field's pattern implies.
//
// Mappings (the only shapes the snapshot subsystem produces):
//
//	*.kv                                → KindKV    (Domain non-empty)
//	*.v                                 → KindHistory
//	*.ef                                → KindIdx
//	caplin/*.seg                        → KindCaplin
//	*.seg (no caplin/ prefix)           → KindKV    (Domain empty — block file)
//	erigondb.toml                       → KindMeta
//	salt-*.txt                          → KindSalt
//
// Names that don't match any pattern are accepted (the validator
// can't speak to them) — the chain has other validators that gate
// shape on different axes.
type KindConsistencyFromName struct{}

// Name implements Validator.
func (KindConsistencyFromName) Name() string { return "kind_consistency_from_name" }

// Validate implements Validator.
func (KindConsistencyFromName) Validate(file *snapshot.FileEntry, _ ContentSource) error {
	if file == nil {
		return fmt.Errorf("nil file entry")
	}
	expected, ok := inferKindFromName(file.Name)
	if !ok {
		return nil // unrecognised pattern; can't speak to this file
	}
	if file.Kind != expected {
		return fmt.Errorf("name %q implies Kind=%q but entry has Kind=%q",
			file.Name, expected, file.Kind)
	}
	return nil
}

// inferKindFromName maps a snapshot file's name to the Kind the
// snapshot subsystem would assign to it. Returns ok=false for
// unrecognised patterns; the caller treats unknown as "can't speak
// to this file" rather than reject.
func inferKindFromName(name string) (snapshot.FileKind, bool) {
	if name == "" {
		return "", false
	}
	if name == "erigondb.toml" {
		return snapshot.KindMeta, true
	}
	if strings.HasPrefix(name, "salt-") && strings.HasSuffix(name, ".txt") {
		return snapshot.KindSalt, true
	}
	switch {
	case strings.HasSuffix(name, ".kv"):
		return snapshot.KindKV, true
	case strings.HasSuffix(name, ".v"):
		return snapshot.KindHistory, true
	case strings.HasSuffix(name, ".ef"):
		return snapshot.KindIdx, true
	case strings.HasSuffix(name, ".seg"):
		if strings.HasPrefix(name, "caplin/") {
			return snapshot.KindCaplin, true
		}
		return snapshot.KindKV, true
	}
	return "", false
}

// DefaultStage1Chain returns the baseline stage-1 validator chain
// every storage adapter starts with when stage-1 validation is
// enabled. Operators with custom needs append to this slice — the
// built-ins are the floor, deployment validators are the ceiling.
func DefaultStage1Chain() Chain {
	return Chain{
		NameNotEmpty{},
		RangeOrdering{},
		KindConsistencyFromName{},
	}
}
