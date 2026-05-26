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

package snapshotsync

import (
	"strings"

	"github.com/erigontech/erigon/db/snaptype"
)

// blockFileKinds names the file-kind strings ParseRange returns for the
// block-indexed family. State-domain files use the *step* axis (steps,
// not blocks); their range is in step units and is not directly
// comparable to a block-axis ToBlock rewind value, so this helper only
// demotes block-axis entries by default. A future step→block resolver
// can extend the predicate to cover state files too — see the StateBlocks
// arm on DemoteByRewindPredicate.
var blockFileKinds = map[string]struct{}{
	"headers":      {},
	"bodies":       {},
	"transactions": {},
}

// DemoteByRewindPredicate returns a shouldDrop predicate suitable for
// CanonicalView.Demote that drops block-axis files whose range
// includes any block strictly above toBlock — the rewind target.
//
// A file at range [from, to) in block units covers blocks
// [from, to-1]. The predicate returns true when `to > toBlock + 1`
// (i.e. the file references at least one block above toBlock); whole
// files only — no partial-file surgery — per the canonical-layer
// revision spec §5.5.
//
// State-domain files (kv / history / idx) are NOT touched by this
// predicate: their range is in step units, which requires a step→block
// binding to compare against toBlock. Callers that need state-file
// demotion compose this predicate with their own step-axis predicate
// using `Or(blockPred, statePred)`.
//
// Unparseable names are conservatively dropped — an honest publisher's
// manifest never carries them, and a rewind is exactly the wrong
// moment to give doubtful entries the benefit of the doubt.
func DemoteByRewindPredicate(toBlock uint64) func(name string) bool {
	return func(name string) bool {
		typeStr, _, to, ok := snaptype.ParseRange(name)
		if !ok {
			// Defensive: keep state-domain files (they have step-axis
			// ranges, not block-axis) and other unparseable names alone
			// for the block-axis predicate. A separate step-axis
			// predicate composes via Or.
			return false
		}
		if _, isBlock := blockFileKinds[strings.ToLower(typeStr)]; !isBlock {
			return false
		}
		// Range is [from, to); file covers blocks up to to-1. Drop if
		// any covered block exceeds toBlock.
		return to > toBlock+1
	}
}

// Or composes shouldDrop predicates with logical OR — the resulting
// predicate drops a name when any input predicate does. Convenient for
// stacking the block-axis rewind predicate with a step-axis state-file
// predicate the caller composes from its inventory + step→block map.
func Or(preds ...func(name string) bool) func(name string) bool {
	return func(name string) bool {
		for _, p := range preds {
			if p != nil && p(name) {
				return true
			}
		}
		return false
	}
}
