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

package cas20

import (
	"testing"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
)

// A length word no write could have produced once crashed both read paths.
func TestCAS20StringLengthWordIsNotTrusted(t *testing.T) {
	word := func(build func(*common.Hash)) common.Hash {
		var h common.Hash
		build(&h)
		return h
	}
	for _, tc := range []struct {
		name string
		word common.Hash
	}{
		{"short string claiming 100 bytes", word(func(h *common.Hash) { h[31] = 200 })},
		{"short string claiming 127 bytes", word(func(h *common.Hash) { h[31] = 254 })},
		{"long string of almost 2^255 bytes", word(func(h *common.Hash) {
			for i := range h {
				h[i] = 0xff
			}
		})},
		{"long string one byte past the cap", common.Hash(
			uint256.NewInt(2*(cas20MaxStringLen+1) + 1).Bytes32())},
		// A long-form word claiming under 32 bytes is as non-canonical as an oversized one.
		{"long string claiming one byte", common.Hash(uint256.NewInt(3).Bytes32())},
		{"long string claiming 31 bytes", common.Hash(uint256.NewInt(2*31 + 1).Bytes32())},
	} {
		t.Run(tc.name, func(t *testing.T) {
			statedb := newTestStateDB(t)
			s := newUnmeteredCAS20Storage(statedb, cas20Addr(cas20VariantAsset, 1))
			statedb.SetState(s.token, slotAt(cas20SlotName), tc.word)

			if got := strOf(s.name()); got != "" {
				t.Errorf("name() = %q, want the empty string", got)
			}
			// Fatal, not Errorf: an unbounded count would hang the unmetered release loop below.
			if got := s.stringChunks(slotAt(cas20SlotName)); got != 0 {
				t.Fatalf("stringChunks = %d, want 0 — setStringAt would release that "+
					"many slots, and nothing would stop it", got)
			}
			// A token whose state arrived that way must be repairable, not bricked.
			s.setName("ok")
			if got := strOf(s.name()); got != "ok" {
				t.Errorf("name() after setName = %q, want %q", got, "ok")
			}
		})
	}

	// The longest string a test can afford is far below the cap, so check the boundary arithmetic directly.
	statedb := newTestStateDB(t)
	s := newUnmeteredCAS20Storage(statedb, cas20Addr(cas20VariantAsset, 2))
	atCap := common.Hash(uint256.NewInt(2*cas20MaxStringLen + 1).Bytes32())
	statedb.SetState(s.token, slotAt(cas20SlotName), atCap)
	if got := s.stringChunks(slotAt(cas20SlotName)); got != cas20MaxStringLen/32 {
		t.Errorf("a string exactly at the cap reports %d chunks, want %d — the bound is "+
			"off by one and rejects a length it should accept", got, cas20MaxStringLen/32)
	}
}
