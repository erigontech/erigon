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

package commitment

import (
	"encoding/binary"
	"testing"

	keccak "github.com/erigontech/fastkeccak"

	"github.com/erigontech/erigon/common"
)

// storageLocsForNibble returns n distinct storage locations whose hashed key (keccak of the slot)
// starts with the given first nibble, so a whale's storage can be laid out under chosen first-nibble
// subtrees. The first storage nibble is the high nibble of keccak(slot)[0] (see KeyToHexNibbleHash).
func storageLocsForNibble(nibble byte, n, seed int) []string {
	out := make([]string, 0, n)
	var s [32]byte
	for i := seed; len(out) < n; i++ {
		binary.BigEndian.PutUint64(s[24:], uint64(i))
		h := keccak.Sum256(s[:])
		if (h[0]>>4)&0xf == nibble {
			out = append(out, common.Bytes2Hex(s[:]))
		}
	}
	return out
}

// A whale's storage collapses (deep fold) to a SINGLE untouched slot under one first nibble: the other
// first-nibble groups are deleted (crossing deepStorageThreshold across >=2 nibbles), leaving the lone
// slot as the surviving child. That survivor is decoded from the on-disk account-prefix branch — it
// carries a cached stateHash and its value is not loaded — so storageRootFromSingleChild returns it as
// a raw leaf. If setAccountStorageRoot drops the cached hash, the later account-leaf hash recompute has
// neither a loaded value nor a memoized hash and fails "storage ... was not loaded as expected".
// Distinct from TestDeepFold_LeafSurvivorCollapse, whose survivor nibble has several slots so its value
// gets loaded during the fold.
func TestDeepFold_SingleSlotSurvivorNotLoaded(t *testing.T) {
	t.Parallel()

	w := findAddressForHexPrefix([]byte{7, 8, 1}, 201)
	s1 := findAddressForHexPrefix([]byte{7, 8, 2}, 202)
	f0 := findAddressForHexPrefix([]byte{0}, 203)
	ff := findAddressForHexPrefix([]byte{0xf}, 204)

	survLoc := storageLocsForNibble(0x1, 1, 1)[0]      // one untouched slot under nibble 1
	delB := storageLocsForNibble(0x2, 700, 1000)       // 700 slots under nibble 2
	delC := storageLocsForNibble(0x3, 700, 1000000)    // 700 slots under nibble 3

	b1 := NewUpdateBuilder().
		Balance(addrHex(w), 100).Balance(addrHex(s1), 5).
		Balance(addrHex(f0), 7).Balance(addrHex(ff), 8).
		Storage(addrHex(w), survLoc, "01")
	for _, loc := range delB {
		b1.Storage(addrHex(w), loc, "01")
	}
	for _, loc := range delC {
		b1.Storage(addrHex(w), loc, "01")
	}
	k1, u1 := b1.Build()

	// Batch 2: delete every nibble-2/3 slot (1400 > deepStorageThreshold across 2 nibbles => deep fold),
	// keeping the untouched nibble-1 slot as the single surviving child.
	b2 := NewUpdateBuilder().Balance(addrHex(w), 200)
	for _, loc := range delB {
		b2.DeleteStorage(addrHex(w), loc)
	}
	for _, loc := range delC {
		b2.DeleteStorage(addrHex(w), loc)
	}
	k2, u2 := b2.Build()

	for _, wk := range []int{1, 4, 8} {
		requireAllEnginesParity(t, k1, u1, k2, u2, wk)
	}
}
