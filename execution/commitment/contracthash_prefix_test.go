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
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/commitment/nibbles"
)

// reference is the pre-optimization implementation kept as the oracle: it
// materializes the full hex expansion via CompactToHex, then repacks the first
// 32 bytes. The zero-alloc ContractHashFromPrefix must agree with it exactly.
func contractHashFromPrefixReference(prefix []byte) (hash [32]byte, ok bool) {
	if len(prefix) < 33 {
		return hash, false
	}
	nib := nibbles.CompactToHex(prefix)
	if len(nib) < 64 {
		return hash, false
	}
	for i := range 32 {
		hash[i] = nib[2*i]<<4 | nib[2*i+1]
	}
	return hash, true
}

func TestContractHashFromPrefix_MatchesReference(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	for range 5000 {
		l := 30 + rng.Intn(40) // spans below and above the 33-byte minimum
		prefix := make([]byte, l)
		rng.Read(prefix)
		wantHash, wantOK := contractHashFromPrefixReference(prefix)
		gotHash, gotOK := ContractHashFromPrefix(prefix)
		require.Equalf(t, wantOK, gotOK, "ok mismatch len=%d prefix0=%#x", l, prefixByte0(prefix))
		require.Equalf(t, wantHash, gotHash, "hash mismatch len=%d prefix0=%#x", l, prefixByte0(prefix))
	}
}

func prefixByte0(p []byte) byte {
	if len(p) == 0 {
		return 0
	}
	return p[0]
}

func TestContractHashFromPrefix_ZeroAlloc(t *testing.T) {
	prefix := make([]byte, 40)
	prefix[0] = 0x10 // odd flag set, to exercise the shifting branch
	allocs := testing.AllocsPerRun(1000, func() { _, _ = ContractHashFromPrefix(prefix) })
	require.Zero(t, allocs, "ContractHashFromPrefix must not allocate")
	prefix[0] = 0x00 // even branch
	allocs = testing.AllocsPerRun(1000, func() { _, _ = ContractHashFromPrefix(prefix) })
	require.Zero(t, allocs, "ContractHashFromPrefix (even) must not allocate")
}
