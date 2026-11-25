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
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/commitment/nibbles"
)

func TestForEachCellRejectsInvalidBranch(t *testing.T) {
	for _, tc := range []struct {
		name string
		data BranchData
		want string
	}{
		{"unknown_fields", branchOfCells(t, cellOf(0x20)), "unknown cell fields"},
		{"field_length_overflow", branchOfCells(t, append(binary.AppendUvarint([]byte{byte(FieldStateHash)}, ^uint64(0)), 0), nil), "past the end"},
		{"trailing_bytes", append(branchOfCells(t), 0), "trailing bytes"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.data.ForEachCell(func(_ int, _ BranchCell) error { return nil })
			require.ErrorContains(t, err, tc.want)
		})
	}
}

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
		l := 30 + rng.Intn(40)
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
	prefix[0] = 0x10
	allocs := testing.AllocsPerRun(1000, func() { _, _ = ContractHashFromPrefix(prefix) })
	require.Zero(t, allocs, "ContractHashFromPrefix must not allocate")
	prefix[0] = 0x00
	allocs = testing.AllocsPerRun(1000, func() { _, _ = ContractHashFromPrefix(prefix) })
	require.Zero(t, allocs, "ContractHashFromPrefix (even) must not allocate")
}
