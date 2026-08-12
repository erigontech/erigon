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

// TestDecodeBranchInto_RoundTrip asserts that DecodeBranchInto recovers
// the cells encoded by BranchEncoder.EncodeBranch — the property test that
// keeps the canonical decoder consistent with the canonical encoder.
func TestDecodeBranchInto_RoundTrip(t *testing.T) {
	t.Parallel()
	row, bm, enc := encodeCellRow(t, 16)
	require.NotEmpty(t, enc)

	// EncodeBranch produces bytes WITH the 2-byte touch-map prefix; the
	// decoder consumes the bytes WITHOUT it (matching the unfoldBranchNode
	// call pattern, which strips the touch-map prefix before decoding).
	branchData := []byte(enc)[2:]

	var cells [16]cell
	maps, err := DecodeBranchInto(branchData, false /* not deleted */, &cells)
	require.NoError(t, err)

	// Bitmap should match what was encoded
	require.Equal(t, bm, maps.Bitmap, "decoded bitmap mismatch")
	require.Equal(t, uint16(0), maps.TouchMap, "expected empty touchMap when deleted=false")
	require.Equal(t, bm, maps.AfterMap, "afterMap should equal bitmap when deleted=false")

	// Each present cell should match the original on the fields that
	// survive encode→decode (extension, account/storage addr, hash).
	// hashedExtension etc. are set by deriveHashedKeys (separate step) and
	// are not part of the decoder's responsibility.
	for i, orig := range row {
		requireDecodedCellEq(t, i, orig, &cells[i])
	}
}

// TestDecodeBranchInto_DeletedFlag verifies the touchMap/afterMap convention
// flips correctly with the deleted parameter.
func TestDecodeBranchInto_DeletedFlag(t *testing.T) {
	t.Parallel()
	_, bm, enc := encodeCellRow(t, 16)
	branchData := []byte(enc)[2:]

	var cells [16]cell
	maps, err := DecodeBranchInto(branchData, true, &cells)
	require.NoError(t, err)
	require.Equal(t, bm, maps.Bitmap)
	require.Equal(t, bm, maps.TouchMap, "deleted=true → touchMap = bitmap")
	require.Equal(t, uint16(0), maps.AfterMap, "deleted=true → afterMap = 0")
}

// TestDecodeBranchInto_TruncatedInput asserts the decoder fails cleanly on
// truncated branch data instead of panicking.
func TestDecodeBranchInto_TruncatedInput(t *testing.T) {
	t.Parallel()
	var cells [16]cell

	// Empty data — should fail at bitmap read.
	_, err := DecodeBranchInto(nil, false, &cells)
	require.Error(t, err)

	// Just bitmap, no cells — should be fine if bitmap is 0.
	_, err = DecodeBranchInto([]byte{0x00, 0x00}, false, &cells)
	require.NoError(t, err, "bitmap=0 with no cell data should decode cleanly")

	// Bitmap claims one cell but data missing — should fail.
	_, err = DecodeBranchInto([]byte{0x00, 0x01}, false, &cells)
	require.Error(t, err, "bitmap with set bit but no cell data should error")
}

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
