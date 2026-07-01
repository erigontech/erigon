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
	"testing"

	"github.com/stretchr/testify/require"
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
