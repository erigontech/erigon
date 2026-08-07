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
	"bytes"
	"testing"

	keccak "github.com/erigontech/fastkeccak"
	"github.com/stretchr/testify/require"
)

// TestPBinIsDelegationClassifiesByBytes pins that classification reads the code
// bytes and nothing else: 23 bytes opening with the marker. Code whose keccak
// hash begins with the marker is still code.
func TestPBinIsDelegationClassifiesByBytes(t *testing.T) {
	t.Parallel()

	marker := []byte{0xEF, 0x01, 0x00}
	indicator := append(bytes.Clone(marker), bytes.Repeat([]byte{0xAB}, 20)...)
	require.True(t, pbinIsDelegation(indicator))

	hashGrindsToMarker := pbinMustHex(t, "0x0000000000000000000000000000000000000000637401")
	require.Len(t, hashGrindsToMarker, pbinDelegationCodeLength)
	h := keccak.Sum256(hashGrindsToMarker)
	require.Equal(t, marker, h[:3], "the ground value must still hash to the marker")
	require.False(t, pbinIsDelegation(hashGrindsToMarker))

	require.False(t, pbinIsDelegation(append(bytes.Clone(marker), bytes.Repeat([]byte{0xAB}, 19)...)))
	require.False(t, pbinIsDelegation(append(bytes.Clone(marker), bytes.Repeat([]byte{0xAB}, 21)...)))
	require.False(t, pbinIsDelegation(marker))
	require.False(t, pbinIsDelegation(nil))
}

func TestPBinEncodeDelegationPadsToThirtyTwo(t *testing.T) {
	t.Parallel()

	indicator := append([]byte{0xEF, 0x01, 0x00}, bytes.Repeat([]byte{0xCD}, 20)...)
	v := pbinEncodeDelegation(indicator)
	require.Equal(t, indicator, v[:pbinDelegationCodeLength])
	require.Equal(t, make([]byte, pbinValueLength-pbinDelegationCodeLength), v[pbinDelegationCodeLength:])

	chunk := pbinChunkifyCode(indicator)[0]
	require.NotEqual(t, chunk, v,
		"an indicator is not chunk-encoded: byte 0 carries code, not a PUSHDATA count")
}
