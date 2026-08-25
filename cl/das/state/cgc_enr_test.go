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

package peerdasstate

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// fulu p2p-interface.md: cgc is a "Uint64 big endian integer with no leading zero
// bytes (0 is encoded as empty byte string)".
func TestEncodeCgcMatchesTheEnrSpec(t *testing.T) {
	for _, tc := range []struct {
		cgc  uint64
		want []byte
	}{
		{0, []byte{}},
		{4, []byte{0x04}},
		{8, []byte{0x08}},
		{255, []byte{0xff}},
		{256, []byte{0x01, 0x00}},
	} {
		got := EncodeCgc(tc.cgc)
		require.Equal(t, tc.want, got, "cgc %d", tc.cgc)
		if len(got) > 0 {
			require.NotZero(t, got[0], "cgc %d has a leading zero byte", tc.cgc)
		}
	}
}
