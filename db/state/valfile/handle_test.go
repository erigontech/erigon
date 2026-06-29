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

package valfile

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHandleRoundTrip(t *testing.T) {
	t.Parallel()
	cases := []Handle{
		{Offset: 4},
		{Offset: 0},
		{Offset: 1 << 20},
		{Offset: 1<<40 - 1},
	}
	for _, h := range cases {
		enc := h.AppendTo(nil)
		got, n, err := DecodeHandle(enc)
		require.NoError(t, err)
		require.Equal(t, len(enc), n)
		require.Equal(t, h, got)
	}
}

func TestHandleFixedWidth(t *testing.T) {
	t.Parallel()
	// Every handle must encode to exactly HandleSize bytes regardless of magnitude,
	// so an external DupSort record is a constant size (enables in-place rewrite).
	cases := []Handle{
		{Offset: 0},
		{Offset: 4},
		{Offset: 1<<40 - 1},
	}
	for _, h := range cases {
		require.Len(t, h.AppendTo(nil), HandleSize)
	}
}

func TestHandleAppendPreservesPrefix(t *testing.T) {
	t.Parallel()
	prefix := []byte{0xde, 0xad}
	h := Handle{Offset: 99}
	enc := h.AppendTo(prefix)
	require.Equal(t, prefix, enc[:2])
	got, n, err := DecodeHandle(enc[2:])
	require.NoError(t, err)
	require.Equal(t, len(enc)-2, n)
	require.Equal(t, h, got)
}

func TestDecodeHandleTruncated(t *testing.T) {
	t.Parallel()
	enc := Handle{Offset: 1 << 30}.AppendTo(nil)
	_, _, err := DecodeHandle(enc[:1]) // fewer than HandleSize bytes
	require.Error(t, err)
	_, _, err = DecodeHandle(nil)
	require.Error(t, err)
}
