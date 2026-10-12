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

package eip8297

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestChunkifyCodePushdataStraddlesBoundary(t *testing.T) {
	code := append(make([]byte, 30), Push32)
	code = append(code, bytes.Repeat([]byte{0xEE}, 32)...)

	chunks := ChunkifyCode(code)
	require.Len(t, chunks, 3)
	require.EqualValues(t, 0, chunks[0][0])
	require.EqualValues(t, 31, chunks[1][0])
	require.EqualValues(t, 1, chunks[2][0])
}

func TestChunkifyCode7702Designator(t *testing.T) {
	designator := append([]byte{0xEF, 0x01, 0x00}, bytes.Repeat([]byte{0xAB}, 20)...)
	require.Len(t, designator, 23)

	chunks := ChunkifyCode(designator)
	require.Len(t, chunks, 1)
	require.EqualValues(t, 0, chunks[0][0])
	require.Equal(t, designator, chunks[0][1:1+len(designator)])
	require.Equal(t, make([]byte, ChunkDataLen-len(designator)), chunks[0][1+len(designator):])
}

func TestChunkifyCodeEmpty(t *testing.T) {
	require.Empty(t, ChunkifyCode(nil))
	require.Empty(t, ChunkifyCode([]byte{}))
}

func TestChunkifyCodeCount(t *testing.T) {
	for _, tc := range []struct{ size, chunks int }{
		{size: 1, chunks: 1},
		{size: 31, chunks: 1},
		{size: 32, chunks: 2},
		{size: StemSubtreeWidth * ChunkDataLen, chunks: StemSubtreeWidth},
		{size: StemSubtreeWidth*ChunkDataLen + 1, chunks: StemSubtreeWidth + 1},
		{size: 24576, chunks: 793},
	} {
		require.Len(t, ChunkifyCode(make([]byte, tc.size)), tc.chunks, "code of %d bytes", tc.size)
	}
}

func TestChunkifyCodeEndsInTruncatedPush(t *testing.T) {
	for _, immediates := range []int{0, 1} {
		tail := append([]byte{Push1 + 1}, bytes.Repeat([]byte{0xAA}, immediates)...)
		code := append(bytes.Repeat([]byte{0x5B}, ChunkDataLen), tail...)
		require.Len(t, code, ChunkDataLen+1+immediates)

		chunks := ChunkifyCode(code)
		require.Len(t, chunks, 2)
		require.EqualValues(t, 0, chunks[0][0])
		require.EqualValues(t, 0, chunks[1][0])
		require.Equal(t, tail, chunks[1][1:1+len(tail)])
		require.Equal(t, make([]byte, ChunkDataLen-len(tail)), chunks[1][1+len(tail):])
	}
}
