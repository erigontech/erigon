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

package communication

import (
	"bytes"
	"math"
	"strings"
	"testing"

	"github.com/golang/snappy"
	"github.com/stretchr/testify/require"
)

func TestMaxWireResponseBytes(t *testing.T) {
	const rawItem = 15 * 1024 * 1024
	perItem := uint64(snappy.MaxEncodedLen(rawItem)) + reqRespChunkFraming

	// A realistic count multiplies out exactly.
	require.Equal(t, 10*perItem, MaxWireResponseBytes(rawItem, 10))

	// The largest count that still fits must not be misreported as overflow.
	maxFit := uint64(math.MaxUint64) / perItem
	require.Equal(t, maxFit*perItem, MaxWireResponseBytes(rawItem, maxFit))

	// A count past that saturates to MaxUint64 instead of wrapping to a small value that would
	// silently truncate a compliant response.
	require.Equal(t, uint64(math.MaxUint64), MaxWireResponseBytes(rawItem, maxFit+1))
	require.Equal(t, uint64(math.MaxUint64), MaxWireResponseBytes(rawItem, math.MaxUint64))
}

// TestMaxWireResponseBytesBoundsStreamEncoding proves the cap never truncates: the responder writes
// each chunk with snappy.NewBufferedWriter (stream framing: a 10-byte stream identifier plus a
// header+CRC per ~64 KiB frame), and snappy.MaxEncodedLen — a snappy *block* bound — must dominate
// that stream output for every item size up to MAX_CHUNK_SIZE, so a compliant chunk is never 413'd.
func TestMaxWireResponseBytesBoundsStreamEncoding(t *testing.T) {
	// xorshift64 fills with incompressible bytes — the worst case for snappy expansion.
	fill := func(b []byte) {
		x := uint64(0x9e3779b97f4a7c15)
		for i := range b {
			x ^= x << 13
			x ^= x >> 7
			x ^= x << 17
			b[i] = byte(x)
		}
	}
	// tiny inputs, a blob sidecar (~129 KiB), 1 MiB, and the spec per-chunk maximum.
	for _, raw := range []int{1, 100, 4096, 131928, 1 << 20, 15 * 1024 * 1024} {
		payload := make([]byte, raw)
		fill(payload)

		var buf bytes.Buffer
		sw := snappy.NewBufferedWriter(&buf)
		_, err := sw.Write(payload)
		require.NoError(t, err)
		require.NoError(t, sw.Flush())
		streamLen := uint64(buf.Len())

		require.LessOrEqualf(t, streamLen, uint64(snappy.MaxEncodedLen(raw)),
			"raw=%d: snappy stream output %d exceeds the block bound %d the budget is built on",
			raw, streamLen, snappy.MaxEncodedLen(raw))

		// The full per-item budget must also cover a worst-case eth2 chunk envelope (result byte +
		// 10-byte length varint + 4 context bytes), which reqRespChunkFraming is sized to absorb.
		const maxEnvelope = 1 + 10 + 4
		require.GreaterOrEqualf(t, MaxWireResponseBytes(raw, 1), streamLen+maxEnvelope,
			"raw=%d: per-item budget must bound stream(%d)+envelope", raw, streamLen)
	}
}

// TestResponseShapeConvention enforces that each entry's MultiChunk flag agrees with the spec
// by_range/_by_root/_by_head naming convention, so a mis-declared protocol fails the build rather
// than silently getting the wrong response-size cap.
func TestResponseShapeConvention(t *testing.T) {
	for _, p := range AllProtocols {
		looksMultiChunk := strings.Contains(p.ID, "_by_range") ||
			strings.Contains(p.ID, "_by_root") ||
			strings.Contains(p.ID, "_by_head")
		require.Equalf(t, looksMultiChunk, p.MultiChunk,
			"protocol %s: MultiChunk=%v contradicts the by_range/_by_root/_by_head naming convention", p.ID, p.MultiChunk)
	}
}

func TestIsMultiChunkProtocol(t *testing.T) {
	require.True(t, IsMultiChunkProtocol(BeaconBlocksByRangeProtocolV2))
	require.True(t, IsMultiChunkProtocol(BeaconBlocksByHeadProtocolV1))
	require.False(t, IsMultiChunkProtocol(StatusProtocolV1))
	// Unknown protocols fail closed to single-object.
	require.False(t, IsMultiChunkProtocol("/eth2/beacon_chain/req/unknown/1/ssz_snappy"))
}
