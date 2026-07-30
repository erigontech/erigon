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
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestPBinChunkifyCodeVectors is the external check on chunk_code (eip:374-397):
// the reference's own chunkings, hash-independent because chunking is pure byte
// layout.
func TestPBinChunkifyCodeVectors(t *testing.T) {
	t.Parallel()
	v := loadPBinSpecVectors(t)
	require.NotEmpty(t, v.Chunkify)

	for _, tc := range v.Chunkify {
		t.Run(tc.Name, func(t *testing.T) {
			t.Parallel()
			got := pbinChunkifyCode(mustHex(t, tc.Code))
			require.Len(t, got, len(tc.Chunks))
			for i, want := range tc.Chunks {
				require.Equal(t, mustHex(t, want), got[i][:], "chunk %d", i)
			}
		})
	}
}

// TestPBinChunkifyCodePushdataStraddlesBoundary pins the part of the scan a
// per-chunk implementation gets wrong: PUSHDATA that begins in one chunk and
// runs into the next, so the later chunk's byte 0 counts bytes pushed by an
// opcode it does not contain.
func TestPBinChunkifyCodePushdataStraddlesBoundary(t *testing.T) {
	t.Parallel()

	// PUSH32 at offset 30 is the last byte of chunk 0, so all 32 of its data
	// bytes land in chunk 1 and 31 of them are still PUSHDATA at chunk 2.
	code := append(make([]byte, 30), pbinPush32)
	code = append(code, bytes.Repeat([]byte{0xEE}, 32)...)

	chunks := pbinChunkifyCode(code)
	require.Len(t, chunks, 3)
	require.EqualValues(t, 0, chunks[0][0], "chunk 0 starts on an opcode")
	require.EqualValues(t, 31, chunks[1][0], "a full chunk of PUSHDATA saturates at 31")
	require.EqualValues(t, 1, chunks[2][0], "one PUSHDATA byte carries into chunk 2")
}

// TestPBinChunkifyCode7702Designator covers the shortest code the tree holds:
// an EIP-7702 designator is 23 bytes, one padded chunk whose first byte is the
// 0xEF marker rather than PUSHDATA.
func TestPBinChunkifyCode7702Designator(t *testing.T) {
	t.Parallel()

	designator := append([]byte{0xEF, 0x01, 0x00}, bytes.Repeat([]byte{0xAB}, 20)...)
	require.Len(t, designator, 23)

	chunks := pbinChunkifyCode(designator)
	require.Len(t, chunks, 1)
	require.EqualValues(t, 0, chunks[0][0])
	require.Equal(t, designator, chunks[0][1:1+len(designator)])
	require.Equal(t, make([]byte, pbinChunkDataLen-len(designator)), chunks[0][1+len(designator):],
		"the tail is zero-padded, not left uninitialised")
}

func TestPBinChunkifyCodeEmpty(t *testing.T) {
	t.Parallel()
	require.Empty(t, pbinChunkifyCode(nil))
	require.Empty(t, pbinChunkifyCode([]byte{}))
}

// TestPBinChunkifyCodeChunkCount pins the sizing the header/overflow split rests
// on: chunks are ceil(len/31), and MaxCodeSize needs more than the 128 the
// account header holds.
func TestPBinChunkifyCodeChunkCount(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct{ size, chunks int }{
		{size: 1, chunks: 1},
		{size: 31, chunks: 1},
		{size: 32, chunks: 2},
		{size: pbinHeaderCodeChunks * pbinChunkDataLen, chunks: pbinHeaderCodeChunks},
		{size: pbinHeaderCodeChunks*pbinChunkDataLen + 1, chunks: pbinHeaderCodeChunks + 1},
		{size: 24576, chunks: 793},
	} {
		require.Len(t, pbinChunkifyCode(make([]byte, tc.size)), tc.chunks, "code of %d bytes", tc.size)
	}
}

// pbinTestCode is deterministic filler of a given length. Every byte is below
// PUSH1, so no chunk carries PUSHDATA and a root mismatch cannot be blamed on
// the scan the vector tests already pin. The fill depends on the length, so two
// different lengths never share a chunk.
func pbinTestCode(n int) []byte {
	code := make([]byte, n)
	for i := range code {
		code[i] = byte(n+i) % pbinPush1
	}
	return code
}

// TestPBinEngineEmitsHeaderCodeChunks is the first half of code in the tree: a
// code-bearing account's chunks have to reach the leaf set the reference tree
// builds for it, at the header sub-indices CODE_OFFSET.. .
func TestPBinEngineEmitsHeaderCodeChunks(t *testing.T) {
	t.Parallel()

	addr := pbinOracleAddr(11)
	code := pbinTestCode(200)
	corpus := new(pbinTestCorpus).accountWithCodeBytes(addr, 4, 500, code)

	// Non-vacuity: the corpus states the fan-out independently of the engine.
	require.Equal(t, 2+7, corpus.leafCount(t), "two header leaves plus ceil(200/31) chunks")

	_, root := corpus.process(t)
	require.Equal(t, corpus.oracleRoot(t), root)
}

// TestPBinCodeChunksFollowHeaderSlots guards H5: chunks sit at the top
// sub-indices of the stem, so emitting them at the account's own visit descends
// past a header storage slot the stream has not delivered yet, and the fold that
// comes back for it rewrites a record it had already written.
func TestPBinCodeChunksFollowHeaderSlots(t *testing.T) {
	t.Parallel()

	addr := pbinOracleAddr(12)
	corpus := new(pbinTestCorpus).
		accountWithCodeBytes(addr, 1, 2, pbinTestCode(200)).
		storage(addr, pbinOracleSlot(5), 0x77).
		storage(addr, pbinOracleSlot(63), 0x88).
		storage(addr, pbinOracleSlot(1000), 0x99)

	_, root := corpus.process(t)
	require.Equal(t, corpus.oracleRoot(t), root)
}

// TestPBinVisitOrderIsMonotonic is the structural assert behind H5: the grid only
// walks forward, so a visit that revisits a key already left behind is a bug in
// the caller's ordering, not something the fold can absorb.
func TestPBinVisitOrderIsMonotonic(t *testing.T) {
	t.Parallel()

	pph, _ := pbinTestEngine(t)
	addr := pbinOracleAddr(13)
	u := Update{Flags: NonceUpdate}

	require.NoError(t, pph.followAndUpdate(pbinTreeKeyAccount(addr, pbinCodeHashLeafKey), addr, &u))
	err := pph.followAndUpdate(pbinTreeKeyAccount(addr, pbinBasicDataLeafKey), addr, &u)
	require.ErrorIs(t, err, errPBinVisitOrder)
}

// TestPBinCodeChunksSurviveAsRecordSiblings pins that a chunk leaf carries its
// own value: an untouched chunk sibling of a touched one has to hash from the
// branch record, and no state domain holds a chunk.
func TestPBinCodeChunksSurviveAsRecordSiblings(t *testing.T) {
	t.Parallel()

	addr := pbinOracleAddr(14)
	// Two chunks, then a shorter code touching only chunk 0: chunk 1 stays behind
	// as a direct leaf sibling, which is the one shape that must reload its value.
	deploy := new(pbinTestCorpus).accountWithCodeBytes(addr, 1, 10, pbinTestCode(62))
	stale := pbinChunkifyCode(pbinTestCode(62))[1]
	redeploy := new(pbinTestCorpus).accountWithCodeBytes(addr, 2, 10, pbinTestCode(31))

	_, _, root := pbinTestBatches(t, deploy, redeploy)

	want := append(redeploy.entries(t), pbinOracleEntry{
		key:   pbinTreeKeyCodeChunk(addr, 1),
		value: stale[:],
	})
	wantRoot := pbinOracleRoot(want)
	require.Equal(t, wantRoot[:], root, "the untouched chunk keeps the value the record holds")
}

// TestPBinShorteningRedeployKeepsStaleChunks records the answer to Q2 as a test
// (guards H8). EIP-8297 has no removal, so a redeploy to shorter code leaves the
// chunks above the new length in place: a forward run commits them, a recompute
// from the state domains cannot know they exist. The two roots are each
// internally consistent and different, which is what makes recompute-from-domains
// invalid as an oracle for a code-bearing account.
func TestPBinShorteningRedeployKeepsStaleChunks(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct{ before, after int }{
		{before: 62, after: 31},  // 2 chunks down to 1: the residue is a leaf sibling
		{before: 200, after: 62}, // 7 down to 2: the residue is a whole subtree
	} {
		t.Run(fmt.Sprintf("%d bytes down to %d", tc.before, tc.after), func(t *testing.T) {
			t.Parallel()

			addr := pbinOracleAddr(15)
			long, short := pbinTestCode(tc.before), pbinTestCode(tc.after)
			deploy := new(pbinTestCorpus).accountWithCodeBytes(addr, 1, 10, long)
			redeploy := new(pbinTestCorpus).accountWithCodeBytes(addr, 2, 10, short)

			_, _, forward := pbinTestBatches(t, deploy, redeploy)

			_, rebuilt := new(pbinTestCorpus).accountWithCodeBytes(addr, 2, 10, short).process(t)
			require.NotEqual(t, rebuilt, forward,
				"a rebuild from state cannot reproduce the stale chunks the forward run kept")

			// The residue is exactly the chunks the old code had and the new one does not.
			want := redeploy.entries(t)
			oldChunks := pbinChunkifyCode(long)
			for i := len(pbinChunkifyCode(short)); i < len(oldChunks); i++ {
				want = append(want, pbinOracleEntry{key: pbinTreeKeyCodeChunk(addr, i), value: oldChunks[i][:]})
			}
			wantRoot := pbinOracleRoot(want)
			require.Equal(t, wantRoot[:], forward)
		})
	}
}

// TestPBinCodelessContextRefusesCodeBearingAccount pins that the code read is
// not optional: a context that cannot serve code cannot commit an account whose
// chunks the tree needs.
func TestPBinCodelessContextRefusesCodeBearingAccount(t *testing.T) {
	t.Parallel()

	addr := pbinOracleAddr(17)
	corpus := new(pbinTestCorpus).accountWithCodeBytes(addr, 1, 10, pbinTestCode(62))

	ms := NewMockState(t)
	corpus.applyTo(t, ms)
	pph := NewPBinPatriciaHashed(pbinCodelessContext{ms})
	upd := WrapKeyUpdates(t, ModeDirect, pbinKeyHasher(), corpus.plainKeys, corpus.updates)

	_, err := pph.Process(context.Background(), upd, "", nil, WarmupConfig{})
	require.ErrorIs(t, err, ErrPBinUnsupported)
}

// pbinCodelessContext is a PatriciaContext with no code read, which embedding the
// interface rather than the concrete state is what produces.
type pbinCodelessContext struct{ PatriciaContext }

// TestPBinCodeSizeMustMatchTheCodeBehindIt pins that the two reads agree: the
// BASIC_DATA size and the chunks come from separate reads, and a size that
// disagrees with the code would commit a leaf set no reference tree holds.
func TestPBinCodeSizeMustMatchTheCodeBehindIt(t *testing.T) {
	t.Parallel()

	addr := pbinOracleAddr(18)
	corpus := new(pbinTestCorpus).accountWithCodeBytes(addr, 1, 10, pbinTestCode(62))

	pph, ms := pbinTestEngine(t)
	corpus.applyTo(t, ms)
	ms.setCode(addr, pbinTestCode(31)) // the account still says 62 bytes

	upd := WrapKeyUpdates(t, ModeDirect, pbinKeyHasher(), corpus.plainKeys, corpus.updates)
	_, err := pph.Process(context.Background(), upd, "", nil, WarmupConfig{})
	require.Error(t, err)
}

// TestPBinZoneKeyLengthIsExplicit pins that the code zone is recognised rather
// than passing as an account key because both are 34 bytes, and that the zones
// the embedding has not allocated are refused.
func TestPBinZoneKeyLengthIsExplicit(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		zone  byte
		want  int
		known bool
	}{
		{zone: pbinAccountZone, want: pbinAccountKeyLength, known: true},
		{zone: pbinCodeZone, want: pbinCodeKeyLength, known: true},
		{zone: pbinStorageZone, want: pbinStorageKeyLength, known: true},
		{zone: 0x02}, {zone: 0x7F}, {zone: 0xFE},
	} {
		got, known := pbinZoneKeyLength(tc.zone)
		require.Equal(t, tc.known, known, "zone %#x", tc.zone)
		require.Equal(t, tc.want, got, "zone %#x", tc.zone)
	}

	require.Panics(t, func() { pbinTreeKey(0x02, make([]byte, 32), 0) }, "an unallocated zone has no key length")
	require.Len(t, pbinTreeKey(pbinCodeZone, make([]byte, 32), 0), pbinCodeKeyLength)
}

// TestPBinLeafValueRoutesByZone pins the second place a code key used to pass by
// accident: the leaf value is picked by the key's zone, so a code-zone key must
// not be read as an account header sub-index.
func TestPBinLeafValueRoutesByZone(t *testing.T) {
	t.Parallel()

	chunk := pbinChunkifyCode(pbinTestCode(31))[0]
	u := Update{Flags: StorageUpdate, StorageLen: pbinValueLength}
	copy(u.Storage[:], chunk[:])

	// A code-zone key at sub-index 0 would be BASIC_DATA if the zone were ignored.
	got, err := pbinLeafValue(pbinTreeKey(pbinCodeZone, make([]byte, 32), 0), &u)
	require.NoError(t, err)
	require.Equal(t, chunk[:], got[:])

	// The same is true inside the account zone: sub-indices at CODE_OFFSET and
	// above are chunks, not storage.
	addr := pbinOracleAddr(19)
	got, err = pbinLeafValue(pbinTreeKeyCodeChunk(addr, 0), &u)
	require.NoError(t, err)
	require.Equal(t, chunk[:], got[:])

	// A chunk leaf holding fewer than 32 value bytes cannot be left-padded into
	// place the way a storage value can: byte 0 is the PUSHDATA count.
	short := Update{Flags: StorageUpdate, StorageLen: 4}
	_, err = pbinLeafValue(pbinTreeKeyCodeChunk(addr, 1), &short)
	require.ErrorIs(t, err, errPBinCellHash)
}

// TestPBinLeafCellHashChecksZoneLength pins the third site: a leaf's key length
// has to match its own zone, so a 34-byte storage key or a 66-byte code key is
// rejected instead of hashing.
func TestPBinLeafCellHashChecksZoneLength(t *testing.T) {
	t.Parallel()

	var h pbinHasher
	u := Update{Flags: StorageUpdate, StorageLen: pbinValueLength}

	for _, tc := range []struct {
		name string
		key  []byte
	}{
		{name: "storage zone at account length", key: append([]byte{pbinStorageZone}, make([]byte, pbinAccountKeyLength-1)...)},
		{name: "code zone at storage length", key: append([]byte{pbinCodeZone}, make([]byte, pbinStorageKeyLength-1)...)},
		{name: "unallocated zone", key: append([]byte{0x02}, make([]byte, pbinAccountKeyLength-1)...)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := pbinCell{kind: pbinNodeLeaf, prefix: pbinPathFromBytes(tc.key), Update: u}
			var path pbinBitpath
			_, err := h.cellHash(&c, &path)
			require.ErrorIs(t, err, errPBinCellHash)
		})
	}
}
