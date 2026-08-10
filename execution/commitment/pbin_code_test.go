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

	keccak "github.com/erigontech/fastkeccak"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/empty"
)

// TestPBinChunkifyCodeVectors checks chunking against the reference's own
// chunkings of chunk_code (eip:"Code").
func TestPBinChunkifyCodeVectors(t *testing.T) {
	t.Parallel()
	v := pbinLoadSpecVectors(t)
	require.NotEmpty(t, v.Chunkify)

	for _, tc := range v.Chunkify {
		t.Run(tc.Name, func(t *testing.T) {
			t.Parallel()
			got := pbinChunkifyCode(pbinMustHex(t, tc.Code))
			require.Len(t, got, len(tc.Chunks))
			for i, want := range tc.Chunks {
				require.Equal(t, pbinMustHex(t, want), got[i][:], "chunk %d", i)
			}
		})
	}
}

// TestPBinChunkifyCodePushdataStraddlesBoundary covers PUSHDATA that begins in
// one chunk and runs into the next: the later chunk's byte 0 counts bytes pushed
// by an opcode it does not contain, which is what a per-chunk scan gets wrong.
func TestPBinChunkifyCodePushdataStraddlesBoundary(t *testing.T) {
	t.Parallel()

	// PUSH32 at offset 30 is the last byte of chunk 0, so its data spans chunks 1 and 2.
	code := append(make([]byte, 30), pbinPush32)
	code = append(code, bytes.Repeat([]byte{0xEE}, 32)...)

	chunks := pbinChunkifyCode(code)
	require.Len(t, chunks, 3)
	require.EqualValues(t, 0, chunks[0][0], "chunk 0 starts on an opcode")
	require.EqualValues(t, 31, chunks[1][0], "a full chunk of PUSHDATA saturates at 31")
	require.EqualValues(t, 1, chunks[2][0], "one PUSHDATA byte carries into chunk 2")
}

// TestPBinChunkifyCode7702Designator covers the shortest code the tree holds: a
// 23-byte EIP-7702 designator is one chunk, zero-padded to the full data length.
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

// TestPBinChunkifyCodeChunkCount pins the sizing the code grouping rests on:
// chunks are ceil(len/31), and MaxCodeSize needs more of them than the 256 one
// code group holds.
func TestPBinChunkifyCodeChunkCount(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct{ size, chunks int }{
		{size: 1, chunks: 1},
		{size: 31, chunks: 1},
		{size: 32, chunks: 2},
		{size: pbinStemSubtreeWidth * pbinChunkDataLen, chunks: pbinStemSubtreeWidth},
		{size: pbinStemSubtreeWidth*pbinChunkDataLen + 1, chunks: pbinStemSubtreeWidth + 1},
		{size: 24576, chunks: 793},
	} {
		require.Len(t, pbinChunkifyCode(make([]byte, tc.size)), tc.chunks, "code of %d bytes", tc.size)
	}
}

// pbinTestCode is deterministic filler of a given length. Every byte is below
// PUSH1, so no chunk carries PUSHDATA, and the fill depends on the length, so
// two different lengths never share a chunk.
func pbinTestCode(n int) []byte {
	code := make([]byte, n)
	for i := range code {
		code[i] = byte(n+i) % pbinPush1
	}
	return code
}

// TestPBinEngineEmitsCodeChunks covers code in the tree: chunks reaching the
// reference leaf set in the content-addressed code zone.
func TestPBinEngineEmitsCodeChunks(t *testing.T) {
	t.Parallel()

	addr := pbinOracleAddr(11)
	code := pbinTestCode(200)
	corpus := new(pbinTestCorpus).accountWithCodeBytes(addr, 4, 500, code)

	// Non-vacuity: the corpus states the fan-out independently of the engine.
	require.Equal(t, 2+7, corpus.leafCount(t), "two header leaves plus ceil(200/31) chunks")

	_, root := corpus.process(t)
	require.Equal(t, corpus.oracleRoot(t), root)
}

// TestPBinCodeChunksFollowHeaderSlots composes one account's code, header slots
// and overflow storage: its leaves span all three zones, and the chunks must
// wait for the walk to leave the account zone.
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

// TestPBinVisitOrderIsMonotonic pins the rule behind the emit order: the grid
// only walks forward, so revisiting a key already left behind is a bug in the
// caller's ordering, not something the fold can absorb.
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
// own value: no state domain holds a chunk, so when a later batch writes into
// the code zone next to an earlier contract's chunks, those chunks have to hash
// from the branch records alone.
func TestPBinCodeChunksSurviveAsRecordSiblings(t *testing.T) {
	t.Parallel()

	first := new(pbinTestCorpus).accountWithCodeBytes(pbinOracleAddr(14), 1, 10, pbinTestCode(62))
	second := new(pbinTestCorpus).accountWithCodeBytes(pbinOracleAddr(24), 1, 20, pbinTestCode(93))

	_, _, root := pbinTestBatches(t, first, second)
	require.Equal(t, pbinTestUnion(first, second).oracleRoot(t), root)
}

// TestPBinRedeployKeepsOldCodeChunks pins the residue a redeploy leaves: chunk
// keys derive from the code hash, so new code names a disjoint leaf set and
// EIP-8297 removes nothing here. A recompute from the state domains cannot know
// the old chunks exist, which is what makes it invalid as an oracle for a
// code-bearing account.
func TestPBinRedeployKeepsOldCodeChunks(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct{ before, after int }{
		{before: 62, after: 31},
		{before: 200, after: 62},
		{before: 31, after: 62}, // growth keeps the residue too: the old hash names other leaves
	} {
		t.Run(fmt.Sprintf("%d bytes to %d", tc.before, tc.after), func(t *testing.T) {
			t.Parallel()

			addr := pbinOracleAddr(15)
			old, next := pbinTestCode(tc.before), pbinTestCode(tc.after)
			deploy := new(pbinTestCorpus).accountWithCodeBytes(addr, 1, 10, old)
			redeploy := new(pbinTestCorpus).accountWithCodeBytes(addr, 2, 10, next)

			_, _, forward := pbinTestBatches(t, deploy, redeploy)

			_, rebuilt := new(pbinTestCorpus).accountWithCodeBytes(addr, 2, 10, next).process(t)
			require.NotEqual(t, rebuilt, forward,
				"a rebuild from state cannot reproduce the stale chunks the forward run kept")

			want := redeploy.entries(t)
			oldHash := keccak.Sum256(old)
			for i, chunk := range pbinChunkifyCode(old) {
				want = append(want, pbinOracleEntry{key: pbinTreeKeyCodeChunk(oldHash, i), value: chunk[:]})
			}
			wantRoot := pbinOracleRoot(want)
			require.Equal(t, wantRoot[:], forward)
		})
	}
}

// TestPBinClearedCodeKeepsChunks takes the same residue down to zero chunks:
// clearing an account's code, as an EIP-7702 delegation reset does, moves the
// header leaves and leaves every chunk behind.
func TestPBinClearedCodeKeepsChunks(t *testing.T) {
	t.Parallel()

	addr := pbinOracleAddr(19)
	designator := pbinTestCode(23) // the size a 7702 designator occupies
	deploy := new(pbinTestCorpus).accountWithCodeBytes(addr, 1, 10, designator)
	cleared := new(pbinTestCorpus).account(addr, 2, 10, empty.CodeHash)

	_, _, forward := pbinTestBatches(t, deploy, cleared)

	want := cleared.entries(t)
	desigHash := keccak.Sum256(designator)
	for i, chunk := range pbinChunkifyCode(designator) {
		want = append(want, pbinOracleEntry{key: pbinTreeKeyCodeChunk(desigHash, i), value: chunk[:]})
	}
	wantRoot := pbinOracleRoot(want)
	require.Equal(t, wantRoot[:], forward, "clearing code leaves its chunks in the tree")

	_, rebuilt := new(pbinTestCorpus).account(addr, 2, 10, empty.CodeHash).process(t)
	require.NotEqual(t, rebuilt, forward, "the state a rebuild reads no longer names the chunks")
}

// TestPBinZeroChunkEmitsNoLeaf pins the absence rule for chunks: a chunk is
// absent only when its whole 32-byte value is zero — 31 zero code bytes and a
// zero PUSHDATA count. The same zero bytes continuing an earlier chunk's PUSH
// keep their leaf, and code_size delimits the code either way.
func TestPBinZeroChunkEmitsNoLeaf(t *testing.T) {
	t.Parallel()

	opcodes := pbinTestCode(31) // every byte below PUSH1, none zero

	t.Run("zero tail chunk is absent", func(t *testing.T) {
		t.Parallel()

		code := append(bytes.Clone(opcodes), make([]byte, 31)...)
		chunks := pbinChunkifyCode(code)
		require.Len(t, chunks, 2)
		require.Equal(t, [pbinValueLength]byte{}, chunks[1])

		corpus := new(pbinTestCorpus).accountWithCodeBytes(pbinOracleAddr(25), 1, 10, code)
		require.Equal(t, 2+1, corpus.leafCount(t), "the zero chunk contributes no leaf")

		_, root := corpus.process(t)
		require.Equal(t, corpus.oracleRoot(t), root)
	})

	t.Run("pushdata continuation keeps the leaf", func(t *testing.T) {
		t.Parallel()

		code := append(bytes.Clone(opcodes[:30]), byte(pbinPushOffset+31))
		code = append(code, make([]byte, 31)...)
		chunks := pbinChunkifyCode(code)
		require.Len(t, chunks, 2)
		require.EqualValues(t, 31, chunks[1][0], "byte 0 counts the PUSH31 data")

		corpus := new(pbinTestCorpus).accountWithCodeBytes(pbinOracleAddr(26), 1, 10, code)
		require.Equal(t, 2+2, corpus.leafCount(t), "the continuation chunk keeps its leaf")

		_, root := corpus.process(t)
		require.Equal(t, corpus.oracleRoot(t), root)
	})
}

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

// pbinCodelessContext hides the concrete state's code read: code is served
// through an optional interface, so embedding PatriciaContext rather than the
// state makes that assertion fail.
type pbinCodelessContext struct{ PatriciaContext }

// TestPBinCodeSizeMustMatchTheCodeBehindIt pins that the two reads agree: the
// BASIC_DATA size and the chunks come from separate reads, so a size that
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
	require.ErrorContains(t, err, "the code domain holds")
}

// TestPBinZoneKeyLengthIsExplicit pins that the zone byte decides the key
// length: an account key and a code key are both 34 bytes, so a code key would
// otherwise pass as an account one. Unallocated zones are refused.
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

// TestPBinLeafValueRoutesByZone covers the same rule at the value encoder: the
// leaf value is picked by the key's zone, so a code-zone key must not be read as
// an account header sub-index.
func TestPBinLeafValueRoutesByZone(t *testing.T) {
	t.Parallel()

	chunk := pbinChunkifyCode(pbinTestCode(31))[0]
	u := Update{Flags: StorageUpdate, StorageLen: pbinValueLength}
	copy(u.Storage[:], chunk[:])

	// A code-zone key at sub-index 0 would be BASIC_DATA if the zone were ignored.
	got, err := pbinLeafValue(pbinTreeKey(pbinCodeZone, make([]byte, 32), 0), &u)
	require.NoError(t, err)
	require.Equal(t, chunk[:], got[:])

	// Inside the account zone, sub-indices past the header storage span are
	// reserved and carry their value verbatim, not as storage.
	got, err = pbinLeafValue(pbinTreeKey(pbinAccountZone, make([]byte, 32), pbinHeaderStorageOffset+pbinHeaderStorageSlots), &u)
	require.NoError(t, err)
	require.Equal(t, chunk[:], got[:])

	// A chunk leaf holding fewer than 32 value bytes cannot be left-padded into
	// place the way a storage value can: byte 0 is the PUSHDATA count.
	short := Update{Flags: StorageUpdate, StorageLen: 4}
	_, err = pbinLeafValue(pbinTreeKeyCodeChunk(keccak.Sum256(pbinTestCode(62)), 1), &short)
	require.ErrorIs(t, err, errPBinCellHash)
}

// TestPBinLeafCellHashChecksZoneLength covers the same rule at the leaf hash: a
// 34-byte storage key or a 66-byte code key is rejected instead of hashed.
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
