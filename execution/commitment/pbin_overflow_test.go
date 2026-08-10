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
	"encoding/hex"
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/length"
)

// pbinTestSpecCodeChunkKey transcribes get_tree_key_for_code_chunk
// (eip:"Code") from the spec's Python, hashing with the independent Keccak the
// tests use. It is the ground truth for the cache-backed derivation.
func pbinTestSpecCodeChunkKey(t *testing.T, codeHash common.Hash, chunkID int) []byte {
	t.Helper()
	position := pbinTestKeccak(t, codeHash[:], pbinTestBE32(uint64(chunkID/pbinStemSubtreeWidth)))
	key := append(append([]byte{pbinCodeZone}, position...), byte(chunkID%pbinStemSubtreeWidth))
	require.Len(t, key, pbinCodeKeyLength)
	return key
}

// TestPBinChunkKeyMatchesSpec pins the code embedding: every chunk is
// content-addressed by code hash, with the chunk id split into a 32-byte tree
// index and a sub-index.
func TestPBinChunkKeyMatchesSpec(t *testing.T) {
	t.Parallel()

	codeHash := common.Hash{0x82, 0x97}

	for _, chunkID := range []int{
		0,
		1,
		pbinStemSubtreeWidth - 1, // the last of the first code group
		pbinStemSubtreeWidth,     // the first of the second
		792,                      // the last chunk MaxCodeSize produces
	} {
		t.Run(fmt.Sprintf("chunk %d", chunkID), func(t *testing.T) {
			t.Parallel()
			got := pbinTreeKeyCodeChunk(codeHash, chunkID)
			require.Equal(t, pbinTestSpecCodeChunkKey(t, codeHash, chunkID), got)
			require.Len(t, got, pbinCodeKeyLength)
			require.EqualValues(t, pbinCodeZone, got[0])
		})
	}

	require.Panics(t, func() { pbinTreeKeyCodeChunk(codeHash, -1) },
		"a negative chunk id names no key")
}

// TestPBinChunkKeyMatchesVectorIndices pins the derivation against the
// reference corpus at every chunk id the corpus carries — both sides of the
// 255/256 and 511/512 group boundaries, and the last chunk of MAX_CODE_SIZE.
func TestPBinChunkKeyMatchesVectorIndices(t *testing.T) {
	e := pbinLoadConformance(t).Embedding
	codeHash := common.BytesToHash(pbinUnhex(t, e.CodeHash))
	keys := pbinDigestCache{sum: pbinBlake3Hash}

	wantIDs := []int{0, 1, 255, 256, 257, 511, 512, 2114}
	require.Len(t, e.CodeChunkKeys, len(wantIDs))
	for _, id := range wantIDs {
		want, ok := e.CodeChunkKeys[strconv.Itoa(id)]
		require.True(t, ok, "the corpus carries no chunk %d", id)
		require.Equal(t, want, "0x"+hex.EncodeToString(keys.codeChunkKey(codeHash, id)), "chunk %d", id)
	}
}

// TestPBinChunkKeyIgnoresAddress: the derivation takes no address, so a digest
// cache warmed on an account stem must not leak its memoized digests into a
// chunk key.
func TestPBinChunkKeyIgnoresAddress(t *testing.T) {
	t.Parallel()

	codeHash := common.Hash{0x82, 0x97}
	var a, b pbinDigestCache
	a.accountKey(pbinOracleAddr(60), pbinBasicDataLeafKey)
	b.accountKey(pbinOracleAddr(61), pbinBasicDataLeafKey)

	for _, chunkID := range []int{0, pbinStemSubtreeWidth - 1, pbinStemSubtreeWidth, 2114} {
		fresh := pbinTreeKeyCodeChunk(codeHash, chunkID)
		require.Equal(t, fresh, a.codeChunkKey(codeHash, chunkID), "chunk %d", chunkID)
		require.Equal(t, fresh, b.codeChunkKey(codeHash, chunkID), "chunk %d", chunkID)
	}
}

// TestPBinCodeKeyNeverRoutesToTheStorageZone pins that a code key cannot reach
// the storage zone. A chunk key derives from code_hash ‖ tree_index, a 64-byte
// preimage that is not a plain key at all, and the stream's key hasher accepts
// only the two plain-key shapes.
func TestPBinCodeKeyNeverRoutesToTheStorageZone(t *testing.T) {
	t.Parallel()

	codeHash := common.Hash{0x11}
	for chunkID := 0; chunkID < 600; chunkID += 37 {
		key := pbinTreeKeyCodeChunk(codeHash, chunkID)
		require.EqualValues(t, pbinCodeZone, key[0], "chunk %d", chunkID)
		require.Len(t, key, pbinCodeKeyLength, "chunk %d", chunkID)
	}

	hasher := pbinKeyHasher()
	for _, plainKey := range [][]byte{
		make([]byte, pbinCodeKeyLength),   // a code key handed back as a plain key
		make([]byte, pbinCodeKeyLength-1), // its stem
		make([]byte, 2*length.Hash),       // the chunk-position preimage itself
	} {
		require.Panics(t, func() { hasher(plainKey) },
			"a %d-byte plain key is neither an account nor a storage key", len(plainKey))
	}
}

// TestPBinChunksCrossGroupBoundary is the code zone end to end: chunk 256 opens
// a second code group on its own stem, and the engine commits both groups.
func TestPBinChunksCrossGroupBoundary(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		chunks int
	}{
		{name: "fills group 0", chunks: pbinStemSubtreeWidth},
		{name: "one chunk into group 1", chunks: pbinStemSubtreeWidth + 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			addr := pbinOracleAddr(61)
			code := pbinTestCode((tc.chunks-1)*pbinChunkDataLen + 1)
			corpus := new(pbinTestCorpus).accountWithCodeBytes(addr, 3, 7, code)
			require.Equal(t, 2+tc.chunks, corpus.leafCount(t))

			pph, ms := pbinTestEngine(t)
			corpus.applyTo(t, ms)
			root := pbinTestProcess(t, pph, corpus.plainKeys, corpus.updates)

			require.Equal(t, corpus.oracleRoot(t), root)
			pbinTestVerifyRecords(t, ms, root, corpus.leafCount(t))
		})
	}

	h := common.Hash{0xAB}
	last, first := pbinTreeKeyCodeChunk(h, pbinStemSubtreeWidth-1), pbinTreeKeyCodeChunk(h, pbinStemSubtreeWidth)
	require.NotEqual(t, last[1:33], first[1:33], "group 1 sits on its own stem")
	require.EqualValues(t, pbinStemSubtreeWidth-1, last[33])
	require.EqualValues(t, 0, first[33], "the sub-index wraps at the group boundary")
}

// TestPBinSharedBytecodeEmitsOneChunkSet pins the point of content addressing
// (eip:"Code"): two accounts running the same bytecode name the same code-zone
// leaves, so the zone holds one copy of them.
func TestPBinSharedBytecodeEmitsOneChunkSet(t *testing.T) {
	t.Parallel()

	code := pbinTestCode((pbinStemSubtreeWidth+2)*pbinChunkDataLen - 3)
	a, b := pbinOracleAddr(62), pbinOracleAddr(63)
	corpus := new(pbinTestCorpus).
		accountWithCodeBytes(a, 1, 10, code).
		accountWithCodeBytes(b, 2, 20, code)

	chunks := len(pbinChunkifyCode(code))
	require.Equal(t, pbinStemSubtreeWidth+2, chunks)
	// Two accounts: four header leaves and one shared chunk set spanning two
	// code groups.
	require.Equal(t, 2*2+chunks, corpus.leafCount(t))

	pph, ms := pbinTestEngine(t)
	corpus.applyTo(t, ms)
	root := pbinTestProcess(t, pph, corpus.plainKeys, corpus.updates)

	require.Equal(t, corpus.oracleRoot(t), root)
	pbinTestVerifyRecords(t, ms, root, corpus.leafCount(t))
}

// TestPBinCodeChunksFollowEveryAccountZoneKey pins where the code-zone block
// sits in the visit order: the zone byte puts it after every account-header key
// and before every storage-zone one, so the chunks of an account visited early
// have to wait for the last account of the run.
func TestPBinCodeChunksFollowEveryAccountZoneKey(t *testing.T) {
	t.Parallel()

	code := pbinTestCode(5 * pbinChunkDataLen)
	early := pbinOracleAddr(64)
	corpus := new(pbinTestCorpus).accountWithCodeBytes(early, 1, 10, code)
	for i := uint64(65); i < 70; i++ {
		addr := pbinOracleAddr(i)
		corpus.account(addr, i, i*2, common.Hash{byte(i)}).
			storage(addr, pbinOracleSlot(7), 0x01).
			storage(addr, pbinOracleSlot(4096), 0x02)
	}

	pph, ms := pbinTestEngine(t)
	corpus.applyTo(t, ms)
	root := pbinTestProcess(t, pph, corpus.plainKeys, corpus.updates)

	require.Equal(t, corpus.oracleRoot(t), root)
	pbinTestVerifyRecords(t, ms, root, corpus.leafCount(t))
}
