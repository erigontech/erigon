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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/length"
)

// pbinTestSpecCodeChunkKey transcribes get_tree_key_for_code_chunk
// (eip:355-367) from the spec's Python, hashing with the independent Keccak the
// tests use. It is the ground truth for the cache-backed derivation.
func pbinTestSpecCodeChunkKey(t *testing.T, addr []byte, codeHash common.Hash, chunkID int) []byte {
	t.Helper()
	if chunkID < pbinStemSubtreeWidth-pbinCodeOffset {
		stem := pbinTestKeccak(t, pbinTestAddress32(addr))
		return append(append([]byte{pbinAccountZone}, stem...), byte(pbinCodeOffset+chunkID))
	}
	overflow := chunkID - (pbinStemSubtreeWidth - pbinCodeOffset)
	position := pbinTestKeccak(t, codeHash[:], pbinTestBE32(uint64(overflow/pbinStemSubtreeWidth)))
	key := append(append([]byte{pbinCodeZone}, position...), byte(overflow%pbinStemSubtreeWidth))
	require.Len(t, key, pbinCodeKeyLength)
	return key
}

// TestPBinCodeOverflowKeyMatchesSpec pins the second half of the code embedding:
// past the account header a chunk is content-addressed by code hash, with the
// overflow index split into a 32-byte tree index and a sub-index.
func TestPBinCodeOverflowKeyMatchesSpec(t *testing.T) {
	t.Parallel()

	addr := pbinOracleAddr(60)
	codeHash := common.Hash{0x82, 0x97}

	for _, chunkID := range []int{
		pbinHeaderCodeChunks,                            // the first overflow chunk
		pbinHeaderCodeChunks + 1,                        // its neighbour on the same code stem
		pbinHeaderCodeChunks + pbinStemSubtreeWidth - 1, // the last of the first code stem
		pbinHeaderCodeChunks + pbinStemSubtreeWidth,     // the first of the second
		792, // the last chunk MaxCodeSize produces
	} {
		t.Run(fmt.Sprintf("chunk %d", chunkID), func(t *testing.T) {
			t.Parallel()
			got := pbinTreeKeyCodeOverflow(codeHash, chunkID)
			require.Equal(t, pbinTestSpecCodeChunkKey(t, addr, codeHash, chunkID), got)
			require.Len(t, got, pbinCodeKeyLength)
			require.EqualValues(t, pbinCodeZone, got[0])
		})
	}

	require.Panics(t, func() { pbinTreeKeyCodeOverflow(codeHash, pbinHeaderCodeChunks-1) },
		"a header chunk has no code-zone key")
}

// TestPBinCodeKeyNeverRoutesToTheStorageZone pins that a code key cannot reach
// the storage zone. An overflow key derives from code_hash ‖ tree_index, a
// 64-byte preimage that is not a plain key at all, and the stream's key hasher
// accepts only the two plain-key shapes.
func TestPBinCodeKeyNeverRoutesToTheStorageZone(t *testing.T) {
	t.Parallel()

	codeHash := common.Hash{0x11}
	for chunkID := pbinHeaderCodeChunks; chunkID < pbinHeaderCodeChunks+600; chunkID += 37 {
		key := pbinTreeKeyCodeOverflow(codeHash, chunkID)
		require.EqualValues(t, pbinCodeZone, key[0], "chunk %d", chunkID)
		require.Len(t, key, pbinCodeKeyLength, "chunk %d", chunkID)
	}

	hasher := pbinKeyHasher()
	for _, plainKey := range [][]byte{
		make([]byte, pbinCodeKeyLength),   // a code key handed back as a plain key
		make([]byte, pbinCodeKeyLength-1), // its stem
		make([]byte, 2*length.Hash),       // the overflow preimage itself
	} {
		require.Panics(t, func() { hasher(plainKey) },
			"a %d-byte plain key is neither an account nor a storage key", len(plainKey))
	}
}

// TestPBinEngineCommitsOverflowCodeChunks is the code zone end to end: code
// outgrowing the account header keeps its first 128 chunks on the account stem
// and puts the rest in the code zone.
func TestPBinEngineCommitsOverflowCodeChunks(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		chunks int
	}{
		{name: "one chunk past the header", chunks: pbinHeaderCodeChunks + 1},
		{name: "crosses a code stem", chunks: pbinHeaderCodeChunks + pbinStemSubtreeWidth + 1},
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
}

// TestPBinOverflowChunksAreSharedByIdenticalCode pins the point of
// content-addressing (eip:352-354): two accounts running the same bytecode name
// the same code-zone leaves, so the zone holds one copy of them.
func TestPBinOverflowChunksAreSharedByIdenticalCode(t *testing.T) {
	t.Parallel()

	code := pbinTestCode((pbinHeaderCodeChunks+2)*pbinChunkDataLen - 3)
	a, b := pbinOracleAddr(62), pbinOracleAddr(63)
	corpus := new(pbinTestCorpus).
		accountWithCodeBytes(a, 1, 10, code).
		accountWithCodeBytes(b, 2, 20, code)

	chunks := len(pbinChunkifyCode(code))
	overflow := chunks - pbinHeaderCodeChunks
	require.Equal(t, 2, overflow)
	// Two accounts: four header leaves, two full sets of header chunks, one
	// shared set in the code zone.
	require.Equal(t, 2*(2+pbinHeaderCodeChunks)+overflow, corpus.leafCount(t))

	pph, ms := pbinTestEngine(t)
	corpus.applyTo(t, ms)
	root := pbinTestProcess(t, pph, corpus.plainKeys, corpus.updates)

	require.Equal(t, corpus.oracleRoot(t), root)
	pbinTestVerifyRecords(t, ms, root, corpus.leafCount(t))
}

// TestPBinOverflowChunksFollowEveryAccountZoneKey pins where the code-zone block
// sits in the visit order: the zone byte puts it after every account-header key
// and before every storage-zone one, so the chunks of an account visited early
// have to wait for the last account of the run.
func TestPBinOverflowChunksFollowEveryAccountZoneKey(t *testing.T) {
	t.Parallel()

	code := pbinTestCode((pbinHeaderCodeChunks + 1) * pbinChunkDataLen)
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
