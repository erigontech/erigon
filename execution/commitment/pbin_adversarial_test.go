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

	"github.com/erigontech/erigon/common/empty"
)

// Intra-batch sequences and the group-boundary shape the vendored corpus does
// not reach. No vector pins them, so the canonical-rebuild oracle is the
// reference throughout. A key touched twice in one corpus is one batch touching
// it twice: state and oracle both keep the last write.

func pbinTestIndicator(fill byte) []byte {
	return append([]byte{0xEF, 0x01, 0x00}, bytes.Repeat([]byte{fill}, 20)...)
}

func TestPBinDelegationSetAndClearedInOneBatch(t *testing.T) {
	t.Parallel()

	addr := pbinOracleAddr(101)
	indicator := pbinTestIndicator(0x33)
	corpus := new(pbinTestCorpus).
		accountWithCodeBytes(addr, 1, 10, indicator).
		accountWithCodeBytes(addr, 2, 10, nil)
	_, root := corpus.process(t)

	cleared := new(pbinTestCorpus).account(addr, 2, 10, empty.CodeHash)
	require.Equal(t, cleared.oracleRoot(t), root,
		"a delegation set and cleared inside one batch ends at the empty-code CODE_HASH leaf")
	require.Equal(t, corpus.oracleRoot(t), root)

	delegation := pbinEncodeDelegation(indicator)
	leftBehind := append(cleared.entries(t),
		pbinOracleEntry{key: pbinTreeKeyAccount(addr, pbinDelegationLeafKey), value: delegation[:]})
	wrong := pbinOracleRoot(leftBehind)
	require.NotEqual(t, wrong[:], root, "the mid-batch indicator must not survive the clear")
}

func TestPBinDelegationRepointedInOneBatch(t *testing.T) {
	t.Parallel()

	addr := pbinOracleAddr(102)
	prior, mid, final := pbinTestIndicator(0x44), pbinTestIndicator(0x55), pbinTestIndicator(0x66)
	stored := new(pbinTestCorpus).accountWithCodeBytes(addr, 1, 10, prior)
	repoint := new(pbinTestCorpus).
		accountWithCodeBytes(addr, 2, 10, mid).
		accountWithCodeBytes(addr, 3, 10, final)
	_, _, root := pbinTestBatches(t, stored, repoint)

	want := new(pbinTestCorpus).accountWithCodeBytes(addr, 3, 10, final)
	require.Equal(t, want.oracleRoot(t), root,
		"two authorizations in one batch leave one delegation leaf holding the last target")

	stale := new(pbinTestCorpus).accountWithCodeBytes(addr, 3, 10, mid)
	require.NotEqual(t, stale.oracleRoot(t), root, "the earlier target must not survive the repoint")

	asCode := new(pbinTestCorpus).accountWithCode(addr, 3, 10, keccak.Sum256(final), uint64(len(final)))
	require.NotEqual(t, asCode.oracleRoot(t), root, "no code-hash leaf may appear for a delegated account")
}

func TestPBinZeroChunkAloneInItsGroup(t *testing.T) {
	t.Parallel()

	addr := pbinOracleAddr(103)
	code := append(pbinTestCode(pbinStemSubtreeWidth*pbinChunkDataLen), make([]byte, pbinChunkDataLen)...)
	chunks := pbinChunkifyCode(code)
	require.Len(t, chunks, pbinStemSubtreeWidth+1)
	require.Equal(t, [pbinValueLength]byte{}, chunks[pbinStemSubtreeWidth],
		"the sole chunk of group 1 must be all-zero, PUSHDATA count included")

	corpus := new(pbinTestCorpus).accountWithCodeBytes(addr, 1, 5, code)
	_, root := corpus.process(t)
	require.Equal(t, corpus.oracleRoot(t), root,
		"a zero chunk alone in its tree_index group leaves the group with no leaf at all")

	withLeaf := append(corpus.entries(t), pbinOracleEntry{
		key:   pbinTreeKeyCodeChunk(keccak.Sum256(code), pbinStemSubtreeWidth),
		value: make([]byte, pbinValueLength),
	})
	wrong := pbinOracleRoot(withLeaf)
	require.NotEqual(t, wrong[:], root, "materializing the zero chunk as a leaf must change the root")
}

func TestPBinSharedCodeOutlivesOneHolder(t *testing.T) {
	t.Parallel()

	holder, doomed := pbinOracleAddr(104), pbinOracleAddr(105)
	code := pbinTestCode(31 * 3)
	both := new(pbinTestCorpus).
		accountWithCodeBytes(holder, 1, 5, code).
		accountWithCodeBytes(doomed, 2, 7, code)

	pph, ms := pbinTestEngine(t)
	both.applyTo(t, ms)
	pbinTestProcess(t, pph, both.plainKeys, both.updates)

	removal := [][]byte{doomed}
	require.NoError(t, ms.applyPlainUpdates(removal, []Update{{Flags: DeleteUpdate}}))
	pph.Reset()
	root := pbinTestProcess(t, pph, removal, []Update{{Flags: DeleteUpdate}})

	survivor := new(pbinTestCorpus).accountWithCodeBytes(holder, 1, 5, code)
	require.Equal(t, survivor.oracleRoot(t), root,
		"deleting one holder leaves the shared chunk set with the other")

	noChunks := new(pbinTestCorpus).accountWithCode(holder, 1, 5, keccak.Sum256(code), uint64(len(code)))
	require.NotEqual(t, noChunks.oracleRoot(t), root, "the survivor's chunks must not go with the removed holder")
}
