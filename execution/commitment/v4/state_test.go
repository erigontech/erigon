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

package v4

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/execution/commitment"
)

func TestStateRoundTrip(t *testing.T) {
	trie := &Trie{root: bytes.Repeat([]byte{0x37}, 32)}
	encoded, err := trie.EncodeState(91, 73, []byte{0xaa})
	require.NoError(t, err)
	require.Equal(t, commitment.CommitmentV4StateMarker, encoded[1])

	restored := &Trie{}
	blockNum, txNum, err := restored.RestoreState(encoded[1:])
	require.NoError(t, err)
	require.Equal(t, uint64(91), blockNum)
	require.Equal(t, uint64(73), txNum)
	root, err := restored.RootHash()
	require.NoError(t, err)
	require.Equal(t, trie.root, root)
}

func TestStateRoundTripEmptyRoot(t *testing.T) {
	trie := &Trie{}
	encoded, err := trie.EncodeState(1, 2, nil)
	require.NoError(t, err)

	restored := &Trie{root: bytes.Repeat([]byte{0x55}, 32)}
	_, _, err = restored.RestoreState(encoded)
	require.NoError(t, err)
	root, err := restored.RootHash()
	require.NoError(t, err)
	require.Equal(t, empty.RootHash[:], root)
}

func TestStateRejectsLegacyAndMalformedBlobs(t *testing.T) {
	trie := &Trie{}
	legacy := make([]byte, commitment.CommitmentV4StateSize)
	legacy[0] = commitment.CommitmentV4StateMarker - 1
	_, _, err := trie.RestoreState(legacy)
	require.ErrorIs(t, err, commitment.ErrCommitmentV4StateMarker)

	for _, value := range [][]byte{{commitment.CommitmentV4StateMarker}, make([]byte, commitment.CommitmentV4StateSize-1), make([]byte, commitment.CommitmentV4StateSize+1)} {
		_, _, err = trie.RestoreState(value)
		require.ErrorIs(t, err, commitment.ErrCommitmentV4StateSize)
	}

	badMarker := make([]byte, commitment.CommitmentV4StateSize)
	badMarker[0] = commitment.CommitmentV4StateMarker + 1
	_, _, err = trie.RestoreState(badMarker)
	require.ErrorIs(t, err, commitment.ErrCommitmentV4StateMarker)
}

func TestStateEncodeRejectsInvalidRoot(t *testing.T) {
	_, err := (&Trie{root: []byte{1}}).EncodeState(0, 0, nil)
	require.ErrorIs(t, err, commitment.ErrCommitmentV4StateSize)
}
