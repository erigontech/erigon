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

package pbt

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/commitment"
)

func TestInitializeTrieAndUpdatesBinUsesRows(t *testing.T) {
	trie, updates, err := commitment.InitializeTrieAndUpdates(commitment.ModeDirect, t.TempDir(), commitment.TrieConfig{Variant: commitment.VariantBinPatriciaTrie})
	require.NoError(t, err)
	defer updates.Close()
	defer trie.Release()

	require.IsType(t, &registeredTrie{}, trie)
	require.Equal(t, commitment.VariantBinPatriciaTrie, trie.Variant())
	_, ok := trie.(interface {
		ProcessPBinFeed(context.Context, *commitment.PBinFeed, func(*commitment.CommitProgress)) ([]byte, error)
	})
	require.True(t, ok)
}

func TestRegisteredTrieStateUsesRowFormat(t *testing.T) {
	trie, updates, err := commitment.InitializeTrieAndUpdates(commitment.ModeDirect, t.TempDir(), commitment.TrieConfig{Variant: commitment.VariantBinPatriciaTrie})
	require.NoError(t, err)
	defer updates.Close()
	defer trie.Release()

	stateful, ok := trie.(commitment.StatefulTrie)
	require.True(t, ok)
	state, err := stateful.EncodeCurrentState(nil)
	require.NoError(t, err)
	require.Equal(t, byte(commitment.PBinStateMarker), state[0])
	require.Equal(t, byte(commitment.PBinRowStateFormat), state[1])
	require.NoError(t, commitment.PBinValidateRowStateFormat(state))
}

func TestRowStateFormatRejectsLegacyFormats(t *testing.T) {
	for format := byte(0); format <= 7; format++ {
		err := commitment.PBinValidateRowStateFormat([]byte{commitment.PBinStateMarker, format})
		require.Error(t, err, "legacy flags %d must be rejected", format)
	}
	require.Error(t, commitment.PBinValidateRowStateFormat([]byte{commitment.PBinStateMarker, 0x10}))
}

func TestInitializeTrieAndUpdatesBinReturnsRegistrationError(t *testing.T) {
	previous := commitment.NewCommitmentBinTrie
	t.Cleanup(func() { commitment.NewCommitmentBinTrie = previous })
	commitment.NewCommitmentBinTrie = nil

	var err error
	require.NotPanics(t, func() {
		_, _, err = commitment.InitializeTrieAndUpdates(commitment.ModeDirect, t.TempDir(), commitment.TrieConfig{Variant: commitment.VariantBinPatriciaTrie})
	})
	require.EqualError(t, err, "commitment: binary trie selected without importing execution/commitment/v3/pbt")
}
