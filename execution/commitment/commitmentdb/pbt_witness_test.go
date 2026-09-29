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

package commitmentdb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/eip8297/witness"
)

type pbinWitnessDispatchTrie struct {
	commitment.Trie
	variant commitment.TrieVariant
	called  bool
}

func (t *pbinWitnessDispatchTrie) Variant() commitment.TrieVariant { return t.variant }

func (t *pbinWitnessDispatchTrie) Witness(context.Context, common.Hash, witness.PBinDriverInput) ([][]byte, [][]byte, common.Hash, error) {
	t.called = true
	return [][]byte{{1}}, [][]byte{{2}}, common.Hash{3}, nil
}

func TestPBinWitnessDispatchesBinaryDomains(t *testing.T) {
	for _, tc := range []struct {
		name   string
		domain kv.Domain
	}{
		{name: "bin-only", domain: kv.CommitmentDomain},
		{name: "hex+bin", domain: kv.CommitmentBinDomain},
	} {
		t.Run(tc.name, func(t *testing.T) {
			trie := &pbinWitnessDispatchTrie{variant: commitment.VariantBinPatriciaTrie}
			sdc := &SharedDomainsCommitmentContext{commitmentDomain: tc.domain, variant: commitment.VariantBinPatriciaTrie, patriciaTrie: trie}
			paths, blobs, root, err := sdc.PBinWitness(context.Background(), common.Hash{}, witness.PBinDriverInput{})
			require.NoError(t, err)
			require.True(t, trie.called)
			require.Equal(t, [][]byte{{1}}, paths)
			require.Equal(t, [][]byte{{2}}, blobs)
			require.Equal(t, common.Hash{3}, root)
		})
	}
}

func TestPBinWitnessDispatchRefusesHex(t *testing.T) {
	trie := &pbinWitnessDispatchTrie{variant: commitment.VariantCommitmentV3}
	sdc := &SharedDomainsCommitmentContext{commitmentDomain: kv.CommitmentDomain, variant: commitment.VariantCommitmentV3, patriciaTrie: trie}
	_, _, _, err := sdc.PBinWitness(context.Background(), common.Hash{}, witness.PBinDriverInput{})
	require.ErrorContains(t, err, "cannot build PBT witnesses")
	require.False(t, trie.called)
}
