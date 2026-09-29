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

package jsonrpc

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/rpc"
)

func TestResolveWitnessRequest(t *testing.T) {
	stringPtr := func(value string) *string { return &value }
	tests := []struct {
		name        string
		mode        *string
		trie        *string
		binTrie     bool
		wantTrie    witnessTrie
		wantMode    witnessMode
		wantError   string
		wantInvalid bool
	}{
		{name: "omitted mpt", wantTrie: witnessTrieMPT, wantMode: witnessModeLegacy},
		{name: "omitted pbt", binTrie: true, wantTrie: witnessTriePBT, wantMode: witnessModeLegacy},
		{name: "explicit mpt post-fork", trie: stringPtr("mpt"), binTrie: true, wantTrie: witnessTrieMPT, wantMode: witnessModeLegacy},
		{name: "explicit pbt pre-fork", trie: stringPtr("pbt"), wantTrie: witnessTriePBT, wantMode: witnessModeLegacy},
		{name: "pbt with legacy mode", trie: stringPtr("pbt"), mode: stringPtr("legacy"), wantError: "mode applies to the MPT witness only"},
		{name: "pbt with canonical mode", trie: stringPtr("pbt"), mode: stringPtr("canonical"), wantError: "mode applies to the MPT witness only"},
		{name: "unknown trie", trie: stringPtr("binary"), wantInvalid: true},
		{name: "mpt legacy mode", trie: stringPtr("mpt"), mode: stringPtr("legacy"), wantTrie: witnessTrieMPT, wantMode: witnessModeLegacy},
		{name: "mpt canonical after fork", trie: stringPtr("mpt"), mode: stringPtr("canonical"), binTrie: true, wantTrie: witnessTrieMPT, wantMode: witnessModeCanonical},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := resolveWitnessRequest(tt.mode, tt.trie, tt.binTrie)
			if tt.wantError != "" {
				require.ErrorContains(t, err, tt.wantError)
				return
			}
			if tt.wantInvalid {
				var invalid *rpc.InvalidParamsError
				require.ErrorAs(t, err, &invalid)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.wantTrie, got.trie)
			require.Equal(t, tt.wantMode, got.mode)
		})
	}
}

func TestWitnessCacheRoutesByTrie(t *testing.T) {
	api, m := pbinWitnessFixture(t, 30)
	block := rpc.BlockNumber(2)
	var hash common.Hash
	require.NoError(t, m.DB.View(t.Context(), func(tx kv.Tx) error {
		var err error
		hash, _, err = m.BlockReader.CanonicalHash(t.Context(), tx, uint64(block))
		return err
	}))
	sentinel := &ExecutionWitnessResult{State: []hexutil.Bytes{{0xde, 0xad}}}
	cache := newWitnessResultCache(96, 0, false, false)
	cache.Add(hash, sentinel)
	api.witnessCache = cache
	t.Cleanup(func() { api.witnessCache = nil })
	tx, err := api.db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	selector := rpc.BlockNumberOrHashWithNumber(block)
	result, hit, reorgedAway := api.serveFromWitnessCache(t.Context(), tx, selector, witnessModeLegacy, witnessTrieMPT, witnessTrieMPT)
	require.True(t, hit)
	require.False(t, reorgedAway)
	require.Same(t, sentinel, result)

	result, hit, reorgedAway = api.serveFromWitnessCache(t.Context(), tx, selector, witnessModeLegacy, witnessTriePBT, witnessTrieMPT)
	require.False(t, hit)
	require.False(t, reorgedAway)
	require.Nil(t, result)
}

func TestWitnessCacheOnlyRejectsNonDefaultTrie(t *testing.T) {
	api, _ := pbinWitnessFixture(t, 30)
	api.witnessCache = newWitnessResultCache(96, 0, true, true)
	t.Cleanup(func() { api.witnessCache = nil })
	block := rpc.BlockNumber(3)
	mpt := "mpt"
	result, err := api.ExecutionWitness(t.Context(), rpc.BlockNumberOrHashWithNumber(block), nil, &mpt)
	require.ErrorIs(t, err, errWitnessTrieUnavailable)
	require.Nil(t, result)
}

func TestExecutionWitnessPBTNotServed(t *testing.T) {
	api, _ := pbinWitnessFixture(t, 30)
	pbt := "pbt"
	tests := []struct {
		name  string
		block rpc.BlockNumber
		trie  *string
	}{
		{name: "pre-fork explicit", block: 2, trie: &pbt},
		{name: "post-fork explicit", block: 3, trie: &pbt},
		{name: "post-fork default", block: 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := api.ExecutionWitness(t.Context(), rpc.BlockNumberOrHashWithNumber(tt.block), nil, tt.trie)
			require.ErrorIs(t, err, errWitnessPBTNotServed)
			require.Nil(t, result)
		})
	}
}
