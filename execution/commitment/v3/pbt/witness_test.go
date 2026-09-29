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
	"bytes"
	"context"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
	"github.com/erigontech/erigon/execution/commitment/eip8297/witness"
)

func TestPBinWitnessMatchesModel(t *testing.T) {
	pbinUseBlake3(t)
	address := []byte{1}
	slot := pbinResolverSlot(4096)
	entries := []eip8297.Entry{
		{Key: eip8297.TreeKeyAccount(address, eip8297.BasicDataLeafKey), Value: pbinWitnessValueBytes(1)},
		{Key: eip8297.TreeKeyAccount([]byte{2}, eip8297.BasicDataLeafKey), Value: pbinWitnessValueBytes(2)},
		{Key: eip8297.TreeKeyStorage([]byte{3}, pbinResolverSlot(64)), Value: pbinWitnessValueBytes(3)},
	}
	sort.Slice(entries, func(i, j int) bool { return bytes.Compare(entries[i].Key, entries[j].Key) < 0 })
	preContext := newTrieTestContext()
	_, err := NewTrie(preContext).Process(entriesToOps(entries))
	require.NoError(t, err)
	preRoot, err := NewTrie(preContext).RootHash()
	require.NoError(t, err)

	value := pbinWitnessValueBytes(0xee)
	input := witness.PBinDriverInput{
		Reads:    [][]byte{eip8297.TreeKeyAccount([]byte{2}, eip8297.BasicDataLeafKey), eip8297.TreeKeyStorage([]byte{3}, pbinResolverSlot(64))},
		Storage:  []witness.PBinStorageWrite{{Address: address, Slot: slot, Value: value}},
		Accounts: []witness.PBinAccountUpdate{{Address: address, Values: map[byte][]byte{eip8297.BasicDataLeafKey: pbinWitnessValueBytes(0xab)}}},
	}

	resolver := NewPBinWitnessResolver(preContext)
	model, err := witness.NewPBinTree(preRoot, resolver.Resolve)
	require.NoError(t, err)
	wantRoot, wantNodes, err := model.Apply(input)
	require.NoError(t, err)
	wantPaths := make([][]byte, len(wantNodes))
	wantBlobs := make([][]byte, len(wantNodes))
	for index, node := range wantNodes {
		wantPaths[index] = node.Path
		wantBlobs[index] = node.Blob
	}

	gotPaths, gotBlobs, gotRoot, err := NewTrie(preContext).Witness(context.Background(), preRoot, input)
	require.NoError(t, err)
	require.Equal(t, wantPaths, gotPaths, "set comparison paths")
	require.Equal(t, wantBlobs, gotBlobs, "set comparison blobs")
	require.Equal(t, wantRoot, gotRoot, "post-root comparison")

	postContext := newTrieTestContext()
	postContext.records = cloneRecords(preContext.records)
	postOps := []Op{
		{Key: eip8297.TreeKeyStorage(address, slot), Value: pbinWitnessValueArray(value)},
		{Key: eip8297.TreeKeyAccount(address, eip8297.BasicDataLeafKey), Value: valueArray(pbinWitnessValueBytes(0xab))},
	}
	sort.Slice(postOps, func(i, j int) bool { return bytes.Compare(postOps[i].Key, postOps[j].Key) < 0 })
	engineRoot, err := NewTrie(postContext).Process(postOps)
	require.NoError(t, err)
	require.Equal(t, engineRoot, gotRoot, "engine post-root comparison")
}

func TestPBinWitnessUsesExpectedParentRoot(t *testing.T) {
	pbinUseBlake3(t)
	entries := pbinResolverEntries()[:8]
	parent := newTrieTestContext()
	_, err := NewTrie(parent).Process(entriesToOps(entries))
	require.NoError(t, err)
	parentRoot, err := NewTrie(parent).RootHash()
	require.NoError(t, err)
	latest := newTrieTestContext()
	latest.records = cloneRecords(parent.records)
	rewrite := entries[0]
	rewrite.Value = testTrieValueBytes(0x88)
	_, err = NewTrie(latest).Process(entriesToOps([]eip8297.Entry{rewrite}))
	require.NoError(t, err)
	history := &pbinResolverHistoryContext{latest: latest, parent: parent.records}
	paths, blobs, postRoot, err := NewTrie(history).Witness(context.Background(), parentRoot, witness.PBinDriverInput{})
	require.NoError(t, err)
	require.NotEmpty(t, paths, "parent anchor must build a root witness")
	wantNodes, _ := pbinOracleNodes(t, entries)
	require.Equal(t, wantNodes[0].blob, blobs[0], "parent witness root blob")
	require.Equal(t, parentRoot, postRoot, "an empty driver must preserve the expected parent root")
}

func TestPBinWitnessRejectsWrongExpectedRoot(t *testing.T) {
	pbinUseBlake3(t)
	entries := pbinResolverEntries()[:8]
	ctx := newTrieTestContext()
	_, err := NewTrie(ctx).Process(entriesToOps(entries))
	require.NoError(t, err)
	_, _, _, err = NewTrie(ctx).Witness(context.Background(), common.Hash{0xee}, witness.PBinDriverInput{})
	require.ErrorContains(t, err, "want")
}

func pbinWitnessValueBytes(seed byte) []byte {
	value := make([]byte, eip8297.ValueLength)
	value[eip8297.ValueLength-1] = seed
	return value
}

func pbinWitnessValueArray(value []byte) (result [eip8297.ValueLength]byte) {
	copy(result[:], value)
	return result
}
