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

package state_test

import (
	"bytes"
	"sort"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
	"github.com/erigontech/erigon/execution/commitment/eip8297/artifact"
)

func TestForEachPBinArtifactLeafStreamsEmptyArtifact(t *testing.T) {
	var snapshot bytes.Buffer
	_, err := artifact.WriteSnapshotStream(&snapshot, func(func([]byte, []byte) error) error { return nil }, func() (common.Hash, error) {
		return eip8297.EmptyTreeHash, nil
	})
	require.NoError(t, err)
	count := 0
	root, err := state.ForEachPBinArtifactLeaf(bytes.NewReader(snapshot.Bytes()), int64(snapshot.Len()), eip8297.HashBytes, func(state.PBinLeaf) error {
		count++
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, eip8297.EmptyTreeHash, root)
	require.Zero(t, count)
}

func TestForEachPBinArtifactLeafKeepsCodeZoneLeaves(t *testing.T) {
	var balance uint256.Int
	balance.SetUint64(1)
	entries := eip8297.EmbedState([][]eip8297.State{{
		{Address: []byte{1}, Balance: balance, Code: []byte{1, 2, 3}},
	}})
	sort.Slice(entries, func(i, j int) bool { return bytes.Compare(entries[i].Key, entries[j].Key) < 0 })
	wantRoot := eip8297.StateRootWithHash(entries, eip8297.HashBytes)
	var snapshot bytes.Buffer
	_, err := artifact.WriteSnapshotStream(&snapshot, func(emit func([]byte, []byte) error) error {
		for _, entry := range entries {
			if err := emit(entry.Key, entry.Value); err != nil {
				return err
			}
		}
		return nil
	}, func() (common.Hash, error) { return wantRoot, nil })
	require.NoError(t, err)
	var got []state.PBinLeaf
	root, err := state.ForEachPBinArtifactLeaf(bytes.NewReader(snapshot.Bytes()), int64(snapshot.Len()), eip8297.HashBytes, func(leaf state.PBinLeaf) error {
		got = append(got, leaf)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, wantRoot, root)
	require.Equal(t, len(entries), len(got))
	codeLeaves := 0
	for _, leaf := range got {
		if leaf.Key[0] == eip8297.CodeZone {
			codeLeaves++
		}
	}
	require.NotZero(t, codeLeaves, "the imported artifact must retain code-zone leaves")
}
