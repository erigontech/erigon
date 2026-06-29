// Copyright 2024 The Erigon Authors
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
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/execution/commitment/trie"
)

// Test_WitnessTracer_CapturedNodesReconstructRoot proves the fold-time tap captures
// the exact consensus node bytes: decoding the full captured node-set rebuilds the
// commitment root. memoizationOff forces every node to be re-hashed so the capture is
// complete.
func Test_WitnessTracer_CapturedNodesReconstructRoot(t *testing.T) {
	ctx := context.Background()
	ms := NewMockState(t)
	hph := NewHexPatriciaHashed(length.Addr, ms, DefaultTrieConfig())
	hph.SetTrace(false)
	hph.memoizationOff = true

	builder := NewUpdateBuilder()
	extAccts, _ := generatePlainKeysWithSameHashPrefix(t, nil, length.Addr, 2, 3)
	for i, a := range extAccts {
		builder.Balance(common.Bytes2Hex(a), uint64(i+1))
	}
	for i := 0; i < 16; i++ {
		a, _ := generateKeyWithHashedPrefix(nil, length.Addr)
		builder.Balance(common.Bytes2Hex(a), uint64(100+i))
	}
	storer := extAccts[0]
	slots, _ := generatePlainKeysWithSameHashPrefix(t, nil, length.Hash, 2, 3)
	for _, sk := range slots {
		builder.Storage(common.Bytes2Hex(storer), common.Bytes2Hex(sk), common.Bytes2Hex(sk))
	}

	plainKeys, updates := builder.Build()
	require.NoError(t, ms.applyPlainUpdates(plainKeys, updates))
	toProcess := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, plainKeys, updates)
	defer toProcess.Close()

	c := newWitnessNodeSet()
	hph.witness.tracer = c
	root, err := hph.Process(ctx, toProcess, "", nil, WarmupConfig{})
	require.NoError(t, err)
	require.NotEmpty(t, c.byHash, "tracer must capture nodes")

	tr, err := trie.RLPDecode(c.nodes(root))
	require.NoError(t, err)
	require.Equal(t, root, tr.Root(), "captured node-set must reconstruct the commitment root")
}
