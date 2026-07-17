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
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/execution/commitment/trie"
)

type capturedNode struct{ rlp, hash string }

type recordingTracer struct{ nodes []capturedNode }

func (r *recordingTracer) onNode(rlp, hash []byte) {
	r.nodes = append(r.nodes, capturedNode{rlp: string(rlp), hash: string(hash)})
}

// Test_witness_capture exercises the witness helper directly: an inactive witness
// passes the keccak writer through untouched and emits nothing, while an active one
// tees leaf bytes through leafBuf and accumulates a branch from its prefix and slots.
func Test_witness_capture(t *testing.T) {
	var w witness
	var sink bytes.Buffer

	// inactive: passthrough writer, emits are no-ops, no panic on nil tracer
	require.False(t, w.active())
	require.Same(t, &sink, w.leafWriter(&sink))
	w.emitLeaf([]byte("x"))
	w.beginBranch([]byte("y"))
	w.writeBranch([]byte("z"))
	w.emitBranch([]byte("w"))

	rec := &recordingTracer{}
	w.tracer = rec
	require.True(t, w.active())

	lw := w.leafWriter(&sink)
	_, _ = lw.Write([]byte("leaf-rlp"))
	w.emitLeaf([]byte("leaf-hash"))

	w.beginBranch([]byte("pre"))
	w.writeBranch([]byte("-slot1"))
	w.writeBranch([]byte("-slot2"))
	w.emitBranch([]byte("branch-hash"))

	require.Equal(t, []capturedNode{
		{rlp: "leaf-rlp", hash: "leaf-hash"},
		{rlp: "pre-slot1-slot2", hash: "branch-hash"},
	}, rec.nodes)

	w.reset()
	require.False(t, w.active())
}

// Test_WitnessTracer_CapturedNodesReconstructRoot proves the fold-time tap captures
// the exact consensus node bytes: decoding the full captured node-set rebuilds the
// commitment root. memoizationOff forces every node to be re-hashed so the capture is
// complete.
func Test_WitnessTracer_CapturedNodesReconstructRoot(t *testing.T) {
	ms := NewMockState(t)
	hph := NewHexPatriciaHashed(length.Addr, ms, DefaultTrieConfig())
	hph.SetTraceWriter(nil)
	hph.memoizationOff = true

	builder := NewUpdateBuilder()
	extAccts, _ := generatePlainKeysWithSameHashPrefix(t, nil, length.Addr, 2, 3)
	for i, a := range extAccts {
		builder.Balance(common.Bytes2Hex(a), uint64(i+1))
	}
	for i := range 16 {
		a, _ := generateKeyWithHashedPrefix(nil, length.Addr)
		builder.Balance(common.Bytes2Hex(a), uint64(100+i))
	}
	storer := extAccts[0]
	slots, _ := generatePlainKeysWithSameHashPrefix(t, nil, length.Hash, 2, 3)
	for _, sk := range slots {
		builder.Storage(common.Bytes2Hex(storer), common.Bytes2Hex(sk), common.Bytes2Hex(sk))
	}

	plainKeys, updates := builder.Build()

	c := newWitnessNodeSet()
	hph.witness.tracer = c
	root := processBatch(t, ms, hph, plainKeys, updates)
	require.NotEmpty(t, c.byHash, "tracer must capture nodes")

	nodeSet, err := c.nodes(root)
	require.NoError(t, err)
	tr, err := trie.RLPDecode(nodeSet)
	require.NoError(t, err)
	require.Equal(t, root, tr.Root(), "captured node-set must reconstruct the commitment root")
}
