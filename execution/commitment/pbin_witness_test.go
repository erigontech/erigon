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

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/length"
)

// pbinWitnessRecorder keeps every emission in arrival order, so a node hashed
// more than once stays visible instead of being folded away.
type pbinWitnessRecorder struct {
	preimages [][]byte
	hashes    [][]byte
}

func (r *pbinWitnessRecorder) onNode(preimage, hash []byte) {
	r.preimages = append(r.preimages, bytes.Clone(preimage))
	r.hashes = append(r.hashes, bytes.Clone(hash))
}

// byHash folds the emissions into the node set a witness carries and checks the
// property that set relies on: one hash, one preimage.
func (r *pbinWitnessRecorder) byHash(t *testing.T) map[string][]byte {
	t.Helper()
	out := make(map[string][]byte, len(r.hashes))
	for i, hash := range r.hashes {
		if prev, seen := out[string(hash)]; seen {
			require.Equal(t, prev, r.preimages[i], "hash %x emitted with two preimages", hash)
			continue
		}
		out[string(hash)] = r.preimages[i]
	}
	return out
}

// pbinWitnessRejectingTracer fails the test on any emission; it stands in for a
// tracer that must have been detached.
type pbinWitnessRejectingTracer struct{ t *testing.T }

func (r *pbinWitnessRejectingTracer) onNode(preimage, hash []byte) {
	r.t.Helper()
	r.t.Fatalf("detached tracer received node %x", hash)
}

// pbinWitnessOracleNodes enumerates the reference tree's nodes as
// preimage-by-hash, derived from the corpus rather than from the engine.
func pbinWitnessOracleNodes(t *testing.T, entries []pbinOracleEntry) map[string][]byte {
	t.Helper()
	var tree pbinOracleTree
	for _, e := range entries {
		tree.insert(e.key, e.value)
	}
	out := make(map[string][]byte)
	pbinWitnessCollectOracleNodes(t, tree.root, out)
	return out
}

func pbinWitnessCollectOracleNodes(t *testing.T, node pbinOracleNode, out map[string][]byte) []byte {
	t.Helper()
	if node == nil {
		return make([]byte, length.Hash)
	}
	var preimage []byte
	switch n := node.(type) {
	case *pbinOracleLeaf:
		preimage = append(preimage, pbinOracleLeafTag)
		preimage = append(preimage, n.key...)
		preimage = append(preimage, n.value...)
	case *pbinOracleBranch:
		left := pbinWitnessCollectOracleNodes(t, n.left, out)
		right := pbinWitnessCollectOracleNodes(t, n.right, out)
		preimage = append(preimage, pbinOracleBranchTag)
		preimage = append(preimage, pbinOracleEncodeBitPrefix(n.prefix)...)
		preimage = append(preimage, left...)
		preimage = append(preimage, right...)
	default:
		t.Fatalf("unknown oracle node %T", node)
	}
	hash := pbinTestKeccak(t, preimage)
	out[string(hash)] = preimage
	return hash
}

// pbinWitnessCorpus spans both zones and carries code, so the emitted set holds
// BASIC_DATA, CODE_HASH, code-chunk and storage leaves.
func pbinWitnessCorpus() *pbinTestCorpus {
	c := new(pbinTestCorpus)
	c.accountWithCodeBytes(pbinOracleAddr(21), 1, 500, bytes.Repeat([]byte{0x60}, 200))
	c.account(pbinOracleAddr(22), 2, 900, common.Hash{0x22})
	for _, slot := range []uint64{0, 63, 64, 256, 1 << 20} {
		c.storage(pbinOracleAddr(21), pbinOracleSlot(slot), 0x11)
		c.storage(pbinOracleAddr(22), pbinOracleSlot(slot), 0x22)
	}
	return c
}

func pbinWitnessProcess(t *testing.T, corpus *pbinTestCorpus, tracer witnessTracer) ([]byte, *PBinPatriciaHashed) {
	t.Helper()
	pph, ms := pbinTestEngine(t)
	corpus.applyTo(t, ms)
	pph.setWitnessTracer(tracer)
	return pbinTestProcess(t, pph, corpus.plainKeys, corpus.updates), pph
}

// TestPBinWitnessTracerNilEmitsNothing: the tap is inert without a tracer, and
// detaching one really detaches it.
func TestPBinWitnessTracerNilEmitsNothing(t *testing.T) {
	t.Parallel()

	corpus := pbinWitnessCorpus()
	pph, ms := pbinTestEngine(t)
	corpus.applyTo(t, ms)

	pph.setWitnessTracer(&pbinWitnessRejectingTracer{t: t})
	pph.setWitnessTracer(nil)

	root := pbinTestProcess(t, pph, corpus.plainKeys, corpus.updates)
	require.Equal(t, corpus.oracleRoot(t), root)
}

// TestPBinWitnessTracerEmitsEveryNode: a traced fold yields every node of the
// tree it builds, each one hashing to the hash it was emitted with, and the
// root is unchanged by the tracing.
func TestPBinWitnessTracerEmitsEveryNode(t *testing.T) {
	t.Parallel()

	corpus := pbinWitnessCorpus()
	untracedRoot, _ := pbinWitnessProcess(t, corpus, nil)

	rec := new(pbinWitnessRecorder)
	root, _ := pbinWitnessProcess(t, corpus, rec)
	require.Equal(t, untracedRoot, root)
	require.Equal(t, corpus.oracleRoot(t), root)

	tags := map[byte]int{}
	for i, preimage := range rec.preimages {
		require.NotEmpty(t, preimage)
		require.Equal(t, pbinTestKeccak(t, preimage), rec.hashes[i], "emission %d does not hash to its own preimage", i)
		tags[preimage[0]]++
	}
	require.Positive(t, tags[pbinLeafTag], "no leaf node emitted")
	require.Positive(t, tags[pbinBranchTag], "no branch node emitted")

	emitted := rec.byHash(t)
	require.Contains(t, emitted, string(root), "root node absent from the emitted set")
	for hash, preimage := range pbinWitnessOracleNodes(t, corpus.entries(t)) {
		got, ok := emitted[hash]
		require.True(t, ok, "node %x of the reference tree was never emitted", hash)
		require.Equal(t, preimage, got)
	}
}

// TestPBinWitnessTracerCoversRootLeaf: a one-key tree folds no row, so its only
// node is hashed by RootHash. A tap in foldBranch would emit nothing here.
func TestPBinWitnessTracerCoversRootLeaf(t *testing.T) {
	t.Parallel()

	addr, slot := pbinOracleAddr(31), pbinOracleSlot(7000)
	corpus := new(pbinTestCorpus).storage(addr, slot, 0x01, 0x02)

	rec := new(pbinWitnessRecorder)
	root, pph := pbinWitnessProcess(t, corpus, rec)
	require.Equal(t, pbinNodeLeaf, pph.grid.root.kind)

	emitted := rec.byHash(t)
	require.Len(t, emitted, 1)
	require.Contains(t, emitted, string(root))
	require.Equal(t, byte(pbinLeafTag), emitted[string(root)][0])
}

// TestPBinWitnessTracerCoversSiblingCells: the two leaves are hashed by
// hashRowCell during the branch fold, the root by RootHash. All three land in
// the emitted set and nothing else does.
func TestPBinWitnessTracerCoversSiblingCells(t *testing.T) {
	t.Parallel()

	addr := pbinOracleAddr(32)
	left, right := pbinOracleSlot(256), pbinOracleSlot(257)
	corpus := new(pbinTestCorpus).storage(addr, left, 0xAA).storage(addr, right, 0xBB)

	rec := new(pbinWitnessRecorder)
	root, pph := pbinWitnessProcess(t, corpus, rec)
	require.Equal(t, pbinNodeBranch, pph.grid.root.kind)

	emitted := rec.byHash(t)
	require.Contains(t, emitted, string(root))
	require.Equal(t, pbinWitnessOracleNodes(t, corpus.entries(t)), emitted)
}

// TestPBinWitnessTracerDetachedOnReset keeps the tracer off the normal
// commitment path a reset engine goes back to serving.
func TestPBinWitnessTracerDetachedOnReset(t *testing.T) {
	t.Parallel()

	corpus := pbinWitnessCorpus()
	pph, ms := pbinTestEngine(t)
	corpus.applyTo(t, ms)

	pph.setWitnessTracer(&pbinWitnessRejectingTracer{t: t})
	pph.Reset()
	require.Nil(t, pph.hasher.tracer)

	require.Equal(t, corpus.oracleRoot(t), pbinTestProcess(t, pph, corpus.plainKeys, corpus.updates))
}

// TestPBinWitnessTracerDetachedOnRelease: a pooled engine that kept its tracer
// would leak the next run's nodes into a finished witness.
func TestPBinWitnessTracerDetachedOnRelease(t *testing.T) {
	t.Parallel()

	corpus := pbinWitnessCorpus()
	pph, ms := pbinTestEngine(t)
	corpus.applyTo(t, ms)

	rec := new(pbinWitnessRecorder)
	pph.setWitnessTracer(rec)
	pph.Release()

	reused := NewPBinPatriciaHashed(ms)
	require.Nil(t, reused.hasher.tracer)
	require.Equal(t, corpus.oracleRoot(t), pbinTestProcess(t, reused, corpus.plainKeys, corpus.updates))
	require.Empty(t, rec.hashes)
}
