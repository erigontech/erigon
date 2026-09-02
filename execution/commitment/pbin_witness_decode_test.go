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
	"fmt"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/length"
)

// merkelize rehashes the tree from its root, so a decode that lost anything
// fails here instead of downstream. A child hash the set has no preimage for is
// blinded: opaque, and carried up as it stands. The replay path resolves leaves
// through pbinWitnessContext and never rehashes the whole tree, so this is a
// check the tests apply, not one the node runs.
func (w *pbinWitnessTree) merkelize() (common.Hash, error) {
	if len(w.nodes) == 0 {
		return pbinEmptyTreeHash, nil
	}
	got, err := w.merkelizeFrom(w.root, 0)
	if err != nil {
		return common.Hash{}, err
	}
	if got != w.root {
		return common.Hash{}, fmt.Errorf("%w: root %x re-merkelizes to %x", errPBinWitnessNode, w.root, got)
	}
	return got, nil
}

// merkelizeFrom rehashes the subtree at hash, sitting at bit position depth. The
// depth bounds the recursion, but not the work: a set whose nodes reference each
// other can cost exponentially many calls, which is why this stays out of the
// engine.
func (w *pbinWitnessTree) merkelizeFrom(hash common.Hash, depth int16) (common.Hash, error) {
	node, ok := w.nodes[hash]
	if !ok {
		return hash, nil
	}
	if node.isLeaf() {
		return w.hasher.leafNodeHash(node.key, node.value), nil
	}
	next := depth + node.prefix.bitLen + 1
	if int(next) > pbinMaxPathBits {
		return common.Hash{}, fmt.Errorf("%w: branch at bit %d with a %d-bit prefix overflows the %d-bit path",
			errPBinWitnessNode, depth, node.prefix.bitLen, pbinMaxPathBits)
	}
	left, err := w.merkelizeFrom(node.children[0], next)
	if err != nil {
		return common.Hash{}, err
	}
	right, err := w.merkelizeFrom(node.children[1], next)
	if err != nil {
		return common.Hash{}, err
	}
	return w.hasher.branchHash(&node.prefix, &left, &right), nil
}

// leafNodeHash is H over a leaf preimage built from a decoded key, where
// leafCellHash packs the key from a path and a cell.
func (h *pbinHasher) leafNodeHash(key, value []byte) common.Hash {
	buf := append(h.buf[:0], pbinLeafTag)
	buf = append(buf, key...)
	buf = append(buf, value...)
	return h.hash(buf)
}

// pbinWitnessCapture folds the corpus with the node set attached, giving the
// root-first slice a witness carries.
func pbinWitnessCapture(t *testing.T, corpus *pbinTestCorpus) (nodes [][]byte, root []byte) {
	t.Helper()
	set := newWitnessNodeSet()
	root, _ = pbinWitnessProcess(t, corpus, set)
	root = bytes.Clone(root)
	nodes, err := set.nodes(root)
	require.NoError(t, err)
	return nodes, root
}

func pbinWitnessDecoded(t *testing.T, nodes [][]byte, root []byte) *pbinWitnessTree {
	t.Helper()
	w, err := pbinDecodeWitness(nodes, root)
	require.NoError(t, err)
	return w
}

func pbinWitnessMerkelized(t *testing.T, nodes [][]byte, root []byte) []byte {
	t.Helper()
	got, err := pbinWitnessDecoded(t, nodes, root).merkelize()
	require.NoError(t, err)
	return got[:]
}

// pbinWitnessReachable walks the decoded tree from its root, returning the nodes
// it reaches and the child hashes it could not resolve. Both are what a consumer
// of the witness actually sees; the captured set holds more.
func pbinWitnessReachable(w *pbinWitnessTree) (reached map[common.Hash]pbinWitnessNode, blinded []common.Hash) {
	reached = make(map[common.Hash]pbinWitnessNode)
	var walk func(hash common.Hash)
	walk = func(hash common.Hash) {
		node, ok := w.nodes[hash]
		if !ok {
			if hash != pbinEmptyTreeHash {
				blinded = append(blinded, hash)
			}
			return
		}
		if _, seen := reached[hash]; seen {
			return
		}
		reached[hash] = node
		if !node.isLeaf() {
			walk(node.children[0])
			walk(node.children[1])
		}
	}
	walk(w.root)
	return reached, blinded
}

// TestPBinDecodeWitnessNodeShapes pins the two preimage layouts against the
// reference transcription's encode_bit_prefix rather than against the encoder
// the engine uses.
func TestPBinDecodeWitnessNodeShapes(t *testing.T) {
	t.Parallel()

	t.Run("leaf", func(t *testing.T) {
		t.Parallel()
		key := pbinTreeKeyAccount(pbinOracleAddr(1), pbinBasicDataLeafKey)
		value := pbinOracleValue(9)

		node, err := pbinDecodeWitnessNode(slices.Concat([]byte{pbinLeafTag}, key, value))
		require.NoError(t, err)
		require.True(t, node.isLeaf())
		require.Equal(t, key, node.key)
		require.Equal(t, value, node.value)
	})

	t.Run("branch", func(t *testing.T) {
		t.Parallel()
		left, right := common.Hash{0xAA}, common.Hash{0xBB}
		preimage := slices.Concat(
			[]byte{pbinBranchTag},
			pbinOracleEncodeBitPrefix([]byte{1, 0, 1}),
			left[:], right[:])

		node, err := pbinDecodeWitnessNode(preimage)
		require.NoError(t, err)
		require.False(t, node.isLeaf())
		require.Equal(t, pbinPathFromBits([]byte{0xA0}, 3), node.prefix)
		require.Equal(t, [2]common.Hash{left, right}, node.children)
	})

	t.Run("branch with an empty prefix", func(t *testing.T) {
		t.Parallel()
		preimage := slices.Concat(
			[]byte{pbinBranchTag},
			pbinOracleEncodeBitPrefix(nil),
			make([]byte, 2*length.Hash))

		node, err := pbinDecodeWitnessNode(preimage)
		require.NoError(t, err)
		require.Equal(t, int16(0), node.prefix.bitLen)
		require.Equal(t, [2]common.Hash{pbinEmptyTreeHash, pbinEmptyTreeHash}, node.children,
			"an absent child is the empty-tree hash, never omitted")
	})
}

// TestPBinDecodeWitnessNodeRejectsMalformed: a witness comes from a peer, so
// every one of these has to error rather than yield a node that hashes to
// something else.
func TestPBinDecodeWitnessNodeRejectsMalformed(t *testing.T) {
	t.Parallel()

	key := pbinTreeKeyAccount(pbinOracleAddr(1), pbinBasicDataLeafKey)
	value := pbinOracleValue(9)
	children := make([]byte, 2*length.Hash)

	branch := func(bitLen uint16, packed []byte) []byte {
		return slices.Concat([]byte{pbinBranchTag, byte(bitLen >> 8), byte(bitLen)}, packed, children)
	}

	for _, tc := range []struct {
		name     string
		preimage []byte
	}{
		{name: "empty preimage", preimage: nil},
		{name: "unknown tag", preimage: slices.Concat([]byte{0x02}, key, value)},
		{name: "leaf without a key", preimage: slices.Concat([]byte{pbinLeafTag}, value)},
		{name: "leaf truncated inside its value", preimage: slices.Concat([]byte{pbinLeafTag}, key, value[:31])},
		{name: "leaf key of an unallocated zone", preimage: slices.Concat([]byte{pbinLeafTag, 0x02}, key[1:], value)},
		{name: "leaf key one byte short of its zone", preimage: slices.Concat([]byte{pbinLeafTag}, key[:len(key)-1], value)},
		{name: "leaf key one byte past its zone", preimage: slices.Concat([]byte{pbinLeafTag}, key, []byte{0}, value)},
		{name: "branch without a bit count", preimage: []byte{pbinBranchTag, 0x00}},
		{name: "branch prefix past the encodable path", preimage: branch(pbinMaxPathBits+1, make([]byte, 67))},
		{name: "branch prefix truncated", preimage: branch(16, []byte{0x00})},
		{name: "branch missing a child hash", preimage: slices.Concat([]byte{pbinBranchTag, 0, 0}, children[:length.Hash])},
		{name: "branch with a trailing byte", preimage: branch(0, []byte{0x00})},
		{name: "branch prefix padded non-canonically", preimage: branch(3, []byte{0xA1})},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := pbinDecodeWitnessNode(tc.preimage)
			require.ErrorIs(t, err, errPBinWitnessNode)
		})
	}
}

// TestPBinWitnessDecodeRoundTrip: a captured fold decodes back into the leaves
// the corpus stands for and re-merkelizes to the root it was captured under.
func TestPBinWitnessDecodeRoundTrip(t *testing.T) {
	t.Parallel()

	corpus := pbinWitnessCorpus()
	nodes, root := pbinWitnessCapture(t, corpus)
	require.Equal(t, corpus.oracleRoot(t), root)

	w := pbinWitnessDecoded(t, nodes, root)
	reached, blinded := pbinWitnessReachable(w)
	require.Empty(t, blinded, "a fold from empty state hashes every node, so nothing is blinded")

	leaves := make(map[string][]byte)
	branches := 0
	for _, node := range reached {
		if node.isLeaf() {
			leaves[string(node.key)] = node.value
			continue
		}
		branches++
	}
	require.Positive(t, branches)
	require.Len(t, leaves, corpus.leafCount(t))
	for _, e := range corpus.entries(t) {
		require.Equal(t, e.value, leaves[string(e.key)], "leaf %x", e.key)
	}

	got, err := w.merkelize()
	require.NoError(t, err)
	require.Equal(t, root, got[:])
}

// TestPBinWitnessDecodeBlindedChild: the witness of a few touched keys proves
// only their paths, and the subtrees it leaves out are opaque hashes the root
// still has to come out of.
func TestPBinWitnessDecodeBlindedChild(t *testing.T) {
	t.Parallel()

	ms, parentRoot := pbinWitnessCommitted(t, pbinWitnessCorpus())
	pending := pbinWitnessPending()

	upd := WrapKeyUpdates(t, ModeUpdate, pbinKeyHasher(), pending.plainKeys, pending.updates)
	nodes, _, root := pbinWitnessesOf(t, ms, upd, false)
	require.Equal(t, parentRoot, root)

	w := pbinWitnessDecoded(t, nodes, root)
	_, blinded := pbinWitnessReachable(w)
	require.NotEmpty(t, blinded, "the witness resolves every child, so it proves nothing about blinding")

	require.Equal(t, root, pbinWitnessMerkelized(t, nodes, root))
}

// TestPBinWitnessDecodePermutationIndependence: the captured set depends on the
// key/value set, not on the order the keys were folded in, and the root it
// re-merkelizes to is the reference implementation's.
func TestPBinWitnessDecodePermutationIndependence(t *testing.T) {
	t.Parallel()

	corpus := pbinWitnessCorpus()
	forward := make([]int, len(corpus.plainKeys))
	for i := range forward {
		forward[i] = i
	}
	reversed := slices.Clone(forward)
	slices.Reverse(reversed)
	interleaved := slices.Concat(forward[len(forward)/2:], forward[:len(forward)/2])

	var tree pbinOracleTree
	for _, e := range corpus.entries(t) {
		tree.insert(e.key, e.value)
	}
	want := pbinOracleMerkelizeWith(tree.root, nil)

	for name, order := range map[string][]int{
		"forward":     forward,
		"reversed":    reversed,
		"interleaved": interleaved,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			nodes, root := pbinWitnessCapture(t, corpus.permute(order))
			require.Equal(t, want[:], pbinWitnessMerkelized(t, nodes, root))
		})
	}
}

// TestPBinWitnessDecodeIgnoresNodeOrder: the witness an RPC consumer receives is
// sorted by node bytes, so the decode may not require the root to lead the slice
// — it is told the root and looks it up.
func TestPBinWitnessDecodeIgnoresNodeOrder(t *testing.T) {
	t.Parallel()

	nodes, root := pbinWitnessCapture(t, pbinWitnessCorpus())
	require.Greater(t, len(nodes), 1)

	sorted := slices.Clone(nodes)
	slices.SortFunc(sorted, bytes.Compare)
	require.NotEqual(t, nodes[0], sorted[0], "the sort has to move the root off the front")
	require.Equal(t, root, pbinWitnessMerkelized(t, sorted, root))

	reversed := slices.Clone(nodes)
	slices.Reverse(reversed)
	require.Equal(t, root, pbinWitnessMerkelized(t, reversed, root))
}

// TestPBinWitnessDecodeSingleNodeRemoval: dropping a node blinds its subtree,
// which leaves the root alone. The one drop that could change it — the root
// node's — has to be caught instead.
func TestPBinWitnessDecodeSingleNodeRemoval(t *testing.T) {
	t.Parallel()

	nodes, root := pbinWitnessCapture(t, pbinWitnessCorpus())
	require.Greater(t, len(nodes), 1)

	rejected := 0
	for i := range nodes {
		short := make([][]byte, 0, len(nodes)-1)
		short = append(short, nodes[:i]...)
		short = append(short, nodes[i+1:]...)

		w, err := pbinDecodeWitness(short, root)
		if err != nil {
			rejected++
			continue
		}
		got, err := w.merkelize()
		if err != nil {
			rejected++
			continue
		}
		require.Equal(t, root, got[:], "dropping node %d moved the root instead of failing", i)
	}
	require.Equal(t, 1, rejected, "only the root node's removal is unrecoverable")
}

// TestPBinWitnessDecodeEmptyTree: an empty tree is 32 zero bytes with no node
// behind it (eip:"Node merkelization"), and a witness claiming any other root with no nodes is
// unusable rather than empty.
func TestPBinWitnessDecodeEmptyTree(t *testing.T) {
	t.Parallel()

	w, err := pbinDecodeWitness(nil, pbinEmptyTreeHash[:])
	require.NoError(t, err)
	got, err := w.merkelize()
	require.NoError(t, err)
	require.Equal(t, pbinEmptyTreeHash, got)

	nonEmpty := common.Hash{0x01}
	_, err = pbinDecodeWitness(nil, nonEmpty[:])
	require.ErrorIs(t, err, errPBinWitnessNode)

	_, err = pbinDecodeWitness(nil, nil)
	require.ErrorIs(t, err, errPBinWitnessNode)
}
