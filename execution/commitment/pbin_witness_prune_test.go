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
	"context"
	"slices"
	"testing"

	keccak "github.com/erigontech/fastkeccak"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/empty"
)

type pbinWitnessPruneFixture struct {
	state      *MockState
	pending    *pbinTestCorpus
	nodes      [][]byte
	provedKeys [][]byte
	root       []byte
	tree       *pbinWitnessTree
}

// pbinWitnessPruneSetup commits a corpus and captures the superset witness of
// the pending updates against it — the input the pruner has to cut down.
func pbinWitnessPruneSetup(t *testing.T) *pbinWitnessPruneFixture {
	t.Helper()
	f := &pbinWitnessPruneFixture{pending: pbinWitnessContextPending()}
	f.state, f.root = pbinWitnessCommitted(t, pbinWitnessCorpus())

	upd := WrapKeyUpdates(t, ModeUpdate, pbinKeyHasher(), f.pending.plainKeys, f.pending.updates)
	nodes, provedKeys, root := pbinWitnessesOf(t, f.state, upd, false)
	require.Equal(t, f.root, root)
	f.nodes, f.provedKeys = nodes, provedKeys
	f.tree = pbinWitnessDecoded(t, f.nodes, f.root)
	return f
}

func (f *pbinWitnessPruneFixture) prune(t *testing.T, provedKeys [][]byte) [][]byte {
	t.Helper()
	lean, err := PBinWitnessNodesForKeys(f.nodes, f.root, provedKeys)
	require.NoError(t, err)
	return lean
}

// postStateRoot applies the pending updates over a node set through the witness
// context, which is what a pruned witness still has to support.
func (f *pbinWitnessPruneFixture) postStateRoot(t *testing.T, nodes [][]byte) []byte {
	t.Helper()
	witness := pbinNewWitnessContext(pbinWitnessDecoded(t, nodes, f.root))
	for addr, code := range f.pending.codes {
		witness.setCode([]byte(addr), code)
	}
	return f.applyOver(t, witness)
}

func (f *pbinWitnessPruneFixture) applyOver(t *testing.T, ctx PatriciaContext) []byte {
	t.Helper()
	upd := WrapKeyUpdates(t, ModeUpdate, pbinKeyHasher(), f.pending.plainKeys, f.pending.updates)
	root, err := NewPBinPatriciaHashed(ctx).Process(context.Background(), upd, "", nil, WarmupConfig{})
	require.NoError(t, err)
	return bytes.Clone(root)
}

func pbinWitnessHashSet(t *testing.T, nodes [][]byte) map[common.Hash]struct{} {
	t.Helper()
	h := pbinHasher{sum: pbinSelectedSum}
	out := make(map[common.Hash]struct{}, len(nodes))
	for _, node := range nodes {
		out[h.hash(node)] = struct{}{}
	}
	return out
}

// pbinWitnessOnPathNodes names the nodes the proved keys walk through and the
// sibling hanging off each branch they descend, stated as "the path taken to
// reach the node is a prefix of some proved key, or its parent's is" over a walk
// of the whole tree — not as the per-key descent the pruner runs.
func pbinWitnessOnPathNodes(w *pbinWitnessTree, provedKeys [][]byte) map[common.Hash]struct{} {
	paths := make([]pbinBitpath, 0, len(provedKeys))
	for _, key := range provedKeys {
		paths = append(paths, pbinPathFromBytes(key))
	}
	onPath := func(arrival *pbinBitpath) bool {
		for i := range paths {
			if paths[i].hasPrefix(arrival) {
				return true
			}
		}
		return false
	}
	out := make(map[common.Hash]struct{})
	keep := func(hash common.Hash) {
		if _, ok := w.nodes[hash]; ok {
			out[hash] = struct{}{}
		}
	}
	var walk func(hash common.Hash, arrival pbinBitpath)
	walk = func(hash common.Hash, arrival pbinBitpath) {
		node, ok := w.nodes[hash]
		if !ok || !onPath(&arrival) {
			return
		}
		out[hash] = struct{}{}
		if node.isLeaf() {
			return
		}
		child := [2]pbinBitpath{}
		for bit := range node.children {
			child[bit] = arrival
			child[bit].append(&node.prefix)
			child[bit].appendBit(uint64(bit))
		}
		for bit := range node.children {
			if onPath(&child[bit]) {
				keep(node.children[1-bit])
			}
			walk(node.children[bit], child[bit])
		}
	}
	walk(w.root, pbinBitpath{})
	return out
}

// TestPBinWitnessPruneKeepsProofPaths: the lean set has to be a witness in its
// own right — it re-merkelizes to the same root and still carries the block's
// updates to the same post-state root the full capture does.
func TestPBinWitnessPruneKeepsProofPaths(t *testing.T) {
	t.Parallel()

	f := pbinWitnessPruneSetup(t)
	lean := f.prune(t, f.provedKeys)

	require.NotEmpty(t, lean)
	require.Equal(t, f.nodes[0], lean[0], "root node is not first")
	require.Equal(t, f.root, pbinWitnessMerkelized(t, lean, f.root))

	require.Equal(t, f.postStateRoot(t, f.nodes), f.postStateRoot(t, lean))
	require.Equal(t, f.applyOver(t, f.state), f.postStateRoot(t, lean))
}

// TestPBinWitnessPruneDropsOffPathNodes: the capture holds nodes neither a proved
// key nor a collapse reaches — whole subtrees hanging two or more levels off a
// path.
func TestPBinWitnessPruneDropsOffPathNodes(t *testing.T) {
	t.Parallel()

	f := pbinWitnessPruneSetup(t)
	lean := f.prune(t, f.provedKeys)

	full := pbinWitnessHashSet(t, f.nodes)
	kept := pbinWitnessHashSet(t, lean)
	require.Less(t, len(lean), len(f.nodes), "nothing was pruned, so the test proves nothing")
	for hash := range kept {
		require.Contains(t, full, hash, "the pruned set invented node %x", hash)
	}
	require.Equal(t, pbinWitnessOnPathNodes(f.tree, f.provedKeys), kept)
}

// TestPBinWitnessPruneKeepsCodeLeaves: a contract's code leaves are proved keys
// of their own (they never reach HashSort), and a pruner walking only the account
// key would drop the code the post-state pass then cannot chunk.
func TestPBinWitnessPruneKeepsCodeLeaves(t *testing.T) {
	t.Parallel()

	f := pbinWitnessPruneSetup(t)
	lean := f.prune(t, f.provedKeys)

	addr := pbinOracleAddr(21)
	code := pbinWitnessContextCode()
	chunks := pbinChunkifyCode(code)
	require.Greater(t, len(chunks), 1)

	leaves := make(map[string]struct{})
	for _, node := range lean {
		decoded, err := pbinDecodeWitnessNode(node)
		require.NoError(t, err)
		if decoded.isLeaf() {
			leaves[string(decoded.key)] = struct{}{}
		}
	}
	for i := range chunks {
		require.Contains(t, leaves, string(pbinTreeKeyCodeChunk(keccak.Sum256(code), i)), "code chunk %d was pruned away", i)
	}
	require.Contains(t, leaves, string(pbinTreeKeyAccount(addr, pbinCodeHashLeafKey)))
}

// TestPBinWitnessPruneStopsAtBlindedChild: a key whose path leaves the witness
// keeps what it walked and stops.
func TestPBinWitnessPruneStopsAtBlindedChild(t *testing.T) {
	t.Parallel()

	f := pbinWitnessPruneSetup(t)
	blind := pbinWitnessKeyThrough(t, pbinWitnessBlindedPath(t, f.tree))
	require.NotContains(t, pbinWitnessKeySet(f.provedKeys), string(blind))

	lean := f.prune(t, [][]byte{blind})
	require.Greater(t, len(lean), 1, "the walk stopped at the root, so it never reached the blinded child")
	require.Equal(t, f.root, pbinWitnessMerkelized(t, lean, f.root))
	require.Equal(t, pbinWitnessOnPathNodes(f.tree, [][]byte{blind}), pbinWitnessHashSet(t, lean))

	both := f.prune(t, append(slices.Clone(f.provedKeys), blind))
	require.Equal(t, f.root, pbinWitnessMerkelized(t, both, f.root))
	require.Equal(t, f.postStateRoot(t, f.nodes), f.postStateRoot(t, both))
}

// pbinWitnessKeyThrough builds the tree key of the zone path leads into, so a
// walk of that key descends exactly the path.
func pbinWitnessKeyThrough(t *testing.T, path pbinBitpath) []byte {
	t.Helper()
	key := path.appendPackedBits(nil)
	require.NotEmpty(t, key)
	want, known := pbinZoneKeyLength(key[0])
	require.True(t, known, "path %x leads into no allocated zone", key)
	require.LessOrEqual(t, len(key), want)
	return append(key, make([]byte, want-len(key))...)
}

func pbinWitnessKeySet(keys [][]byte) map[string]struct{} {
	out := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		out[string(key)] = struct{}{}
	}
	return out
}

// TestPBinWitnessPruneRejectsMalformedKey: a proved key of no zone would panic in
// the bit-path conversion, which an RPC handler must not do.
func TestPBinWitnessPruneRejectsMalformedKey(t *testing.T) {
	t.Parallel()

	f := pbinWitnessPruneSetup(t)
	for _, tc := range []struct {
		name string
		key  []byte
	}{
		{name: "empty key", key: nil},
		{name: "unallocated zone", key: bytes.Repeat([]byte{0x02}, pbinAccountKeyLength)},
		{name: "wrong length for its zone", key: make([]byte, pbinAccountKeyLength+1)},
		{name: "longer than the path", key: make([]byte, 2*pbinStorageKeyLength)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := PBinWitnessNodesForKeys(f.nodes, f.root, [][]byte{tc.key})
			require.ErrorIs(t, err, errPBinWitnessNode)
		})
	}
}

// TestPBinWitnessPruneKeepsSubtreePrefix: an account removal proves the subtree
// it drops, not the leaves inside it, so a key shorter than its zone's length is
// a proved key and the walk stops where that subtree begins.
func TestPBinWitnessPruneKeepsSubtreePrefix(t *testing.T) {
	t.Parallel()

	f := pbinWitnessPruneSetup(t)
	var leafKey []byte
	for _, key := range f.provedKeys {
		if len(key) == pbinAccountKeyLength && key[0] == pbinAccountZone {
			leafKey = key
			break
		}
	}
	require.NotEmpty(t, leafKey, "the capture proved no account-zone leaf")
	stem := leafKey[:pbinAccountKeyLength-1]

	kept := pbinWitnessHashSet(t, f.prune(t, [][]byte{stem}))
	require.NotEmpty(t, kept)
	for hash := range kept {
		require.Contains(t, pbinWitnessHashSet(t, f.prune(t, [][]byte{leafKey})), hash,
			"the stem walk descended past the subtree the leaf key reaches")
	}
	require.Equal(t, f.root, pbinWitnessMerkelized(t, f.prune(t, [][]byte{stem}), f.root))
}

// TestPBinWitnessServesRemoval: collapsing a branch re-hashes the surviving
// sibling under a longer prefix, which needs its own preimage — a branch hash
// commits to the prefix it had and can't be reused as-is. Both sibling shapes
// are covered: a leaf the fold already hashes, and a branch that arrives as a
// bare hash from its parent's record.
func TestPBinWitnessServesRemoval(t *testing.T) {
	t.Parallel()

	addr, bystander := pbinOracleAddr(41), pbinOracleAddr(42)
	// Storage-zone sub-indices split on the low bits of the slot: 0 and 1 sit
	// under one branch, 2 under the other.
	stored := func(slots ...uint64) *pbinTestCorpus {
		c := new(pbinTestCorpus).account(bystander, 1, 2, empty.CodeHash)
		for _, slot := range slots {
			c.storage(addr, pbinOracleSlot(slot), 0x01)
		}
		return c
	}
	for _, tc := range []struct {
		name              string
		stored, survivors *pbinTestCorpus
		gone              uint64
	}{
		{
			name:      "collapse onto a leaf sibling",
			stored:    stored(256, 257, 258, 259),
			survivors: stored(257, 258, 259),
			gone:      256,
		},
		{
			name:      "collapse onto a branch sibling",
			stored:    stored(256, 257, 258),
			survivors: stored(256, 257),
			gone:      258,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ms, root := pbinWitnessCommitted(t, tc.stored)
			zeroed := new(pbinTestCorpus).storage(addr, pbinOracleSlot(tc.gone))

			upd := WrapKeyUpdates(t, ModeUpdate, pbinKeyHasher(), zeroed.plainKeys, zeroed.updates)
			nodes, provedKeys, captured := pbinWitnessesOf(t, ms, upd, false)
			require.Equal(t, root, captured)

			lean, err := PBinWitnessNodesForKeys(nodes, root, provedKeys)
			require.NoError(t, err)
			require.Less(t, len(lean), len(nodes), "nothing was pruned, so the test proves nothing")

			want := tc.survivors.oracleRoot(t)
			require.NotEqual(t, root, want, "the removal did not move the root")
			for _, set := range []struct {
				name  string
				nodes [][]byte
			}{{"superset", nodes}, {"lean", lean}} {
				witness := pbinNewWitnessContext(pbinWitnessDecoded(t, set.nodes, root))
				got, err := NewPBinPatriciaHashed(witness).Process(context.Background(),
					WrapKeyUpdates(t, ModeUpdate, pbinKeyHasher(), zeroed.plainKeys, zeroed.updates),
					"", nil, WarmupConfig{})
				require.NoError(t, err, "%s cannot serve the removal", set.name)
				require.Equal(t, want, got, "%s reached the wrong post-state root", set.name)
			}
		})
	}
}

// TestPBinWitnessPruneEmptyCapture: an update set that touches nothing produces
// no nodes to prune.
func TestPBinWitnessPruneEmptyCapture(t *testing.T) {
	t.Parallel()

	lean, err := PBinWitnessNodesForKeys(nil, pbinEmptyTreeHash[:], nil)
	require.NoError(t, err)
	require.Empty(t, lean)
}
