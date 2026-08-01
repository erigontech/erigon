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

// pbinWitnessOnPathNodes names the nodes the proved keys walk through, stated as
// "the path taken to reach the node is a prefix of some proved key" over a walk
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
		for bit := range node.children {
			child := arrival
			child.append(&node.prefix)
			child.appendBit(uint64(bit))
			walk(node.children[bit], child)
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

// TestPBinWitnessPruneDropsOffPathNodes: the capture holds nodes no proved key
// walks through — sibling leaves a binary branch already commits to, and branches
// re-hashed under a shorter prefix earlier in the fold. Keeping them is the whole
// cost the pruner exists to remove.
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
		require.Contains(t, leaves, string(pbinTestChunkKey(addr, keccak.Sum256(code), i)), "code chunk %d was pruned away", i)
	}
	require.Contains(t, leaves, string(pbinTreeKeyAccount(addr, pbinCodeHashLeafKey)))
}

// TestPBinWitnessPruneStopsAtBlindedChild: a key whose path leaves the witness
// keeps what it walked and stops. The key is built from a path the witness is
// known to blind, so the case cannot silently stop being one.
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

// TestPBinWitnessPruneEmptyCapture: no capture, nothing to prune. The empty
// result is what an update set touching nothing produces.
func TestPBinWitnessPruneEmptyCapture(t *testing.T) {
	t.Parallel()

	lean, err := PBinWitnessNodesForKeys(nil, pbinEmptyTreeHash[:], nil)
	require.NoError(t, err)
	require.Empty(t, lean)
}
