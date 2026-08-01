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
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
)

// pbinWitnessContextCode is the code pbinWitnessCorpus commits for account 21.
// The pending set keeps it unchanged: the witness pass reads pre-state code and
// checks it against the update's code size, so a resized contract is a corpus
// the pass refuses before the context is ever reached.
func pbinWitnessContextCode() []byte { return bytes.Repeat([]byte{0x60}, 200) }

// pbinWitnessContextPending touches a coded account, a fresh account and two
// slots, so the post-state pass has to split leaves, create branches, pack
// BASIC_DATA and chunk code over the witness alone.
func pbinWitnessContextPending() *pbinTestCorpus {
	c := new(pbinTestCorpus)
	c.accountWithCodeBytes(pbinOracleAddr(21), 5, 1500, pbinWitnessContextCode())
	c.account(pbinOracleAddr(23), 3, 300, common.Hash{0x23})
	c.storage(pbinOracleAddr(21), pbinOracleSlot(64), 0xEE)
	c.storage(pbinOracleAddr(23), pbinOracleSlot(5), 0x55)
	return c
}

type pbinWitnessContextFixture struct {
	state      *MockState
	pending    *pbinTestCorpus
	witness    *pbinWitnessContext
	tree       *pbinWitnessTree
	parentRoot []byte
}

// pbinWitnessContextSetup commits a corpus, takes the witness of the pending
// updates against it, and hands back a context backed by nothing else.
func pbinWitnessContextSetup(t *testing.T) *pbinWitnessContextFixture {
	t.Helper()
	f := &pbinWitnessContextFixture{pending: pbinWitnessContextPending()}
	f.state, f.parentRoot = pbinWitnessCommitted(t, pbinWitnessCorpus())

	upd := WrapKeyUpdates(t, ModeUpdate, pbinKeyHasher(), f.pending.plainKeys, f.pending.updates)
	nodes, _, root := pbinWitnessesOf(t, f.state, upd, false)
	require.Equal(t, f.parentRoot, root)

	f.tree = pbinWitnessDecoded(t, nodes, root)
	f.witness = pbinNewWitnessContext(f.tree)
	for addr, code := range f.pending.codes {
		f.witness.setCode([]byte(addr), code)
	}
	return f
}

func (f *pbinWitnessContextFixture) apply(t *testing.T, ctx PatriciaContext) []byte {
	t.Helper()
	upd := WrapKeyUpdates(t, ModeUpdate, pbinKeyHasher(), f.pending.plainKeys, f.pending.updates)
	root, err := NewPBinPatriciaHashed(ctx).Process(context.Background(), upd, "", nil, WarmupConfig{})
	require.NoError(t, err)
	return root
}

// TestPBinWitnessContextPostStateRoot is the point of the whole context: the
// engine applies the block's updates over the witness and reaches the root it
// reaches over full state, so no second mutable binary trie is needed.
func TestPBinWitnessContextPostStateRoot(t *testing.T) {
	t.Parallel()

	f := pbinWitnessContextSetup(t)
	got := f.apply(t, f.witness)
	want := f.apply(t, f.state)

	require.Equal(t, want, got)
	require.NotEqual(t, f.parentRoot, want, "the pending updates do not move the root, so the test proves nothing")
}

// TestPBinWitnessContextProvesNothingItDoesNotHold: the witness stops at the
// touched paths, and the subtrees it leaves opaque are what the root still has
// to be recomputed through.
func TestPBinWitnessContextPartialWitness(t *testing.T) {
	t.Parallel()

	f := pbinWitnessContextSetup(t)
	_, blinded := pbinWitnessReachable(f.tree)
	require.NotEmpty(t, blinded, "the witness holds every node, so it proves nothing about partial state")

	full, _ := pbinWitnessCapture(t, pbinWitnessCorpus())
	require.Less(t, len(f.tree.nodes), len(full), "the witness is not smaller than the whole tree")

	require.Equal(t, f.apply(t, f.state), f.apply(t, f.witness))
}

// TestPBinWitnessContextBlindedBranchErrors: a read that needs a node the
// witness left out must name the path and fail, never come back empty — an
// empty record reads as an absent subtree and builds a wrong root.
func TestPBinWitnessContextBlindedBranchErrors(t *testing.T) {
	t.Parallel()

	f := pbinWitnessContextSetup(t)
	path := pbinWitnessBlindedPath(t, f.tree)

	record, _, err := f.witness.Branch(pbinEncodeBitPath(&path))
	require.ErrorIs(t, err, errPBinWitnessBlinded)
	require.Empty(t, record)
	require.Contains(t, err.Error(), hex.EncodeToString(path.appendPackedBits(nil)), "the error does not name the path")
}

// pbinWitnessBlindedPath walks to the first child the witness has no preimage
// for and returns its absolute path, which is the key the engine would read a
// record at.
func pbinWitnessBlindedPath(t *testing.T, w *pbinWitnessTree) pbinBitpath {
	t.Helper()
	var found pbinBitpath
	var ok bool
	var walk func(hash common.Hash, path pbinBitpath)
	walk = func(hash common.Hash, path pbinBitpath) {
		node, present := w.nodes[hash]
		if !present || ok {
			return
		}
		path.append(&node.prefix)
		if node.isLeaf() {
			return
		}
		for bit := range node.children {
			child := path
			child.appendBit(uint64(bit))
			if _, present := w.nodes[node.children[bit]]; !present {
				found, ok = child, true
				return
			}
			walk(node.children[bit], child)
		}
	}
	walk(w.root, pbinBitpath{})
	require.True(t, ok, "the witness blinds no child")
	return found
}

// TestPBinWitnessContextRefusesUnknownState: the context serves the witness and
// nothing else. A plain key it never issued a handle for has no state, and
// answering with an empty update would hash a zeroed leaf into the root.
func TestPBinWitnessContextRefusesUnknownState(t *testing.T) {
	t.Parallel()

	f := pbinWitnessContextSetup(t)
	addr := pbinOracleAddr(21)

	_, err := f.witness.Account(addr)
	require.ErrorIs(t, err, errPBinWitnessNoState)

	_, err = f.witness.Storage(append(bytes.Clone(addr), pbinOracleSlot(64)...))
	require.ErrorIs(t, err, errPBinWitnessNoState)

	_, err = f.witness.Code(pbinOracleAddr(99))
	require.ErrorIs(t, err, errPBinWitnessNoState)
}

// TestPBinWitnessContextLeafHandlesRoundTrip: a BASIC_DATA leaf is packed from
// account fields, so a record carries those fields rather than the 32 bytes the
// witness holds. The handle the cell carries has to lead back to a state that
// packs to exactly those bytes.
func TestPBinWitnessContextLeafHandlesRoundTrip(t *testing.T) {
	t.Parallel()

	f := pbinWitnessContextSetup(t)
	f.apply(t, f.witness)
	require.NotEmpty(t, f.witness.leaves, "no leaf needed a handle, so the packing path is untested")

	for handle, state := range f.witness.leaves {
		update := state
		got, err := f.witness.Account([]byte(handle))
		require.NoError(t, err)
		require.Equal(t, &update, got)
		require.False(t, got.Deleted())
	}
}
