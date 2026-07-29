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
	"math/rand"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
)

// pbinTestBatches runs each corpus through one engine and one state in order,
// the way consecutive blocks reach the trie, and returns the root after the last.
func pbinTestBatches(t *testing.T, batches ...*pbinTestCorpus) (*PBinPatriciaHashed, *MockState, []byte) {
	t.Helper()
	pph, ms := pbinTestEngine(t)
	var root []byte
	for _, b := range batches {
		require.NoError(t, ms.applyPlainUpdates(b.plainKeys, b.updates))
		root = bytes.Clone(pbinTestProcess(t, pph, b.plainKeys, b.updates))
	}
	return pph, ms, root
}

// pbinTestUnion is the leaf set the batches leave behind. A key touched twice
// keeps its last value, which is what the oracle's duplicate-key insert does and
// what MockState's update merge does.
func pbinTestUnion(batches ...*pbinTestCorpus) *pbinTestCorpus {
	u := new(pbinTestCorpus)
	for _, b := range batches {
		u.plainKeys = append(u.plainKeys, b.plainKeys...)
		u.updates = append(u.updates, b.updates...)
	}
	return u
}

// leafCount is how many leaves the corpus stands for once repeated keys collapse.
func (c *pbinTestCorpus) leafCount(t *testing.T) int {
	t.Helper()
	seen := make(map[string]struct{})
	for _, e := range c.entries(t) {
		seen[string(e.key)] = struct{}{}
	}
	return len(seen)
}

func (c *pbinTestCorpus) permute(order []int) *pbinTestCorpus {
	out := new(pbinTestCorpus)
	for _, i := range order {
		out.plainKeys = append(out.plainKeys, c.plainKeys[i])
		out.updates = append(out.updates, c.updates[i])
	}
	return out
}

// TestPBinUntouchedSiblingSurvivesBatch guards H2. At arity 2 a cell's sibling is
// the whole other half of the subtree, so a batch that rewrites a node from the
// touched child alone loses everything under the other one.
func TestPBinUntouchedSiblingSurvivesBatch(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name           string
		batchA, batchB *pbinTestCorpus
	}{
		{
			name: "sibling slot in the same storage group",
			batchA: new(pbinTestCorpus).
				storage(pbinOracleAddr(41), pbinOracleSlot(256), 0x01).
				storage(pbinOracleAddr(41), pbinOracleSlot(257), 0x02),
			batchB: new(pbinTestCorpus).
				storage(pbinOracleAddr(41), pbinOracleSlot(257), 0x03),
		},
		{
			name: "sibling under another account",
			batchA: new(pbinTestCorpus).
				account(pbinOracleAddr(42), 1, 10, common.Hash{0x01}).
				account(pbinOracleAddr(43), 2, 20, common.Hash{0x02}),
			batchB: new(pbinTestCorpus).
				account(pbinOracleAddr(43), 3, 30, common.Hash{0x03}),
		},
		{
			name: "a third key joins a shared branch",
			batchA: new(pbinTestCorpus).
				storage(pbinOracleAddr(44), pbinOracleSlot(256), 0x01).
				storage(pbinOracleAddr(44), pbinOracleSlot(257), 0x02),
			batchB: new(pbinTestCorpus).
				storage(pbinOracleAddr(44), pbinOracleSlot(258), 0x03),
		},
		{
			name: "one header slot of an account spanning both zones",
			batchA: new(pbinTestCorpus).
				account(pbinOracleAddr(45), 1, 10, common.Hash{0x01}).
				storage(pbinOracleAddr(45), pbinOracleSlot(0), 0x01).
				storage(pbinOracleAddr(45), pbinOracleSlot(63), 0x02).
				storage(pbinOracleAddr(45), pbinOracleSlot(64), 0x03).
				storage(pbinOracleAddr(45), pbinOracleSlot(1000), 0x04),
			batchB: new(pbinTestCorpus).
				storage(pbinOracleAddr(45), pbinOracleSlot(63), 0x09),
		},
		{
			name:   "one of a deep-shared-prefix cluster",
			batchA: pbinTestDeepSharedPrefixCorpus(),
			batchB: new(pbinTestCorpus).
				account(pbinOracleMinedAddrs()[1], 99, 999, common.Hash{0x99}),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, ms, root := pbinTestBatches(t, tc.batchA, tc.batchB)

			union := pbinTestUnion(tc.batchA, tc.batchB)
			require.Equal(t, union.oracleRoot(t), root)
			pbinTestVerifyRecords(t, ms, root, union.leafCount(t))
		})
	}
}

// TestPBinSplitInsideStoredPrefix guards H1. A probe diverging inside a stored
// branch's prefix shortens that prefix, and the prefix is inside the node's hash,
// so a hash carried over from the record is stale. The counters are what pin that
// this run actually took that path rather than passing by luck.
func TestPBinSplitInsideStoredPrefix(t *testing.T) {
	t.Parallel()

	addr := pbinOracleAddr(61)
	batchA := new(pbinTestCorpus).
		account(addr, 1, 2, common.Hash{0x01}).
		storage(addr, pbinOracleSlot(256), 0x01).
		storage(addr, pbinOracleSlot(257), 0x02)
	// Sub-indices 0, 1 and 2 differ only in their last two bits, so the third slot
	// leaves the stored branch's prefix one bit before its end.
	batchB := new(pbinTestCorpus).storage(addr, pbinOracleSlot(258), 0x03)

	pph, ms := pbinTestEngine(t)
	require.NoError(t, ms.applyPlainUpdates(batchA.plainKeys, batchA.updates))
	pbinTestProcess(t, pph, batchA.plainKeys, batchA.updates)
	afterA := pph.counters

	require.NoError(t, ms.applyPlainUpdates(batchB.plainKeys, batchB.updates))
	root := pbinTestProcess(t, pph, batchB.plainKeys, batchB.updates)

	require.Greater(t, pph.counters.splitsInsidePrefix, afterA.splitsInsidePrefix)
	require.Greater(t, pph.counters.materializeReads, afterA.materializeReads,
		"a branch cell read back from a record must rehash under its shortened prefix")

	union := pbinTestUnion(batchA, batchB)
	require.Equal(t, union.oracleRoot(t), root)
	pbinTestVerifyRecords(t, ms, root, union.leafCount(t))
}

// TestPBinDeepSharedPrefixCorpus is H1's other half: a mined cluster whose keys
// agree far past the root, so the splits happen deep instead of at the first
// bits, spread over batches so the survivors come back from records.
func TestPBinDeepSharedPrefixCorpus(t *testing.T) {
	t.Parallel()

	addrs := pbinOracleMinedAddrs()
	require.GreaterOrEqual(t, len(addrs), 4)

	batches := make([]*pbinTestCorpus, 0, len(addrs))
	for i, addr := range addrs {
		batches = append(batches, new(pbinTestCorpus).
			account(addr, uint64(i), uint64(i)*7, common.Hash{byte(i)}))
	}

	pph, ms, root := pbinTestBatches(t, batches...)
	require.Positive(t, pph.counters.splitsInsidePrefix)

	union := pbinTestUnion(batches...)
	require.Equal(t, union.oracleRoot(t), root)
	pbinTestVerifyRecords(t, ms, root, union.leafCount(t))
}

// pbinTestOrderings returns the corpus in every arrival order worth trying: as
// written, reversed, both tree-key directions, and one shuffle.
func pbinTestOrderings(t *testing.T, c *pbinTestCorpus) map[string]*pbinTestCorpus {
	t.Helper()

	hasher := pbinKeyHasher()
	treeKeys := make([][]byte, len(c.plainKeys))
	order := make([]int, len(c.plainKeys))
	for i, plainKey := range c.plainKeys {
		treeKeys[i] = hasher(plainKey)
		order[i] = i
	}

	ascending := slices.Clone(order)
	slices.SortFunc(ascending, func(a, b int) int { return bytes.Compare(treeKeys[a], treeKeys[b]) })
	descending := slices.Clone(ascending)
	slices.Reverse(descending)
	reversed := slices.Clone(order)
	slices.Reverse(reversed)

	shuffled := slices.Clone(order)
	rnd := rand.New(rand.NewSource(0x8297))
	rnd.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })

	return map[string]*pbinTestCorpus{
		"as given":            c,
		"reversed":            c.permute(reversed),
		"tree key ascending":  c.permute(ascending),
		"tree key descending": c.permute(descending),
		"shuffled":            c.permute(shuffled),
	}
}

// pbinTestProcessSeq feeds the corpus one key per Process call, the way
// per-block processing arrives, and returns the root after the last key.
func pbinTestProcessSeq(t *testing.T, c *pbinTestCorpus) (*MockState, []byte) {
	t.Helper()
	pph, ms := pbinTestEngine(t)
	var root []byte
	for i := range c.plainKeys {
		require.NoError(t, ms.applyPlainUpdates(c.plainKeys[i:i+1], c.updates[i:i+1]))
		root = bytes.Clone(pbinTestProcess(t, pph, c.plainKeys[i:i+1], c.updates[i:i+1]))
	}
	return ms, root
}

func pbinTestUniqueReprCorpora() []struct {
	name   string
	corpus *pbinTestCorpus
} {
	return []struct {
		name   string
		corpus *pbinTestCorpus
	}{
		{
			name: "accounts",
			corpus: new(pbinTestCorpus).
				account(pbinOracleAddr(71), 1, 999860099, common.Hash{0x01}).
				account(pbinOracleAddr(72), 3, 900234, common.Hash{0x02}).
				account(pbinOracleAddr(73), 0, 0, common.Hash{}).
				account(pbinOracleAddr(74), 7, 2000000000000138901, common.Hash{0x04}),
		},
		{
			name: "storage across both zones",
			corpus: new(pbinTestCorpus).
				storage(pbinOracleAddr(75), pbinOracleSlot(0), 0x01).
				storage(pbinOracleAddr(75), pbinOracleSlot(63), 0x02).
				storage(pbinOracleAddr(75), pbinOracleSlot(64), 0x03).
				storage(pbinOracleAddr(75), pbinOracleSlot(256), 0x04).
				storage(pbinOracleAddr(76), pbinOracleSlot(256), 0x05).
				storage(pbinOracleAddr(76), pbinOracleSlot(257), 0x06),
		},
		{name: "mixed accounts and storage", corpus: pbinTestMixedCorpus()},
		{name: "deep shared prefix", corpus: pbinTestDeepSharedPrefixCorpus()},
	}
}

// TestPBinUniqueRepresentation ports Test_HexPatriciaHashed_UniqueRepresentation
// and its variants: the root follows the state the keys leave behind, not the
// order they arrive in nor how many Process calls they are split across.
func TestPBinUniqueRepresentation(t *testing.T) {
	t.Parallel()

	for _, tc := range pbinTestUniqueReprCorpora() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			want := tc.corpus.oracleRoot(t)
			leaves := tc.corpus.leafCount(t)

			for name, ordered := range pbinTestOrderings(t, tc.corpus) {
				_, batchState, batchRoot := pbinTestBatches(t, ordered)
				require.Equal(t, want, batchRoot, "batch, ordering %s", name)
				pbinTestVerifyRecords(t, batchState, batchRoot, leaves)

				seqState, seqRoot := pbinTestProcessSeq(t, ordered)
				require.Equal(t, want, seqRoot, "sequential, ordering %s", name)
				pbinTestVerifyRecords(t, seqState, seqRoot, leaves)
			}
		})
	}
}

// TestPBinUniqueRepresentationAcrossRounds ports
// Test_HexPatriciaHashed_UniqueRepresentation2: a second round of updates lands
// on trees built two different ways, and both must still agree.
func TestPBinUniqueRepresentationAcrossRounds(t *testing.T) {
	t.Parallel()

	addrs := [][]byte{pbinOracleAddr(81), pbinOracleAddr(82), pbinOracleAddr(83)}
	round1 := new(pbinTestCorpus).
		account(addrs[0], 1, 999860099, common.Hash{0x01}).
		account(addrs[1], 3, 900234, common.Hash{0x02}).
		storage(addrs[1], pbinOracleSlot(64), 0x01).
		account(addrs[2], 0, 2000000000000138901, common.Hash{0x03})
	round2 := new(pbinTestCorpus).
		account(addrs[0], 2, 2345234560099, common.Hash{0x11}).
		storage(addrs[1], pbinOracleSlot(64), 0x02).
		storage(addrs[1], pbinOracleSlot(1000), 0x03)

	pphBatch, batchState := pbinTestEngine(t)
	pphSeq, seqState := pbinTestEngine(t)

	batchRoot := func(c *pbinTestCorpus) []byte {
		require.NoError(t, batchState.applyPlainUpdates(c.plainKeys, c.updates))
		return bytes.Clone(pbinTestProcess(t, pphBatch, c.plainKeys, c.updates))
	}
	seqRoot := func(c *pbinTestCorpus) []byte {
		var root []byte
		for i := range c.plainKeys {
			require.NoError(t, seqState.applyPlainUpdates(c.plainKeys[i:i+1], c.updates[i:i+1]))
			root = bytes.Clone(pbinTestProcess(t, pphSeq, c.plainKeys[i:i+1], c.updates[i:i+1]))
		}
		return root
	}

	require.Equal(t, round1.oracleRoot(t), batchRoot(round1))
	require.Equal(t, round1.oracleRoot(t), seqRoot(round1))

	union := pbinTestUnion(round1, round2)
	root := batchRoot(round2)
	require.Equal(t, union.oracleRoot(t), root)
	require.Equal(t, root, seqRoot(round2))

	pbinTestVerifyRecords(t, batchState, root, union.leafCount(t))
	pbinTestVerifyRecords(t, seqState, root, union.leafCount(t))
}
