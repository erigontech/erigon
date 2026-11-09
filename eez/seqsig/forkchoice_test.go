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

package seqsig_test

import (
	"encoding/binary"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/eez/seqsig"
	"github.com/erigontech/erigon/execution/types"
)

type header struct {
	number uint64
	parent common.Hash
}

// localChain is a node's own chain: canonical hashes by number and the
// headers it has by hash, with hashes chosen freely.
type localChain struct {
	safe      *seqsig.BlockID
	canonical map[uint64]common.Hash
	headers   map[common.Hash]header
}

func newChain() *localChain {
	return &localChain{canonical: map[uint64]common.Hash{}, headers: map[common.Hash]header{}}
}

func (c *localChain) withSafe(number uint64, hash common.Hash) *localChain {
	c.safe = &seqsig.BlockID{Number: number, Hash: hash}
	return c
}

func (c *localChain) canon(hash common.Hash, number uint64, parent common.Hash) *localChain {
	c.canonical[number] = hash
	return c.known(hash, number, parent)
}

func (c *localChain) known(hash common.Hash, number uint64, parent common.Hash) *localChain {
	c.headers[hash] = header{number, parent}
	return c
}

func (c *localChain) canonBlocks(blocks ...*types.Block) *localChain {
	for _, b := range blocks {
		c.canon(b.Hash(), b.NumberU64(), b.ParentHash())
	}
	return c
}

func (c *localChain) knownBlocks(blocks ...*types.Block) *localChain {
	for _, b := range blocks {
		c.known(b.Hash(), b.NumberU64(), b.ParentHash())
	}
	return c
}

func (c *localChain) Head() (seqsig.BlockID, error) {
	var head seqsig.BlockID
	for n, h := range c.canonical {
		if n >= head.Number {
			head = seqsig.BlockID{Number: n, Hash: h}
		}
	}
	return head, nil
}

func (c *localChain) Safe() (seqsig.BlockID, bool, error) {
	if c.safe == nil {
		return seqsig.BlockID{}, false, nil
	}
	return *c.safe, true, nil
}

func (c *localChain) Header(hash common.Hash) (uint64, common.Hash, bool, error) {
	h, ok := c.headers[hash]
	return h.number, h.parent, ok, nil
}

func (c *localChain) CanonicalHash(number uint64) (common.Hash, bool, error) {
	h, ok := c.canonical[number]
	return h, ok, nil
}

func h(b byte) common.Hash { return repeat(b) }

// hn is a synthetic hash for height n.
func hn(n uint64) common.Hash {
	var out common.Hash
	out[0] = 0xab
	binary.BigEndian.PutUint64(out[24:], n)
	return out
}

func id(number uint64, hash common.Hash) seqsig.BlockID {
	return seqsig.BlockID{Number: number, Hash: hash}
}

// TestExtendsSafe pins whether a block descends from safe: extends,
// conflicts, or unverifiable from the headers the node has.
func TestExtendsSafe(t *testing.T) {
	// hn(1)…hn(1025) on hn(0), with safe at hn(0).
	deep := newChain().withSafe(0, hn(0))
	for n := uint64(1); n <= 1025; n++ {
		deep.known(hn(n), n, hn(n-1))
	}
	for _, c := range []struct {
		name   string
		chain  *localChain
		id     seqsig.BlockID
		parent common.Hash
		want   string
	}{
		{"no safe anchor", newChain(), id(1, h(1)), h(0), "extends"},
		{"at the safe height, the safe block", newChain().withSafe(10, h(10)), id(10, h(10)), h(9), "extends"},
		{"at the safe height, another block", newChain().withSafe(10, h(10)), id(10, h(99)), h(9), "conflicts"},
		{"below safe", newChain().withSafe(10, h(10)), id(9, h(9)), h(8), "conflicts"},
		{"directly above safe, on safe", newChain().withSafe(10, h(10)), id(11, h(11)), h(10), "extends"},
		{"directly above safe, on another block", newChain().withSafe(10, h(10)), id(11, h(11)), h(99), "conflicts"},
		{
			"canonical, without a parent walk",
			newChain().withSafe(10, h(10)).canon(h(12), 12, h(99)),
			id(12, h(12)), h(99), "extends",
		},
		{
			"known ancestry down to safe",
			newChain().withSafe(10, h(10)).known(h(12), 12, h(11)).known(h(11), 11, h(10)),
			id(13, h(13)), h(12), "extends",
		},
		{
			"known ancestry off safe at the safe height",
			newChain().withSafe(10, h(10)).known(h(12), 12, h(11)).known(h(11), 11, h(99)),
			id(13, h(13)), h(12), "conflicts",
		},
		{"missing ancestry", newChain().withSafe(10, h(10)), id(12, h(12)), h(11), "unverifiable"},
		{"1,024 headers down to safe", deep, id(1025, h(1)), hn(1024), "extends"},
		{"1,025 headers down to safe", deep, id(1026, h(1)), hn(1025), "unverifiable"},
	} {
		got, err := seqsig.ExtendsSafe(c.chain, c.chain.safe, c.id, c.parent)
		require.NoError(t, err, c.name)
		require.Equal(t, c.want, got, c.name)
	}
}

func blk(number uint64, parent common.Hash, salt byte) *types.Block {
	return types.NewBlock(&types.Header{
		ParentHash: parent,
		Number:     *uint256.NewInt(number),
		Extra:      []byte{salt},
	}, nil, nil, nil, nil, nil)
}

// line returns blocks 0…n-1, each the parent of the next.
func line(n int) []*types.Block {
	out := []*types.Block{blk(0, common.Hash{}, 0)}
	for len(out) < n {
		out = append(out, blk(uint64(len(out)), out[len(out)-1].Hash(), 0))
	}
	return out
}

// signedHead is a verified head for b, carrying b when withBlock is set.
func signedHead(b *types.Block, epochMs, counter uint64, withBlock bool) *seqsig.VerifiedHead {
	v := &seqsig.VerifiedHead{Head: seqsig.Head{
		RollupID:   1,
		Number:     b.NumberU64(),
		BlockHash:  b.Hash(),
		ParentHash: b.ParentHash(),
		EpochMs:    epochMs,
		Counter:    counter,
	}}
	if withBlock {
		v.Block = b
	}
	return v
}

func serving(blocks ...*types.Block) map[common.Hash]*types.Block {
	m := make(map[common.Hash]*types.Block, len(blocks))
	for _, b := range blocks {
		m[b.Hash()] = b
	}
	return m
}

// blocksBack serves up to count blocks back from from, newest first.
func blocksBack(src map[common.Hash]*types.Block, from common.Hash, count uint64) []*types.Block {
	var out []*types.Block
	for hash := from; uint64(len(out)) < count; {
		b, ok := src[hash]
		if !ok {
			break
		}
		out = append(out, b)
		hash = b.ParentHash()
	}
	return out
}

type decideCase struct {
	name       string
	chain      *localChain
	source     map[common.Hash]*types.Block
	candidates []*seqsig.VerifiedHead
	indexed    uint64
	noPeers    bool

	action  seqsig.Action
	target  *seqsig.VerifiedHead
	err     error
	blocks  []*types.Block
	counts  []uint64
	skipped []*seqsig.VerifiedHead
}

// decide carries out every Fetch against the case's source until a decision
// moves or leaves the head, and returns it with the size of each fetch.
func decide(t *testing.T, c decideCase) (seqsig.Decision, []uint64) {
	t.Helper()
	in := seqsig.Inputs{
		Candidates: c.candidates,
		Indexed:    c.indexed,
		Peers:      1,
		Fetched:    map[common.Hash][]*types.Block{},
	}
	if c.noPeers {
		in.Peers = 0
	}
	var counts []uint64
	for range 2 * seqsig.MaxAncestryWalk {
		d, err := seqsig.Decide(c.chain, in)
		require.NoError(t, err)
		if d.Action != seqsig.Fetch {
			return d, counts
		}
		require.NotContains(t, in.Fetched, d.From)
		counts = append(counts, d.Count)
		in.Fetched[d.From] = blocksBack(c.source, d.From, d.Count)
	}
	t.Fatal("no decision")
	return seqsig.Decision{}, nil
}

func hashes(blocks []*types.Block) []common.Hash {
	var out []common.Hash
	for _, b := range blocks {
		out = append(out, b.Hash())
	}
	return out
}

// TestDecideFetch pins the walk from a target back to the first local block,
// with no safe block set.
func TestDecideFetch(t *testing.T) {
	b := line(3)
	bx := blk(1, b[0].Hash(), 0xff)
	long := line(151)
	deep := line(1026)
	beyond := blk(seqsig.MaxAncestryWalk+1, h(0xaa), 0)
	atBound := blk(seqsig.MaxAncestryWalk, h(0xaa), 0)

	t2 := signedHead(b[2], 100, 1, true)
	t0 := signedHead(b[0], 100, 1, true)
	t1 := signedHead(b[1], 100, 1, true)
	t1bare := signedHead(b[1], 100, 1, false)
	tLong := signedHead(long[150], 100, 1, true)
	tDeep := signedHead(deep[1025], 100, 1, true)
	tBeyond := signedHead(beyond, 100, 1, true)
	tAtBound := signedHead(atBound, 100, 1, true)

	ones := make([]uint64, 1023)
	for i := range ones {
		ones[i] = 1
	}

	for _, c := range []decideCase{
		{
			name:  "the walk stops at the first local block",
			chain: newChain().canonBlocks(b[0]), source: serving(b[1]), candidates: []*seqsig.VerifiedHead{t2},
			action: seqsig.Adopt, target: t2, blocks: b[1:3], counts: []uint64{1},
		},
		{
			name:  "a target that is the local head needs no move",
			chain: newChain().canonBlocks(b[0]), candidates: []*seqsig.VerifiedHead{t0},
			action: seqsig.Ignore, target: t0,
		},
		{
			name:  "a served ancestor with another hash is refused",
			chain: newChain().canonBlocks(b[0]), source: map[common.Hash]*types.Block{b[1].Hash(): bx},
			candidates: []*seqsig.VerifiedHead{t2},
			action:     seqsig.Ignore, target: t2, err: seqsig.ErrBadAncestor, counts: []uint64{1},
		},
		{
			// An unrelated local head at the target's height lets the walk start.
			name:  "a walk past 1,024 blocks is unverifiable",
			chain: newChain().canon(h(0xee), 1025, h(0xed)), source: serving(deep...),
			candidates: []*seqsig.VerifiedHead{tDeep},
			action:     seqsig.Ignore, target: tDeep, err: seqsig.ErrUnverifiable, counts: ones,
		},
		{
			name:  "a target more than 1,024 blocks ahead is not walked",
			chain: newChain().canonBlocks(b[0]), candidates: []*seqsig.VerifiedHead{tBeyond},
			action: seqsig.Ignore, target: tBeyond, err: seqsig.ErrAhead,
		},
		{
			name:  "a target 1,024 blocks ahead is walked",
			chain: newChain().canonBlocks(b[0]), candidates: []*seqsig.VerifiedHead{tAtBound},
			action: seqsig.Ignore, target: tAtBound, err: seqsig.ErrNotServed, counts: []uint64{seqsig.FetchBatch},
		},
		{
			name:  "fetches are sized to the gap, at most 64 blocks",
			chain: newChain().canonBlocks(long[0]), source: serving(long...), candidates: []*seqsig.VerifiedHead{tLong},
			action: seqsig.Adopt, target: tLong, blocks: long[1:], counts: []uint64{64, 64, 21},
		},
		{
			name:  "without a connected peer nothing is fetched",
			chain: newChain().canonBlocks(b[0]), source: serving(b[1]), candidates: []*seqsig.VerifiedHead{t2}, noPeers: true,
			action: seqsig.Ignore, target: t2, err: seqsig.ErrNoPeers,
		},
		{
			name:  "held with no safe block while derivation has indexed a batch",
			chain: newChain().canonBlocks(b[0]), candidates: []*seqsig.VerifiedHead{t1}, indexed: 1,
			action: seqsig.Ignore, target: t1, err: seqsig.ErrHeld,
		},
		{
			name:  "a target in hand on a local parent needs no peer",
			chain: newChain().canonBlocks(b[0]), candidates: []*seqsig.VerifiedHead{t1}, noPeers: true,
			action: seqsig.Adopt, target: t1, blocks: b[1:2],
		},
		{
			name:  "a head without its block is fetched by hash",
			chain: newChain().canonBlocks(b[0]), source: serving(b[1]), candidates: []*seqsig.VerifiedHead{t1bare},
			action: seqsig.Adopt, target: t1bare, blocks: b[1:2], counts: []uint64{1},
		},
		{
			name:  "an ancestor nobody serves stops the walk",
			chain: newChain().canonBlocks(b[0]), candidates: []*seqsig.VerifiedHead{t2},
			action: seqsig.Ignore, target: t2, err: seqsig.ErrNotServed, counts: []uint64{1},
		},
	} {
		t.Run(c.name, func(t *testing.T) { check(t, c) })
	}
}

// TestDecideForkChoice pins the choice among candidates against safe, on a
// local chain c0…c10 with safe at c10 unless a case says otherwise.
func TestDecideForkChoice(t *testing.T) {
	c := line(13)
	base := func() *localChain { return newChain().canonBlocks(c[:11]...).withSafe(10, c[10].Hash()) }

	a11 := blk(11, c[10].Hash(), 1)
	a12 := blk(12, a11.Hash(), 1)
	a13 := blk(13, a12.Hash(), 1)
	b11 := blk(11, c[10].Hash(), 2)
	b12 := blk(12, b11.Hash(), 2)
	below := blk(9, c[8].Hash(), 3)
	x10 := blk(10, c[9].Hash(), 4)
	x11 := blk(11, x10.Hash(), 4)
	x12 := blk(12, x11.Hash(), 4)
	y16 := blk(16, h(0x15), 5)

	headA11 := signedHead(a11, 200, 1, true)
	headA12 := signedHead(a12, 200, 2, true)
	headA13 := signedHead(a13, 200, 3, true)
	headB11 := signedHead(b11, 100, 5, true)
	headB12 := signedHead(b12, 100, 6, true)
	headBelow := signedHead(below, 300, 1, true)
	headX12 := signedHead(x12, 300, 1, true)
	headY16 := signedHead(y16, 300, 1, true)
	headC11 := signedHead(c[11], 300, 1, false)
	headC12 := signedHead(c[12], 300, 1, false)

	for _, tc := range []decideCase{
		{
			name: "no candidate leaves the head", chain: base(),
			action: seqsig.Ignore,
		},
		{
			name: "the first candidate that descends from safe wins", chain: base(),
			candidates: []*seqsig.VerifiedHead{headA11, headB11},
			action:     seqsig.Adopt, target: headA11, blocks: []*types.Block{a11},
		},
		{
			name: "a provable conflict with safe is skipped", chain: base(),
			candidates: []*seqsig.VerifiedHead{headBelow, headB11},
			action:     seqsig.Adopt, target: headB11, blocks: []*types.Block{b11}, skipped: []*seqsig.VerifiedHead{headBelow},
		},
		{
			name: "unknown ancestry is not a conflict", chain: base(),
			candidates: []*seqsig.VerifiedHead{headA13, headB11},
			action:     seqsig.Ignore, target: headA13, err: seqsig.ErrNotServed, counts: []uint64{2},
		},
		{
			name: "missing ancestors are fetched and adopted with the head", chain: base(), source: serving(a11, a12),
			candidates: []*seqsig.VerifiedHead{headA13, headB11},
			action:     seqsig.Adopt, target: headA13, blocks: []*types.Block{a11, a12, a13}, counts: []uint64{2},
		},
		{
			name: "held while derivation has indexed a batch above safe", chain: base(), indexed: 11,
			candidates: []*seqsig.VerifiedHead{headB11},
			action:     seqsig.Ignore, target: headB11, err: seqsig.ErrHeld,
		},
		{
			name: "a conflict is skipped before the hold", chain: base(), indexed: 11,
			candidates: []*seqsig.VerifiedHead{headBelow, headB11},
			action:     seqsig.Ignore, target: headB11, err: seqsig.ErrHeld, skipped: []*seqsig.VerifiedHead{headBelow},
		},
		{
			name: "derivation indexed up to safe holds nothing", chain: base(), indexed: 10,
			candidates: []*seqsig.VerifiedHead{headB11},
			action:     seqsig.Adopt, target: headB11, blocks: []*types.Block{b11},
		},
		{
			name: "the preferred chain is followed while it descends from safe", chain: base().canonBlocks(a11),
			candidates: []*seqsig.VerifiedHead{headA12, headB12},
			action:     seqsig.Adopt, target: headA12, blocks: []*types.Block{a12},
		},
		{
			name:       "failover when L1 settles a block the preferred chain lacks",
			chain:      base().knownBlocks(a11).canonBlocks(b11).withSafe(11, b11.Hash()),
			candidates: []*seqsig.VerifiedHead{headA12, headB12},
			action:     seqsig.Adopt, target: headB12, blocks: []*types.Block{b12}, skipped: []*seqsig.VerifiedHead{headA12},
		},
		{
			name: "an ancestry fetched down below safe is skipped", chain: base(), source: serving(x10, x11),
			candidates: []*seqsig.VerifiedHead{headX12, headB11},
			action:     seqsig.Adopt, target: headB11, blocks: []*types.Block{b11}, counts: []uint64{1, 1},
			skipped: []*seqsig.VerifiedHead{headX12},
		},
		{
			name: "a fork point with unknown ancestry down to safe stops the decision", chain: base().known(h(0x15), 15, h(0x14)),
			candidates: []*seqsig.VerifiedHead{headY16, headB11},
			action:     seqsig.Ignore, target: headY16, err: seqsig.ErrUnknownAncestry,
		},
		{
			name: "a known candidate below the head is a rewind", chain: newChain().canonBlocks(c...).withSafe(10, c[10].Hash()),
			candidates: []*seqsig.VerifiedHead{headC11},
			action:     seqsig.Rewind, target: headC11,
		},
		{
			name: "the head itself needs no move", chain: newChain().canonBlocks(c...).withSafe(10, c[10].Hash()),
			candidates: []*seqsig.VerifiedHead{headC12},
			action:     seqsig.Ignore, target: headC12,
		},
	} {
		t.Run(tc.name, func(t *testing.T) { check(t, tc) })
	}
}

func check(t *testing.T, c decideCase) {
	t.Helper()
	d, counts := decide(t, c)
	require.Equal(t, c.action, d.Action, "decision error: %v", d.Err)
	require.Same(t, c.target, d.Target)
	if c.err == nil {
		require.NoError(t, d.Err)
	} else {
		require.ErrorIs(t, d.Err, c.err)
	}
	require.Equal(t, hashes(c.blocks), hashes(d.Blocks))
	require.Equal(t, c.counts, counts)
	var skipped []*seqsig.VerifiedHead
	for _, s := range d.Skipped {
		require.ErrorIs(t, s.Err, seqsig.ErrConflictsSafe)
		skipped = append(skipped, s.Head)
	}
	require.Equal(t, c.skipped, skipped)
}

// TestDecideFollowsTrackerOrder pins that the highest version wins whatever
// order the heads arrived in.
func TestDecideFollowsTrackerOrder(t *testing.T) {
	c := line(11)
	chain := newChain().canonBlocks(c...).withSafe(10, c[10].Hash())
	old := signedHead(blk(11, c[10].Hash(), 1), 100, 9, true)
	restarted := signedHead(blk(11, c[10].Hash(), 2), 200, 1, true)
	old.Signer, restarted.Signer = common.HexToAddress("0x01"), common.HexToAddress("0x01")

	var tr seqsig.Tracker
	for _, v := range []*seqsig.VerifiedHead{restarted, old} {
		o, _ := tr.Observe(v, 200)
		require.Equal(t, seqsig.ObservedNew, o)
	}
	d, err := seqsig.Decide(chain, seqsig.Inputs{Candidates: tr.Candidates(), Peers: 1})
	require.NoError(t, err)
	require.Equal(t, seqsig.Adopt, d.Action)
	require.Same(t, restarted, d.Target)
}
