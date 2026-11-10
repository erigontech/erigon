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

package sequencer_test

import (
	"context"
	"crypto/ecdsa"
	"sync"
	"testing"
	"time"

	"github.com/golang/snappy"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/eez/config"
	"github.com/erigontech/erigon/eez/deriver/derivertest"
	"github.com/erigontech/erigon/eez/follower"
	"github.com/erigontech/erigon/eez/protocol"
	"github.com/erigontech/erigon/eez/protocol/da"
	"github.com/erigontech/erigon/eez/seqsig"
	"github.com/erigontech/erigon/eez/sequencer"
	"github.com/erigontech/erigon/execution/types"
)

// The known answers of eez/seqsig (T-HEAD-1): development key #0 signs block
// 7 on parent 0x11…11, every other header field at its zero or empty-trie
// value and an empty body, as counter 1 of epoch 1,700,000,000,000, rollup 1,
// on L2 chain 1337 with registry 0x…aa. wireKeccak is the Keccak-256 of the
// envelope's RLP with the block attached.
const (
	kaKeyHex       = "ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
	kaSignerHex    = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"
	kaBlockHashHex = "0x0c467060700fd4695ec429a3f1eff9670ac6098f428f1552ec22ded432f3767b"
	kaSignatureHex = "0x7b5b31c607467a2dd273bfd2cc34506477fa1da5f5bb8b0ec77c94855196d08e0a6bb082f1d409a8a8cb32b02f0eb4e2fdb53c12abf7a2bbfa50fa72f59493ea1c"
	kaWireKeccak   = "0xe133b68e84de19ba125e6a6ffb7ee01a8e9e77123e09edf43736fa3b4387cdac"
	kaEpochMs      = 1_700_000_000_000
)

var (
	kaDomain = seqsig.Domain{ChainID: 1337, Registry: common.HexToAddress("0xaa"), RollupID: 1}
	kaParent = common.HexToHash("0x1111111111111111111111111111111111111111111111111111111111111111")
)

func kaKey(t *testing.T) *ecdsa.PrivateKey {
	t.Helper()
	k, err := crypto.HexToECDSA(kaKeyHex)
	require.NoError(t, err)
	return k
}

// kaBlock returns block number on kaParent with an empty body.
func kaBlock(number uint64) *types.Block {
	return types.NewBlock(&types.Header{ParentHash: kaParent, Root: empty.RootHash, Number: *uint256.NewInt(number)}, nil, nil, nil, nil, nil)
}

// blockSet is a chain of the blocks added to it, each canonical. It counts
// the reads of each hash and calls onRead, when set, on each.
type blockSet struct {
	onRead func(common.Hash)

	mu     sync.Mutex
	blocks map[common.Hash]*types.Block
	read   map[common.Hash]int
}

func newBlockSet(blocks ...*types.Block) *blockSet {
	s := &blockSet{blocks: map[common.Hash]*types.Block{}, read: map[common.Hash]int{}}
	s.add(blocks...)
	return s
}

func (s *blockSet) reads(hash common.Hash) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.read[hash]
}

func (s *blockSet) add(blocks ...*types.Block) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, b := range blocks {
		s.blocks[b.Hash()] = b
	}
}

func (s *blockSet) CanonicalBlock(_ context.Context, hash common.Hash) (*types.Block, error) {
	if s.onRead != nil {
		s.onRead(hash)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.read[hash]++
	return s.blocks[hash], nil
}

// startSigner runs the signer of cfg until t ends.
func startSigner(t *testing.T, cfg sequencer.SignerConfig) *sequencer.HeadSigner {
	t.Helper()
	s, err := sequencer.NewHeadSigner(cfg, log.New())
	require.NoError(t, err)
	require.NoError(t, s.Start())
	t.Cleanup(func() { require.NoError(t, s.Stop()) })
	return s
}

// waitSigned waits for s to have signed hash as counter.
func waitSigned(t *testing.T, s *sequencer.HeadSigner, hash common.Hash, counter uint64) seqsig.Envelope {
	t.Helper()
	var e seqsig.Envelope
	require.Eventually(t, func() bool {
		var ok bool
		e, ok = s.Latest()
		return ok && e.Head.BlockHash == hash && e.Head.Counter == counter
	}, 5*time.Second, time.Millisecond, "head %x as counter %d, latest %+v", hash, counter, e.Head)
	return e
}

// requireLatest checks that s's latest envelope is hash as counter.
func requireLatest(t *testing.T, s *sequencer.HeadSigner, hash common.Hash, counter uint64) {
	t.Helper()
	e, ok := s.Latest()
	require.True(t, ok)
	require.Equal(t, hash, e.Head.BlockHash)
	require.Equal(t, counter, e.Head.Counter)
}

// TestHeadSignerKnownAnswers is T-SEQ-6 against T-HEAD-1: the envelope of
// the known-answer block, signed first by a process of the known-answer
// epoch, carries its signature and wire encoding, and the next head takes
// the next counter.
func TestHeadSignerKnownAnswers(t *testing.T) {
	b7, b8 := kaBlock(7), kaBlock(8)
	s := startSigner(t, sequencer.SignerConfig{Key: kaKey(t), Domain: kaDomain, EpochMs: kaEpochMs, Blocks: newBlockSet(b7, b8)})
	require.Equal(t, common.HexToAddress(kaSignerHex), s.Address())
	require.Equal(t, uint64(kaEpochMs), s.EpochMs())
	_, ok := s.Latest()
	require.False(t, ok)

	require.Equal(t, common.HexToHash(kaBlockHashHex), b7.Hash())
	s.Moved(b7.Hash())
	e := waitSigned(t, s, b7.Hash(), 1)
	require.Equal(t, seqsig.Head{RollupID: 1, Number: 7, BlockHash: b7.Hash(), ParentHash: kaParent, EpochMs: kaEpochMs, Counter: 1}, e.Head)
	require.Equal(t, common.FromHex(kaSignatureHex), e.Signature)
	data, err := seqsig.Encode(e)
	require.NoError(t, err)
	enc, err := snappy.Decode(nil, data)
	require.NoError(t, err)
	require.Equal(t, common.HexToHash(kaWireKeccak), crypto.Keccak256Hash(enc))

	s.Moved(b8.Hash())
	e = waitSigned(t, s, b8.Hash(), 2)
	v, err := e.Verify(kaDomain, common.HexToAddress(kaSignerHex))
	require.NoError(t, err)
	require.Equal(t, b8.Hash(), v.Block.Hash())
}

// TestHeadSignerSkips: the signer waits up to a second for a head's block,
// then gives up on it; a head whose block stays unreadable, or one replaced
// before its block is readable, is never signed and takes no counter. A head
// handed again is not signed again; the head moving back to a block signed
// before is.
func TestHeadSignerSkips(t *testing.T) {
	a, b, c, d, e := kaBlock(1), kaBlock(2), kaBlock(3), kaBlock(4), kaBlock(5)
	blocks := newBlockSet()
	s := startSigner(t, sequencer.SignerConfig{Key: kaKey(t), Domain: kaDomain, Blocks: blocks})

	s.Moved(a.Hash())
	time.Sleep(300 * time.Millisecond)
	_, ok := s.Latest()
	require.False(t, ok, "A signed before its block is readable")
	blocks.add(a)
	waitSigned(t, s, a.Hash(), 1)

	s.Moved(b.Hash())
	time.Sleep(1300 * time.Millisecond)
	reads := blocks.reads(b.Hash())
	require.GreaterOrEqual(t, reads, 10, "B was not retried")
	time.Sleep(300 * time.Millisecond)
	require.Equal(t, reads, blocks.reads(b.Hash()), "B still read past the deadline")
	requireLatest(t, s, a.Hash(), 1)
	blocks.add(c)
	s.Moved(c.Hash())
	waitSigned(t, s, c.Hash(), 2)

	s.Moved(d.Hash())
	time.Sleep(100 * time.Millisecond)
	blocks.add(e)
	start := time.Now()
	s.Moved(e.Hash())
	waitSigned(t, s, e.Hash(), 3)
	require.Less(t, time.Since(start), 500*time.Millisecond, "E waited for D's read deadline")
	blocks.add(d)

	s.Moved(e.Hash())
	time.Sleep(200 * time.Millisecond)
	requireLatest(t, s, e.Hash(), 3)
	s.Moved(c.Hash())
	waitSigned(t, s, c.Hash(), 4)
}

// stalledBlocks is a chain whose reads hang until their context ends.
type stalledBlocks struct{}

func (stalledBlocks) CanonicalBlock(ctx context.Context, _ common.Hash) (*types.Block, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

// TestHeadSignerNeverBlocks: handing heads to a signer whose reads hang
// returns at once.
func TestHeadSignerNeverBlocks(t *testing.T) {
	s := startSigner(t, sequencer.SignerConfig{Key: kaKey(t), Domain: kaDomain, Blocks: stalledBlocks{}})
	handed := make(chan struct{})
	go func() {
		defer close(handed)
		for i := range 10_000 {
			s.Moved(common.Hash{byte(i), byte(i >> 8)})
		}
	}()
	select {
	case <-handed:
	case <-time.After(time.Second):
		t.Fatal("Moved blocked")
	}
}

// TestHeadSignerSignsReadHead: a head whose block was read is signed although
// a newer head arrived during the read; the newer head follows.
func TestHeadSignerSignsReadHead(t *testing.T) {
	d, e := kaBlock(1), kaBlock(2)
	blocks := newBlockSet(d, e)
	var s *sequencer.HeadSigner
	blocks.onRead = func(h common.Hash) {
		if h == d.Hash() {
			s.Moved(e.Hash())
		}
	}
	s = startSigner(t, sequencer.SignerConfig{Key: kaKey(t), Domain: kaDomain, Blocks: blocks})
	s.Moved(d.Hash())
	waitSigned(t, s, e.Hash(), 2)
}

// TestHeadSignerEpoch: without a configured epoch, the signer takes the time
// it is handed its first head, after boot, not its construction time.
func TestHeadSignerEpoch(t *testing.T) {
	a := kaBlock(1)
	s := startSigner(t, sequencer.SignerConfig{Key: kaKey(t), Domain: kaDomain, Blocks: newBlockSet(a)})
	require.Zero(t, s.EpochMs())
	time.Sleep(50 * time.Millisecond)
	handed := uint64(time.Now().UnixMilli())
	s.Moved(a.Hash())
	e := waitSigned(t, s, a.Hash(), 1)
	require.GreaterOrEqual(t, e.Head.EpochMs, handed)
	require.Equal(t, e.Head.EpochMs, s.EpochMs())
}

// signing runs the harness's follower with a head signer, and records the
// heads the follower hands it and the envelopes seen.
type signing struct {
	*harness
	signer *sequencer.HeadSigner
	domain seqsig.Domain
	key    *ecdsa.PrivateKey

	mu    sync.Mutex
	moves []common.Hash
	// signed are the heads expected signed, in order.
	signed []common.Hash
}

func newSigning(t *testing.T, h *harness) *signing {
	t.Helper()
	cc := derivertest.Config()
	l2 := cc.L2.(*config.L2Config)
	return &signing{harness: h, domain: seqsig.Domain{ChainID: cc.ChainID.Uint64(), Registry: l2.Registry, RollupID: l2.RollupID}, key: kaKey(t)}
}

// start runs a new process: a signer of the current epoch and a follower that
// hands it its heads.
func (sg *signing) start(t *testing.T) {
	t.Helper()
	s, err := sequencer.NewHeadSigner(sequencer.SignerConfig{Key: sg.key, Domain: sg.domain, Blocks: sg.chain}, log.New())
	require.NoError(t, err)
	require.NoError(t, s.Start())
	t.Cleanup(func() { require.NoError(t, s.Stop()) })
	sg.mu.Lock()
	sg.signer, sg.moves, sg.signed = s, nil, nil
	sg.mu.Unlock()
	sg.harness.start(t, sg.followerOf(t, func(cfg *follower.Config) {
		cfg.OnL2Head = func(head common.Hash) {
			sg.mu.Lock()
			sg.moves = append(sg.moves, head)
			sg.mu.Unlock()
			s.Moved(head)
		}
	}))
}

// expect waits for the next head change, to hash, to be signed as the next
// counter of the process's epoch, its block attached, and checks that every
// head the follower handed over so far was signed once.
func (sg *signing) expect(t *testing.T, hash common.Hash) seqsig.Envelope {
	t.Helper()
	sg.signed = append(sg.signed, hash)
	e := waitSigned(t, sg.signer, hash, uint64(len(sg.signed)))
	require.Equal(t, sg.signer.EpochMs(), e.Head.EpochMs)
	v, err := e.Verify(sg.domain, crypto.PubkeyToAddress(sg.key.PublicKey))
	require.NoError(t, err)
	require.NotNil(t, v.Block)
	require.Equal(t, hash, v.Block.Hash())
	sg.mu.Lock()
	defer sg.mu.Unlock()
	require.Equal(t, sg.signed, sg.moves, "heads handed to the signer")
	return e
}

// inbound delivers value wei from an L1 account to to.
func inbound(to common.Address, value uint64) da.Action {
	return da.Action{
		SourceRollupID: protocol.MainnetRollupID,
		TargetRollupID: 1,
		SourceAddress:  common.HexToAddress("0x11"),
		TargetAddress:  to,
		Value:          *uint256.NewInt(value),
		Data:           []byte{},
		Success:        true,
		ReturnData:     []byte{},
	}
}

// TestSignedHeads is T-SEQ-6: every head change of a sequencer node is
// signed once, under strictly increasing counters, with its block attached.
// Heads boot moves through are not signed, the head boot ends at is. The
// changes cover Produce, Insert, HeadTo, a substitute's rewind and empty
// sibling, a partial settlement's rebuilt Sync block and an L1 reorg's
// retreat. A restarted node signs under a later epoch, from counter 1.
func TestSignedHeads(t *testing.T) {
	c := derivertest.NewComposer(t)
	collector := *derivertest.Config().FeeCollector
	emptyBlock := derivertest.Block{Coinbase: collector, Extra: []byte{}, Txs: [][]byte{}}
	settled := c.Compose(nil, emptyBlock, emptyBlock)
	c.Post(derivertest.Posting{Batch: settled})
	_, chain := derivertest.NewL2(t)
	sg := newSigning(t, &harness{c: c, chain: chain, pool: &queue{}})
	sg.start(t)
	ctx := t.Context()
	hash := func(n uint64) common.Hash { return sg.block(t, n).Hash() }

	first := sg.expect(t, settled.Blocks[1].Hash())
	require.Equal(t, uint64(2), first.Head.Number)

	rc := &raceCommitter{Follower: sg.follower}
	s, err := sequencer.New(derivertest.Config(), sequencer.Config{Timing: devnet()}, rc, sg.pool, log.New())
	require.NoError(t, err)
	for n := uint64(3); n <= 4; n++ {
		require.NoError(t, s.Handle(ctx, sequencer.LiveTick{SyncHeight: 12}))
		sg.expect(t, hash(n))
	}
	emptyFour := hash(4)
	require.NoError(t, sg.follower.HeadTo(ctx, hash(3)))
	sg.expect(t, hash(3))

	failed := sg.prebuilt(t, sg.head(t), c.Transfer(common.HexToAddress("0xb0b"), 1))
	require.NoError(t, sg.follower.Insert(ctx, []*types.Block{failed}, nil))
	sg.expect(t, failed.Hash())

	rc.between = func() {
		rc.between = nil
		sg.expect(t, hash(3))
	}
	sibling, err := s.Substitute(ctx, failed.Header())
	require.NoError(t, err)
	require.Equal(t, emptyFour, sibling.Hash(), "an empty sibling on the same parent")
	sg.expect(t, sibling.Hash())

	partial := c.Compose([]da.Action{inbound(common.HexToAddress("0xa11ce"), 7), inbound(common.HexToAddress("0xb0b"), 8)}, emptyBlock, emptyBlock)
	require.Equal(t, hash(3), partial.Blocks[0].Hash())
	rebuilt := partial.Syncs[1]
	l1Partial := c.Post(derivertest.Posting{Batch: partial, Applied: []int{0, 1}}).Header.Number
	require.Eventually(t, func() bool { return sg.follower.Status().Safe == 4 }, 10*time.Second, 5*time.Millisecond)
	sg.expect(t, rebuilt.Hash())
	c.Rewind(rebuilt.Header())

	for n := uint64(5); n <= 6; n++ {
		require.NoError(t, s.Handle(ctx, sequencer.LiveTick{SyncHeight: 12}))
		sg.expect(t, hash(n))
	}
	sg.settle(t, 5, 5)
	c.ReorgL1(l1Partial)
	sg.expect(t, rebuilt.Hash())
	require.Eventually(t, func() bool { return sg.follower.Status().Safe == 4 }, 10*time.Second, 5*time.Millisecond)

	epoch := sg.signer.EpochMs()
	sg.stop()
	sg.start(t)
	restarted := sg.expect(t, rebuilt.Hash())
	require.Greater(t, restarted.Head.EpochMs, epoch)
}
