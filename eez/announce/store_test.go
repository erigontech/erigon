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

package announce_test

import (
	"cmp"
	"context"
	"crypto/ecdsa"
	"errors"
	"testing"
	"time"

	"github.com/holiman/uint256"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/diagnostics/metrics"
	"github.com/erigontech/erigon/eez/announce"
	"github.com/erigontech/erigon/eez/config"
	"github.com/erigontech/erigon/eez/deriver"
	"github.com/erigontech/erigon/eez/deriver/derivertest"
	"github.com/erigontech/erigon/eez/follower"
	"github.com/erigontech/erigon/eez/sequencer"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/txnprovider"
)

type noTxns struct{}

func (noTxns) ProvideTxns(context.Context, ...txnprovider.ProvideOption) ([]types.Transaction, error) {
	return nil, nil
}

var timing = sequencer.Timing{L1: 12 * time.Second, L2: 2 * time.Second, Proof: 4 * time.Second, Slack: 1500 * time.Millisecond}

// node is a sequencer's chain, its follower and its announcement store.
type node struct {
	t         *testing.T
	l1        *derivertest.Composer
	chain     *deriver.ExecChain
	follower  *follower.Follower
	store     *announce.Store
	key       *ecdsa.PrivateKey
	chainID   *uint256.Int
	genesis   *types.Header
	finalized uint64
}

func newNode(t *testing.T) *node {
	t.Helper()
	c := derivertest.NewComposer(t)
	_, chain := derivertest.NewL2(t)
	cfg := follower.DefaultConfig
	cfg.Poll, cfg.Retry, cfg.MaxRetry = 5*time.Millisecond, 5*time.Millisecond, 20*time.Millisecond
	f, err := follower.New(c.L1, chain, derivertest.Config(), cfg, log.New())
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = f.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	st, err := announce.Open(t.Context(), t.TempDir(), log.New())
	require.NoError(t, err)
	t.Cleanup(st.Close)
	key, err := crypto.ToECDSA(load(t).Valid[0].PrivateKey)
	require.NoError(t, err)
	genesis, err := chain.HeaderByNumber(t.Context(), 0)
	require.NoError(t, err)
	return &node{t: t, l1: c, chain: chain, follower: f, store: st, key: key, chainID: derivertest.Config().ChainID, genesis: genesis}
}

// signer returns a signer of n's blocks into its store.
func (n *node) signer() *announce.Signer {
	return announce.NewSigner(n.key, n.chainID, n.store, n.follower, func() uint64 { return n.finalized }, log.New())
}

// sequencer returns a sequencer of cfg committing through c.
func (n *node) sequencer(cfg sequencer.Config, c sequencer.Committer) *sequencer.Sequencer {
	n.t.Helper()
	cfg.Timing = timing
	s, err := sequencer.New(derivertest.Config(), cfg, c, noTxns{}, log.New())
	require.NoError(n.t, err)
	return s
}

// tick has s produce one Live block.
func (n *node) tick(s *sequencer.Sequencer) {
	n.t.Helper()
	require.NoError(n.t, s.Handle(n.t.Context(), sequencer.LiveTick{SyncHeight: 1_000}))
}

func (n *node) head() *types.Header {
	n.t.Helper()
	h, err := n.chain.Head(n.t.Context())
	require.NoError(n.t, err)
	return h
}

// hash returns the hash of the canonical block number.
func (n *node) hash(number uint64) common.Hash {
	n.t.Helper()
	b, err := n.chain.BlockByNumber(n.t.Context(), number)
	require.NoError(n.t, err)
	return b.Hash()
}

// announced returns the announcement kept for the canonical block number.
func (n *node) announced(number uint64) []byte {
	n.t.Helper()
	env, err := n.store.Get(n.t.Context(), number, n.hash(number))
	require.NoError(n.t, err)
	return env
}

// signed returns the announcement of the canonical block number with n's key.
func (n *node) signed(number uint64) []byte {
	n.t.Helper()
	env, err := announce.Sign(n.key, n.chainID, n.hash(number))
	require.NoError(n.t, err)
	return env
}

// settle posts the batch that settles the local blocks 1 to last, which hold
// no transactions, and waits for derivation to keep them.
func (n *node) settle(last uint64) {
	n.t.Helper()
	var blocks []derivertest.Block
	for i := uint64(1); i <= last; i++ {
		b, err := n.chain.BlockByNumber(n.t.Context(), i)
		require.NoError(n.t, err)
		require.Empty(n.t, b.Transactions())
		blocks = append(blocks, derivertest.Block{Coinbase: b.Coinbase(), Extra: b.Extra(), Txs: [][]byte{}})
	}
	batch := n.l1.Compose(nil, blocks...)
	require.Equal(n.t, n.hash(last), batch.Blocks[len(batch.Blocks)-1].Hash())
	n.l1.Post(derivertest.Posting{Batch: batch})
	require.Eventually(n.t, func() bool { return n.follower.Settled() == last }, 10*time.Second, 5*time.Millisecond)
}

// failures reads eez_sequencer_announcement_failures_total.
func failures() uint64 {
	return metrics.GetOrCreateCounter("eez_sequencer_announcement_failures_total").GetValueUint64()
}

// composer hands the sequencer empty Sync blocks built on a copy of the
// chain.
type composer struct {
	t      *testing.T
	chain  *deriver.ExecChain
	mirror *deriver.ExecChain
	// coinbase is the beneficiary of its blocks, the fee collector when zero.
	coinbase common.Address
	built    []common.Hash
}

func (c *composer) ComposeSyncSlot(ctx context.Context, parent *types.Header, ts, _ uint64, _ sequencer.Mode) *types.Block {
	var blocks []*types.Block
	for n := uint64(1); n <= parent.Number.Uint64(); n++ {
		b, err := c.chain.BlockByNumber(ctx, n)
		require.NoError(c.t, err)
		blocks = append(blocks, b)
	}
	require.NoError(c.t, c.mirror.Insert(ctx, blocks))
	cc := derivertest.Config()
	in := deriver.HeaderInputs{
		Number:     parent.Number.Uint64() + 1,
		ParentHash: parent.Hash(),
		Time:       ts,
		Coinbase:   cmp.Or(c.coinbase, *cc.FeeCollector),
		Extra:      []byte{},
		GasLimit:   cc.L2.(*config.L2Config).BlockGasLimit,
	}
	built, err := c.mirror.Extend(ctx, parent, in, nil, common.Hash{}, common.Hash{})
	require.NoError(c.t, err)
	c.built = append(c.built, built.Block.Hash())
	return built.Block
}

func (*composer) RecoverFailed(context.Context) {}

// insertSpy is a committer that records, for each block inserted, whether
// its announcement was kept already, and which canonical blocks below it had
// theirs.
type insertSpy struct {
	*follower.Follower
	store     *announce.Store
	announced []bool
	below     [][]uint64
}

func (c *insertSpy) Insert(ctx context.Context, blocks []*types.Block, hook follower.CommitHook) error {
	for _, b := range blocks {
		env, err := c.store.Get(ctx, b.NumberU64(), b.Hash())
		if err != nil {
			return err
		}
		c.announced = append(c.announced, env != nil)
		var below []uint64
		for n := uint64(1); n < b.NumberU64(); n++ {
			hash, _, err := c.CanonicalHash(ctx, n)
			if err != nil {
				return err
			}
			if env, err := c.store.Get(ctx, n, hash); err != nil {
				return err
			} else if env != nil {
				below = append(below, n)
			}
		}
		c.below = append(c.below, below)
	}
	return c.Follower.Insert(ctx, blocks, hook)
}

// failing fails every announcement.
type failing struct{ calls int }

func (f *failing) Announce(context.Context, *types.Header) error {
	f.calls++
	return errors.New("disk full")
}

func (f *failing) Record(context.Context, *types.Header) error {
	f.calls++
	return errors.New("disk full")
}

// faulty fails the first put of each block in puts, and every prune while
// prune is set.
type faulty struct {
	*announce.Store
	puts  map[uint64]bool
	prune error
}

func (k *faulty) Put(ctx context.Context, number uint64, hash common.Hash, env []byte) error {
	if k.puts[number] {
		delete(k.puts, number)
		return errors.New("disk full")
	}
	return k.Store.Put(ctx, number, hash, env)
}

func (k *faulty) Prune(ctx context.Context, number uint64) error {
	if k.prune != nil {
		return k.prune
	}
	return k.Store.Prune(ctx, number)
}

// TestRecord is T-SEQ-8 on a sequencer: every block it commits is announced
// with its key, a composer's before it becomes the head, and the store
// serves the announcement of the block at a number until L1 finality passes
// it. A failed announcement is counted and does not stop production.
func TestRecord(t *testing.T) {
	n := newNode(t)
	signer := n.signer()
	require.Equal(t, load(t).Valid[0].Address, signer.Address())
	_, mirror := derivertest.NewL2(t)
	comp := &composer{t: t, chain: n.chain, mirror: mirror}
	spy := &insertSpy{Follower: n.follower, store: n.store}
	s := n.sequencer(sequencer.Config{Composer: comp, Announcements: signer}, spy)
	require.NoError(t, s.Handle(t.Context(), sequencer.SyncSlot{Height: 6, Time: n.genesis.Time + 12, L1Head: 1}))
	head := n.head()
	require.Equal(t, []common.Hash{head.Hash()}, comp.built, "the Sync block is the composer's")
	require.Equal(t, uint64(6), head.Number.Uint64())
	require.Equal(t, []bool{true}, spy.announced, "the composer's block is announced before it is the head")
	for i := uint64(1); i <= 6; i++ {
		require.Equal(t, n.signed(i), n.announced(i), "block %d", i)
		other, err := n.store.Get(t.Context(), i, common.Hash{1})
		require.NoError(t, err)
		require.Nil(t, other, "the hash is checked")
	}

	n.finalized = 4
	n.tick(s)
	for i := uint64(1); i <= 7; i++ {
		require.Equal(t, i >= 4, n.announced(i) != nil, "block %d", i)
	}

	before := failures()
	broken := &failing{}
	n.tick(n.sequencer(sequencer.Config{Announcements: broken}, n.follower))
	require.Equal(t, uint64(8), n.head().Number.Uint64())
	require.Equal(t, 1, broken.calls)
	require.Equal(t, before+1, failures())
}

// TestRecordComposedBlockAlone: before a composer's block becomes the head
// only its own announcement is kept. The unsettled blocks a previous run left
// unannounced are announced once it is the head, against the settled block
// derivation has published by then.
func TestRecordComposedBlockAlone(t *testing.T) {
	n := newNode(t)
	unsigned := n.sequencer(sequencer.Config{}, n.follower)
	for range 5 {
		n.tick(unsigned)
	}
	_, mirror := derivertest.NewL2(t)
	comp := &composer{t: t, chain: n.chain, mirror: mirror}
	spy := &insertSpy{Follower: n.follower, store: n.store}
	s := n.sequencer(sequencer.Config{Composer: comp, Announcements: n.signer()}, spy)
	require.NoError(t, s.Handle(t.Context(), sequencer.SyncSlot{Height: 6, Time: n.genesis.Time + 12, L1Head: 1}))
	require.Equal(t, uint64(6), n.head().Number.Uint64())
	require.Equal(t, []bool{true}, spy.announced, "the composer's block is announced before it is the head")
	require.Equal(t, [][]uint64{nil}, spy.below, "and none below it")
	for i := uint64(1); i <= 6; i++ {
		require.Equal(t, n.signed(i), n.announced(i), "block %d", i)
	}
}

// TestRecordSubstitute: the empty sibling that replaces a failed Sync block
// is announced, its announcement replacing the failed block's.
func TestRecordSubstitute(t *testing.T) {
	n := newNode(t)
	_, mirror := derivertest.NewL2(t)
	// The composer's Sync block pays another beneficiary than its empty
	// sibling, so the two differ.
	comp := &composer{t: t, chain: n.chain, mirror: mirror, coinbase: common.Address{9}}
	s := n.sequencer(sequencer.Config{Composer: comp, Announcements: n.signer()}, n.follower)
	require.NoError(t, s.Handle(t.Context(), sequencer.SyncSlot{Height: 6, Time: n.genesis.Time + 12, L1Head: 1}))
	failed := n.head()
	require.Equal(t, n.signed(6), n.announced(6))

	sibling, err := s.Substitute(t.Context(), failed)
	require.NoError(t, err)
	require.Equal(t, failed.Number.Uint64(), sibling.Number.Uint64())
	require.NotEqual(t, failed.Hash(), sibling.Hash())
	require.Equal(t, sibling.Hash(), n.hash(6))
	require.Equal(t, n.signed(6), n.announced(6), "the sibling is announced")
	env, err := n.store.Get(t.Context(), 6, failed.Hash())
	require.NoError(t, err)
	require.Nil(t, env, "the failed block's announcement is replaced")
}

// TestRecordBackfill: the unsettled blocks a previous run left unannounced
// are announced with the next block, the settled ones are not, and a block
// whose announcement failed, with the head or later, is announced with the
// next.
func TestRecordBackfill(t *testing.T) {
	n := newNode(t)
	unsigned := n.sequencer(sequencer.Config{}, n.follower)
	for range 3 {
		n.tick(unsigned)
	}
	n.settle(2)

	signer := n.signer()
	signer.SetKeeper(&faulty{Store: n.store, puts: map[uint64]bool{3: true, 6: true}})
	s := n.sequencer(sequencer.Config{Announcements: signer}, n.follower)
	n.tick(s)
	require.Nil(t, n.announced(3), "its put failed")
	require.Equal(t, n.signed(4), n.announced(4))
	n.tick(s)
	for i := uint64(1); i <= 5; i++ {
		if i <= 2 {
			require.Nil(t, n.announced(i), "settled block %d", i)
			continue
		}
		require.Equal(t, n.signed(i), n.announced(i), "block %d", i)
	}

	before := failures()
	n.tick(s)
	require.Nil(t, n.announced(6))
	require.Equal(t, before+1, failures())
	n.tick(s)
	require.Equal(t, n.signed(6), n.announced(6), "block 6 with block 7")
	require.Equal(t, n.signed(7), n.announced(7))
}

// TestRecordKeepsKept: an announcement kept already, another producer's taken
// by a follower on the same datadir, is not signed over.
func TestRecordKeepsKept(t *testing.T) {
	n := newNode(t)
	unsigned := n.sequencer(sequencer.Config{}, n.follower)
	for range 2 {
		n.tick(unsigned)
	}
	other, err := crypto.HexToECDSA("0202020202020202020202020202020202020202020202020202020202020202")
	require.NoError(t, err)
	kept, err := announce.Sign(other, n.chainID, n.hash(1))
	require.NoError(t, err)
	require.NotEqual(t, n.signed(1), kept)
	require.NoError(t, n.store.Put(t.Context(), 1, n.hash(1), kept))

	n.tick(n.sequencer(sequencer.Config{Announcements: n.signer()}, n.follower))
	require.Equal(t, kept, n.announced(1))
	require.Equal(t, n.signed(2), n.announced(2))
	require.Equal(t, n.signed(3), n.announced(3))
}

// TestRecordPruneFailed: a failed prune is not a failed announcement; the
// block's announcement is served and no failure is counted.
func TestRecordPruneFailed(t *testing.T) {
	n := newNode(t)
	signer := n.signer()
	signer.SetKeeper(&faulty{Store: n.store, prune: errors.New("disk full")})
	before := failures()
	n.tick(n.sequencer(sequencer.Config{Announcements: signer}, n.follower))
	require.Equal(t, n.signed(1), n.announced(1))
	require.Equal(t, before, failures())
}

var errCursor = errors.New("cursor failed")

// cursorFails is a database whose write transactions open cursors that fail
// to position: on First, or on Next when next is set.
type cursorFails struct {
	kv.RwDB
	next bool
}

func (d cursorFails) Update(ctx context.Context, f func(kv.RwTx) error) error {
	return d.RwDB.Update(ctx, func(tx kv.RwTx) error { return f(cursorFailsTx{tx, d.next}) })
}

type cursorFailsTx struct {
	kv.RwTx
	next bool
}

func (tx cursorFailsTx) RwCursor(table string) (kv.RwCursor, error) {
	c, err := tx.RwTx.RwCursor(table)
	return failingCursor{c, tx.next}, err
}

type failingCursor struct {
	kv.RwCursor
	next bool
}

func (c failingCursor) First() ([]byte, []byte, error) {
	if c.next {
		return c.RwCursor.First()
	}
	return nil, nil, errCursor
}

func (failingCursor) Next() ([]byte, []byte, error) { return nil, nil, errCursor }

// TestPruneCursorFailed: a cursor that fails to position fails the prune.
func TestPruneCursorFailed(t *testing.T) {
	for _, next := range []bool{false, true} {
		st, err := announce.Open(t.Context(), t.TempDir(), log.New())
		require.NoError(t, err)
		t.Cleanup(st.Close)
		require.NoError(t, st.Put(t.Context(), 1, common.Hash{1}, []byte{1}))
		st.WrapDB(func(db kv.RwDB) kv.RwDB { return cursorFails{db, next} })
		require.ErrorIs(t, st.Prune(t.Context(), 2), errCursor, "next %t", next)
	}
}
