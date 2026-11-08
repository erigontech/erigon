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

package sequencer

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/eez/config"
	"github.com/erigontech/erigon/eez/deriver"
	"github.com/erigontech/erigon/eez/follower"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/txnprovider"
)

// DefaultMaxDepth is the default MaxDepth.
const DefaultMaxDepth = 64

// offGridStall is the number of consecutive Sync blocks skipped off the grid
// that logs a stall.
const offGridStall = 3

// Kind is the role of a produced block in its slot.
type Kind uint8

const (
	// Live blocks are produced on the L2 cadence.
	Live Kind = iota
	// Future blocks are produced ahead of their timestamps while the proof
	// runs.
	Future
	// Sync is the slot's last block, which the next L1 block settles.
	Sync
)

func (k Kind) String() string {
	switch k {
	case Future:
		return "future"
	case Sync:
		return "sync"
	}
	return "live"
}

// Mode is how a Sync slot is composed.
type Mode uint8

const (
	// ModeSteady is a slot on time: its batch targets the L1 block after the
	// anchor.
	ModeSteady Mode = iota
	// ModeCatchup is a slot of a head behind the schedule: its Sync block
	// carries no cross-chain content.
	ModeCatchup
	// ModeEmpty is a slot too late for its proof to reach the next L1 block:
	// its Sync block is empty and nothing is posted.
	ModeEmpty
)

func (m Mode) String() string {
	switch m {
	case ModeCatchup:
		return "catchup"
	case ModeEmpty:
		return "empty"
	}
	return "steady"
}

// SyncSlotComposer composes the Sync blocks of a sequencer's slots.
type SyncSlotComposer interface {
	// ComposeSyncSlot returns the Sync block on parent with timestamp ts, its
	// batch aimed at L1 block target, zero for the next one available; nil
	// has the sequencer fill the block from the pool.
	ComposeSyncSlot(ctx context.Context, parent *types.Header, ts, target uint64, mode Mode) *types.Block
	// RecoverFailed handles the bundles that failed on L1, replacing each
	// failed Sync block through Sequencer.Substitute. It runs before every
	// trigger reads the head, which it may move.
	RecoverFailed(ctx context.Context)
}

// Committer is the follower seen by the sequencer.
type Committer interface {
	// Head returns the header of the head block.
	Head(ctx context.Context) (*types.Header, error)
	// Produce builds the block of inputs in on the head from txs and makes
	// it the head, running hook when set. follower.ErrStaleParent reports a
	// head that has moved.
	Produce(ctx context.Context, in deriver.HeaderInputs, txs txnprovider.TxnProvider, hook follower.CommitHook) (*types.BlockWithReceipts, error)
	// Insert makes blocks, the first a child of the head, the head, running
	// hook when set. follower.ErrStaleParent reports a head that has moved.
	Insert(ctx context.Context, blocks []*types.Block, hook follower.CommitHook) error
	// HeadBefore moves the head back to the parent of block and returns it;
	// follower.ErrBelowSafe reports a block at or below the safe block,
	// follower.ErrNotCanonical one another block has replaced.
	HeadBefore(ctx context.Context, block *types.Header) (*types.Header, error)
	// Settled returns the block the latest settlement ended at.
	Settled() uint64
}

// Witnesses captures the execution witness of each produced block.
type Witnesses interface {
	// Pin snapshots the head's state for the witness of its child.
	Pin(ctx context.Context) (interface{ Close() }, error)
	// Record builds and keeps the witness of block h, a child of pin's head.
	Record(ctx context.Context, pin interface{ Close() }, h *types.Header) error
}

// Config tunes a Sequencer.
type Config struct {
	Timing Timing
	// FeeRecipient is the beneficiary of produced blocks; zero selects the
	// chain's fee collector.
	FeeRecipient common.Address
	// MaxDepth pauses a steady slot once the head runs that many blocks past
	// the latest settlement; zero disables it. The next trigger is a catch-up,
	// which ignores it: pausing that too would stop a slot whose settlement
	// failed from being produced again. It bounds no depth through a stall.
	MaxDepth uint64
	// DABudget bounds the DA bytes of each block's transactions; zero is
	// DABudget(K).
	DABudget int
	// Witnesses, when set, captures each produced block's witness.
	Witnesses Witnesses
	// Composer, when set, composes the Sync blocks.
	Composer SyncSlotComposer
}

// Sequencer produces an EEZ chain's blocks from the pool on the triggers of
// its schedule.
type Sequencer struct {
	cfg         Config
	l2          *config.L2Config
	beneficiary common.Address
	committer   Committer
	pool        txnprovider.TxnProvider
	logger      log.Logger
	now         func() time.Time
	// offGrid counts consecutive Sync blocks skipped off the grid.
	offGrid int

	statusMu sync.Mutex
	status   Status
}

// Status is a snapshot of a sequencer's production.
type Status struct {
	// Produced counts the blocks produced per Kind.
	Produced [3]uint64
	// Last is the latest produced block.
	Last *types.Header
	// SyncHeight is the Sync block of the latest trigger.
	SyncHeight uint64
	// OffGridSkips counts consecutive Sync blocks skipped off the grid.
	OffGridSkips int
}

// New returns a sequencer of the chain cc, producing on committer from pool.
// Only the rollup0 rule set is supported.
func New(cc *chain.Config, cfg Config, committer Committer, pool txnprovider.TxnProvider, logger log.Logger) (*Sequencer, error) {
	l2, ok := cc.L2.(*config.L2Config)
	if !ok {
		return nil, fmt.Errorf("chain %s is not an EEZ chain", cc.ChainName)
	}
	if l2.RollupX != nil {
		return nil, errors.New("the sequencer produces rollup0 chains only")
	}
	if err := cfg.Timing.Validate(); err != nil {
		return nil, err
	}
	if want := time.Duration(l2.BlockInterval) * time.Second; cfg.Timing.L2 != want {
		return nil, fmt.Errorf("%w: L2 block time %s, the chain's is %s", ErrTiming, cfg.Timing.L2, want)
	}
	if cfg.DABudget == 0 {
		cfg.DABudget = DABudget(cfg.Timing.K())
	}
	beneficiary := cfg.FeeRecipient
	if beneficiary == (common.Address{}) && cc.FeeCollector != nil {
		beneficiary = *cc.FeeCollector
	}
	return &Sequencer{cfg: cfg, l2: l2, beneficiary: beneficiary, committer: committer, pool: pool, logger: logger, now: time.Now}, nil
}

// Status returns the sequencer's production so far. It is safe to call
// concurrently with Run.
func (s *Sequencer) Status() Status {
	s.statusMu.Lock()
	defer s.statusMu.Unlock()
	return s.status
}

// Run handles the events of events until ctx ends or events closes. A
// failed trigger is logged; the next one starts from the head again.
func (s *Sequencer) Run(ctx context.Context, events <-chan Event) {
	for {
		select {
		case <-ctx.Done():
			return
		case e, ok := <-events:
			if !ok {
				s.logger.Error("[eez] sequencer schedule closed, sequencer stops", "event", "eez.sequencer.schedule_rx.closed")
				return
			}
			if err := s.Handle(ctx, e); err != nil && ctx.Err() == nil {
				loadMetrics().triggerFailed.Inc()
				s.logger.Error("[eez] sequencer trigger failed", "event", "eez.sequencer.advance.failed", "err", err)
			}
		}
	}
}

// Handle produces what e asks for.
func (s *Sequencer) Handle(ctx context.Context, e Event) error {
	if c := s.cfg.Composer; c != nil {
		c.RecoverFailed(ctx)
	}
	var err error
	switch e := e.(type) {
	case LiveTick:
		err = s.liveTick(ctx, e)
	case SyncSlot:
		err = s.syncSlot(ctx, e)
	}
	if errors.Is(err, follower.ErrStaleParent) {
		// Derivation moved the head under the trigger; the next one starts
		// from the new head.
		s.logger.Debug("[eez] head moved under the sequencer", "event", "eez.sequencer.stale_parent", "err", err)
		return nil
	}
	return err
}

// liveTick produces one Live block while the head is below the Live blocks'
// end of the slot.
func (s *Sequencer) liveTick(ctx context.Context, tick LiveTick) error {
	head, err := s.committer.Head(ctx)
	if err != nil {
		return err
	}
	future := s.cfg.Timing.Future()
	if liveEnd := tick.SyncHeight - min(tick.SyncHeight, future+1); head.Number.Uint64() >= liveEnd || s.paused(head) {
		return nil
	}
	_, err = s.produce(ctx, Live, head)
	return err
}

// syncSlot produces the rest of the slot: its Live, Future and Sync blocks,
// or a Catchup toward it whose Sync block keeps its grid.
func (s *Sequencer) syncSlot(ctx context.Context, slot SyncSlot) error {
	head, err := s.committer.Head(ctx)
	if err != nil {
		return err
	}
	c := s.cfg.Timing.Compose(head.Number.Uint64(), slot.Height, MaxCatchup)
	s.logger.Info("[eez] Sync slot", "event", "eez.sequencer.sync_slot.composition", "head", head.Number.Uint64(), "height", slot.Height,
		"time", slot.Time, "l1Head", slot.L1Head, "shape", c.Shape, "live", c.Live, "future", c.Future)
	s.statusMu.Lock()
	s.status.SyncHeight = slot.Height
	s.statusMu.Unlock()
	switch c.Shape {
	case Catchup:
		// Each block commits on the one before it, so the Sync block keeps its
		// height; a head moved by derivation fails the next with ErrStaleParent.
		if head, err = s.produceN(ctx, Live, head, c.Live, false); err != nil {
			return err
		}
		return s.syncBlock(ctx, head, head.Time+s.l2.BlockInterval, slot.L1Head+1, ModeCatchup)
	case Slot:
		if head, err = s.produceN(ctx, Live, head, c.Live, true); err != nil || head == nil {
			return err
		}
		if head, err = s.produceN(ctx, Future, head, c.Future, true); err != nil || head == nil {
			return err
		}
		if s.paused(head) {
			return nil
		}
		// A Sync block off the grid would settle a root derivation does not
		// rebuild.
		if next := head.Time + s.l2.BlockInterval; next != slot.Time {
			s.offGridSkip("parent + L2 block time is not the slot's timestamp", "head", head.Number.Uint64(), "expected", next, "time", slot.Time)
			return nil
		}
		mode, target := ModeSteady, slot.L1Head+1
		if s.late(slot.Time) {
			mode, target = ModeEmpty, 0
			s.logger.Warn("[eez] Sync slot too late for the next L1 block", "event", "eez.sequencer.sync_slot.deferred_late",
				"height", slot.Height, "time", slot.Time, "l1Head", slot.L1Head)
		}
		return s.syncBlock(ctx, head, slot.Time, target, mode)
	}
	return nil
}

// Substitute replaces failed, a Sync block whose bundle failed on L1, with an
// empty Sync block on its parent; a composer calls it from RecoverFailed. It
// fails with follower.ErrBelowSafe once L1 settled that height,
// follower.ErrNotCanonical once another block replaced failed, and
// follower.ErrStaleParent when derivation moves the head between the
// rollback and the sibling.
func (s *Sequencer) Substitute(ctx context.Context, failed *types.Header) (*types.Header, error) {
	parent, err := s.committer.HeadBefore(ctx, failed)
	if err != nil {
		return nil, err
	}
	h, err := s.build(ctx, Sync, parent, noTxns{})
	if err != nil {
		return nil, err
	}
	s.logger.Warn("[eez] failed Sync block substituted with its empty sibling", "event", "eez.composer.recovery.substituted",
		"number", h.Number.Uint64(), "hash", h.Hash())
	return h, nil
}

// late reports whether a proof started now reaches L1 after the submission
// deadline of the Sync block at ts.
func (s *Sequencer) late(ts uint64) bool {
	deadline := time.Unix(int64(ts), 0).Add(-s.cfg.Timing.Slack)
	return s.now().Add(s.cfg.Timing.Proof).After(deadline)
}

// syncBlock commits the Sync block on parent: the composer's, else one from
// the pool, empty for a slot too late.
func (s *Sequencer) syncBlock(ctx context.Context, parent *types.Header, ts, target uint64, mode Mode) error {
	var composed *types.Block
	if c := s.cfg.Composer; c != nil {
		composed = c.ComposeSyncSlot(ctx, parent, ts, target, mode)
	}
	var err error
	switch {
	case composed != nil:
		_, err = s.insert(ctx, Sync, composed)
	case mode == ModeEmpty:
		_, err = s.build(ctx, Sync, parent, noTxns{})
	default:
		_, err = s.produce(ctx, Sync, parent)
	}
	if err != nil {
		return err
	}
	s.setOffGrid(0)
	return nil
}

// produceN produces n blocks of kind on head and returns the new head; nil
// when capped stops it.
func (s *Sequencer) produceN(ctx context.Context, kind Kind, head *types.Header, n uint64, capped bool) (*types.Header, error) {
	for range n {
		if capped && s.paused(head) {
			return nil, nil
		}
		var err error
		if head, err = s.produce(ctx, kind, head); err != nil {
			return nil, err
		}
	}
	return head, nil
}

// produce builds the block of kind after parent from the pool.
func (s *Sequencer) produce(ctx context.Context, kind Kind, parent *types.Header) (*types.Header, error) {
	txs := newPoolTxns(s.pool, parent.Hash(), s.cfg.DABudget, s.logger)
	if err := txs.await(ctx, parent.Number.Uint64()); err != nil {
		return nil, err
	}
	return s.build(ctx, kind, parent, txs)
}

// build builds the block of kind after parent from txs.
func (s *Sequencer) build(ctx context.Context, kind Kind, parent *types.Header, txs txnprovider.TxnProvider) (*types.Header, error) {
	in := deriver.HeaderInputs{
		Number:     parent.Number.Uint64() + 1,
		ParentHash: parent.Hash(),
		Time:       parent.Time + s.l2.BlockInterval,
		Coinbase:   s.beneficiary,
		Extra:      []byte{},
		GasLimit:   s.l2.BlockGasLimit,
	}
	return s.commit(ctx, kind, in.Number, func(ctx context.Context, hook follower.CommitHook) (*types.Block, error) {
		built, err := s.committer.Produce(ctx, in, txs, hook)
		if err != nil {
			return nil, err
		}
		return built.Block, nil
	})
}

// insert commits block, a composer's block of kind, on the head.
func (s *Sequencer) insert(ctx context.Context, kind Kind, block *types.Block) (*types.Header, error) {
	return s.commit(ctx, kind, block.NumberU64(), func(ctx context.Context, hook follower.CommitHook) (*types.Block, error) {
		return block, s.committer.Insert(ctx, []*types.Block{block}, hook)
	})
}

// commit runs put, which makes block number the head and runs hook, which
// records the block's witness.
func (s *Sequencer) commit(ctx context.Context, kind Kind, number uint64, put func(ctx context.Context, hook follower.CommitHook) (*types.Block, error)) (*types.Header, error) {
	// The witness belongs to the block, which can commit after ctx ends.
	wctx := context.WithoutCancel(ctx)
	var pin interface{ Close() }
	var pinErr error
	defer func() {
		if pin != nil {
			pin.Close()
		}
	}()
	// Before derivation resumes: a block it replaces has no witness to build.
	record := func(h *types.Header) {
		switch {
		case pinErr != nil:
			s.witnessFailed(number, pinErr)
		case pin != nil:
			if err := s.cfg.Witnesses.Record(wctx, pin, h); err != nil {
				s.witnessFailed(number, err)
			}
		}
	}
	// The pin must hold the parent's state: taken in the command, on the
	// parent before the block commits, so no read transaction waits for it.
	hook := func() func(*types.Header) {
		if w := s.cfg.Witnesses; w != nil {
			pin, pinErr = w.Pin(wctx)
		}
		return record
	}
	b, err := put(ctx, hook)
	if err != nil {
		return nil, err
	}
	h := b.Header()
	s.logger.Info("[eez] produced block", "event", "eez.sequencer.block.produced", "kind", kind, "number", h.Number.Uint64(),
		"hash", h.Hash(), "time", h.Time, "txs", len(b.Transactions()))
	s.statusMu.Lock()
	s.status.Produced[kind]++
	s.status.Last = h
	s.statusMu.Unlock()
	m := loadMetrics()
	m.produced[kind].Inc()
	m.head.SetUint64(h.Number.Uint64())
	return h, nil
}

// witnessFailed logs a block left without a witness, which no attester can
// prove.
func (s *Sequencer) witnessFailed(number uint64, err error) {
	loadMetrics().witnessFailed.Inc()
	s.logger.Error("[eez] produced block without a witness", "event", "eez.sequencer.witness.failed", "number", number, "err", err)
}

// paused reports whether head has reached the cap on blocks ahead of the
// latest settlement.
func (s *Sequencer) paused(head *types.Header) bool {
	if s.cfg.MaxDepth == 0 {
		return false
	}
	settled := s.committer.Settled()
	depth := head.Number.Uint64() - min(head.Number.Uint64(), settled)
	if depth < s.cfg.MaxDepth {
		return false
	}
	s.logger.Debug("[eez] sequencer paused at the speculative cap", "event", "eez.sequencer.speculative.paused", "head", head.Number.Uint64(),
		"settled", settled, "depth", depth, "max", s.cfg.MaxDepth)
	return true
}

// offGridSkip records a Sync block skipped off the grid; a run of them is a
// stall.
func (s *Sequencer) offGridSkip(why string, ctx ...any) {
	s.setOffGrid(s.offGrid + 1)
	loadMetrics().offGridSkips.Inc()
	s.logger.Warn("[eez] Sync block skipped off the grid: "+why, append([]any{"event", "eez.sequencer.sync_slot.off_grid_skipped"}, ctx...)...)
	if s.offGrid >= offGridStall {
		s.logger.Error("[eez] consecutive Sync blocks skipped off the grid, production is not advancing", "event", "eez.sequencer.sync_slot.off_grid_stalled",
			"skips", s.offGrid)
	}
}

func (s *Sequencer) setOffGrid(n int) {
	s.offGrid = n
	s.statusMu.Lock()
	s.status.OffGridSkips = n
	s.statusMu.Unlock()
}
