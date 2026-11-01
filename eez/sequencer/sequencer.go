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

// DefaultMaxDepth is the default cap on the blocks the head runs ahead of
// the latest settlement.
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

// Committer is the follower seen by the sequencer.
type Committer interface {
	// Head returns the header of the head block.
	Head(ctx context.Context) (*types.Header, error)
	// Produce builds the block of inputs in on the head from txs and makes
	// it the head; follower.ErrStaleParent reports a head that has moved.
	Produce(ctx context.Context, in deriver.HeaderInputs, txs txnprovider.TxnProvider) (*types.BlockWithReceipts, error)
	// Settled returns the block the latest settlement ended at.
	Settled() uint64
}

// Config tunes a Sequencer.
type Config struct {
	Timing Timing
	// FeeRecipient is the beneficiary of produced blocks; zero selects the
	// chain's fee collector.
	FeeRecipient common.Address
	// MaxDepth caps the blocks the head runs ahead of the latest settlement
	// in a steady slot; zero disables the cap.
	MaxDepth uint64
	// DABudget bounds the DA bytes of each block's transactions; zero is
	// DABudget(K).
	DABudget int
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
	return &Sequencer{cfg: cfg, l2: l2, beneficiary: beneficiary, committer: committer, pool: pool, logger: logger}, nil
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
		// The Sync block targets a height, not a count: a head moved by
		// derivation would land a counted one off the grid.
		terminal := head.Number.Uint64() + c.Live + 1
		if head, err = s.produceN(ctx, Live, head, c.Live, false); err != nil {
			return err
		}
		if head.Number.Uint64()+1 != terminal {
			s.offGridSkip("head moved during the catch-up", "head", head.Number.Uint64(), "terminal", terminal)
			return nil
		}
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
	default:
		return nil
	}
	if _, err := s.produce(ctx, Sync, head); err != nil {
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
	in := deriver.HeaderInputs{
		Number:     parent.Number.Uint64() + 1,
		ParentHash: parent.Hash(),
		Time:       parent.Time + s.l2.BlockInterval,
		Coinbase:   s.beneficiary,
		Extra:      []byte{},
		GasLimit:   s.l2.BlockGasLimit,
	}
	built, err := s.committer.Produce(ctx, in, newPoolTxns(s.pool, s.cfg.DABudget, s.logger))
	if err != nil {
		return nil, err
	}
	h := built.Block.Header()
	s.logger.Info("[eez] produced block", "event", "eez.sequencer.block.produced", "kind", kind, "number", h.Number.Uint64(),
		"hash", h.Hash(), "time", h.Time, "txs", len(built.Block.Transactions()))
	s.statusMu.Lock()
	s.status.Produced[kind]++
	s.status.Last = h
	s.statusMu.Unlock()
	m := loadMetrics()
	m.produced[kind].Inc()
	m.head.SetUint64(h.Number.Uint64())
	return h, nil
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
