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

package composer

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/eez/deriver"
	"github.com/erigontech/erigon/eez/protocol"
	"github.com/erigontech/erigon/eez/protocol/da"
	core "github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/types"
)

// ErrMultiCall refuses an outbound transaction making other than one call
// to L1: an entry settles one.
var ErrMultiCall = errors.New("an outbound transaction makes one L1 call")

// composition is a Sync block being composed: its draft, the L1 state its
// held transactions run on, the posting's gas, and the held transactions
// popped for it.
type composition struct {
	c     *Composer
	ctx   context.Context
	fx    *effects
	src   *Source
	draft *Draft
	// gas is the budget of the posting settling the window; alone, of one
	// settling the Sync block alone, the lightest.
	gas, alone *budget
	// escrow is the rollup's ether outbound calls not yet composed can
	// carry; nil until read.
	escrow *uint256.Int
	// rest are the popped transactions not yet tried; deferred, those that
	// wait for the next slot.
	rest, deferred []*Held
	// popped counts the transactions popped; unroomed, those deferred after
	// a simulation: they take from the drain as the others do.
	popped, unroomed int
}

// errSimulated ends an outbound transaction's simulation, undoing its
// writes.
var errSimulated = errors.New("simulated")

// above reports whether h is above root in root's nonce chain.
func above(h *Held) func(root *Held) bool {
	return func(root *Held) bool { return root.chainKey() == h.chainKey() && h.Nonce > root.Nonce }
}

// pop takes up to n held transactions from the pool, outbound first: a Sync
// block's outbound entries precede its inbound ones. It reports whether any
// came.
func (k *composition) pop(n int) bool {
	k.rest = k.c.x.Pool.PopN(n)
	slices.SortStableFunc(k.rest, func(a, b *Held) int { return cmp.Compare(outboundFirst(a), outboundFirst(b)) })
	k.popped += len(k.rest)
	return len(k.rest) > 0
}

func outboundFirst(h *Held) int {
	if h.Direction == Outbound {
		return 0
	}
	return 1
}

// drain composes the popped transactions, popping more while the drain has
// room.
func (k *composition) drain() error {
	for {
		if len(k.rest) == 0 {
			// Those waiting on the chain or on the one ahead leave the drain
			// room for others.
			want := k.c.x.MaxTxs - (k.popped - len(k.deferred) + k.unroomed)
			if want <= 0 || !k.pop(want) {
				return nil
			}
		}
		if err := k.ctx.Err(); err != nil {
			return err
		}
		if err := k.add(k.rest[0]); err != nil {
			return err
		}
		k.rest = k.rest[1:]
	}
}

// add composes h, or settles why it cannot: it leaves the pool, waits, or
// is evicted with the rest of its nonce chain. An error stops the
// composition with h untried.
func (k *composition) add(h *Held) error {
	c := k.c
	switch {
	case slices.ContainsFunc(k.deferred, above(h)):
		// It waits on the one of its chain ahead.
		k.deferred = append(k.deferred, h)
		return nil
	case h.Direction == Outbound && k.src.posting == postingClosed:
		// A trigger ran: the posting, which its call would join, precedes it.
		k.deferred = append(k.deferred, h)
		return nil
	}
	nonce := k.src.Nonce
	if h.Direction == Outbound {
		nonce = k.draft.Nonce
	}
	onChain, err := nonce(h.Sender)
	if err != nil {
		return err
	}
	switch {
	case h.Nonce < onChain:
		c.logger.Info("[eez] held transaction dropped: its nonce is spent", "event", "eez.composer.stale",
			"hash", h.Hash, "sender", h.Sender, "direction", h.Direction, "nonce", h.Nonce, "onChain", onChain)
		c.x.Pool.Release([]*Held{h})
		return nil
	case h.Nonce > onChain:
		// Admitted on a pending nonce: a predecessor sent elsewhere has yet
		// to land.
		if h.Deferred++; h.Deferred >= maxDeferrals {
			k.evict(h, Poison(fmt.Errorf("nonce %d still ahead of the sender's %d", h.Nonce, onChain)))
			return nil
		}
		if h.Deferred == 1 {
			c.logger.Info("[eez] held transaction waits: its nonce is ahead of the sender's", "event", "eez.composer.nonce_gap",
				"hash", h.Hash, "sender", h.Sender, "direction", h.Direction, "nonce", h.Nonce, "onChain", onChain)
		}
		k.deferred = append(k.deferred, h)
		return nil
	}
	tx, err := types.DecodeTransaction(h.Raw)
	switch {
	case err != nil:
		err = Poison(err)
	case h.Direction == Outbound:
		err = k.outbound(h, tx)
	default:
		err = k.inbound(h, tx)
	}
	if errors.Is(err, ErrFull) && len(k.fx.held) == 0 {
		err = Poison(fmt.Errorf("alone: %w", err))
	}
	switch {
	case err == nil:
		h.Deferred = 0
		k.fx.held = append(k.fx.held, h)
		k.fx.txs = k.draft.Txs()
		return nil
	case errors.Is(err, errNoRoom):
		k.deferred = append(k.deferred, h)
		k.unroomed++
		return nil
	case IsPoison(err):
		k.evict(h, err)
		return nil
	case errors.Is(err, ErrFunding):
		if len(c.charge(c.x.Pool, []*Held{h})) == 0 {
			k.drop(h)
			return nil
		}
		c.logger.Info("[eez] held transaction waits: the manager cannot pay its value", "event", "eez.composer.funding",
			"hash", h.Hash, "sender", h.Sender, "nonce", h.Nonce, "attempts", h.Attempts, "err", err)
		k.deferred = append(k.deferred, h)
		k.unroomed++
		return nil
	}
	return err
}

// room returns nil when the posting and its bundle take cost, h's;
// errOverBudget when only a composition with less does; errNoRoom, up to
// maxDeferrals, when only a lighter window leaves room; and poison when a
// posting of the Sync block alone cannot take it.
func (k *composition) room(h *Held, cost l1Cost) error {
	switch {
	case k.gas.fits(cost):
		return nil
	case !k.alone.fits(cost):
		return Poison(fmt.Errorf("alone: %w", errOverBudget))
	case len(k.fx.held) > 0:
		return errOverBudget
	}
	if h.Deferred++; h.Deferred >= maxDeferrals {
		return Poison(fmt.Errorf("no window left room in %d compositions: %w", maxDeferrals, errOverBudget))
	}
	return errNoRoom
}

// evict evicts h with the rest of its nonce chain, err why.
func (k *composition) evict(h *Held, err error) {
	k.c.logger.Warn("[eez] held transaction evicted: it cannot compose", "event", "eez.composer.poison",
		"hash", h.Hash, "sender", h.Sender, "direction", h.Direction, "nonce", h.Nonce, "err", err)
	k.c.x.Pool.Evict(h.Sender, h.Direction, h.Nonce)
	k.drop(h)
}

// drop forgets those popped above h, evicted, in its chain: Evict ended
// their reservations, and one the front admits after it is a new chain.
func (k *composition) drop(h *Held) {
	behind := func(t *Held) bool { return above(t)(h) }
	k.rest, k.deferred = slices.DeleteFunc(k.rest, behind), slices.DeleteFunc(k.deferred, behind)
}

// inbound delivers the calls tx, an L1 transaction, makes into the rollup.
func (k *composition) inbound(h *Held, tx types.Transaction) error {
	ds, err := NewDeliveries(k.c.x.Sync, k.draft)
	if err != nil {
		return err
	}
	var cost l1Cost
	if _, err := k.src.Run(k.ctx, tx, ds.Dispatch, func([]*Call) error {
		var err error
		if cost, err = inboundCost(ds.Delivered); err != nil {
			return err
		}
		cost.trigger = tx.GetGasLimit()
		return k.room(h, cost)
	}); err != nil {
		return err
	}
	k.gas.take(cost)
	k.draft = ds.fork
	k.fx.deliveries = append(k.fx.deliveries, ds.Delivered...)
	return nil
}

// outbound composes h, tx, a rollup transaction calling L1: simulated on a
// fork of the draft, its one call runs on L1 as the manager delivers it;
// the call's entry, loaded ahead of tx on the fork, must be what tx
// consumes.
func (k *composition) outbound(h *Held, tx types.Transaction) error {
	x := k.c.x
	var (
		e     *protocol.ExecutionEntry
		value uint256.Int
		cost  l1Cost
		fork  *Draft
	)
	err := k.src.Atomic(func() error {
		var err error
		if fork, err = k.draft.Fork(); err != nil {
			return err
		}
		// The simulation runs without the load, and leaves the fork as it was.
		var calls []*Call
		_, err = fork.simulation(x.Sync.EEZL2, x.Sync.RollupID).Run(k.ctx, tx, func(c *Call) ([]byte, error) {
			if c.RollupID != protocol.MainnetRollupID {
				return nil, Poison(fmt.Errorf("call to rollup %d: only calls to L1 leave the rollup", c.RollupID))
			}
			return k.src.Execute(c)
		}, func(cs []*Call) error {
			calls = cs
			return errSimulated
		})
		if !errors.Is(err, errSimulated) {
			return err
		}
		if len(calls) != 1 {
			return Poison(fmt.Errorf("%w, not %d", ErrMultiCall, len(calls)))
		}
		call := calls[0]
		if e, err = outboundEntry(call, x.Sync.RollupID); err != nil {
			return Poison(err)
		}
		value = call.Value
		if !value.IsZero() {
			escrow, err := k.escrowLeft()
			if err != nil {
				return err
			}
			if escrow.Lt(&value) {
				return fmt.Errorf("%w: escrow %s under %s", ErrFunding, escrow, &value)
			}
		}
		if cost, err = outboundCost(e, h.Raw, call.Gas); err != nil {
			return err
		}
		if err := k.room(h, cost); err != nil {
			return err
		}
		load, err := k.draft.syncTx(x.Sync, []protocol.OutboundSettlement{{Entry: e, UserTx: h.Raw}}, nil)
		if err != nil {
			return err
		}
		var logs []*types.Log
		for _, t := range []types.Transaction{load, tx} {
			r, err := fork.Apply(t, nil)
			switch {
			case errors.Is(err, core.ErrGasLimitReached):
				return ErrFull
			case errors.Is(err, ErrStateRead):
				return err
			case err != nil:
				return Poison(fmt.Errorf("with its L1 call loaded: %w", err))
			case r.Status != types.ReceiptStatusSuccessful:
				return Poison(errors.New("the transaction fails with its L1 call loaded"))
			}
			logs = append(logs, r.Logs...)
		}
		// The simulation ran without the load: the transaction must consume
		// the entry it made.
		if err := deriver.VerifyOutbound([]protocol.ExecutionEntry{*e}, logs, x.Sync.RollupID, x.Sync.EEZL2); err != nil {
			return Poison(err)
		}
		return nil
	})
	if err != nil {
		return err
	}
	k.gas.take(cost)
	if k.escrow != nil {
		k.escrow.Sub(k.escrow, &value)
	}
	k.draft = fork
	k.fx.outbound = append(k.fx.outbound, *e)
	return nil
}

// escrowLeft returns what the rollup's outbound calls can still carry,
// reading the escrow at the anchor once.
func (k *composition) escrowLeft() (*uint256.Int, error) {
	if k.escrow == nil {
		if k.c.x.Escrow == nil {
			return nil, errors.New("no escrow to pay outbound value from")
		}
		e, err := k.c.x.Escrow(k.ctx, k.fx.anchor)
		if err != nil {
			return nil, fmt.Errorf("read the rollup's escrow: %w", err)
		}
		k.escrow = new(uint256.Int).Set(e)
	}
	return k.escrow, nil
}

// outboundEntry is the L1 entry settling c, a successful call from rollup
// to L1, as derivation rebuilds it.
func outboundEntry(c *Call, rollup uint64) (*protocol.ExecutionEntry, error) {
	return protocol.EntryFromAction(&da.Action{
		SourceRollupID: rollup, TargetRollupID: protocol.MainnetRollupID, SourceAddress: c.Source, TargetAddress: c.Target,
		Value: c.Value, Data: c.Data, Success: true, ReturnData: c.Return,
	}, rollup)
}
