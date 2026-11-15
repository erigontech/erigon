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

// Package sequencer produces an EEZ chain ahead of settlement: one block per
// L2 interval from the transaction pool, laid out in Sync slots anchored to
// the L1 heads.
package sequencer

import (
	"errors"
	"fmt"
	"time"
)

// ErrTiming reports a timing that cannot lay out a Sync slot.
var ErrTiming = errors.New("invalid sequencer timing")

// MaxCatchup bounds the blocks one Catchup trigger produces. The range
// settles over several batches, each ending at a Sync position of its grid.
const MaxCatchup = 300

// Timing is the wall-clock layout of a rollup's blocks. Every L1 block spans
// K L2 blocks: Live blocks produced on the L2 cadence, Future blocks produced
// ahead of their timestamps while the proof runs, and the Sync block, which
// the next L1 block settles.
type Timing struct {
	// L1 and L2 are the block times.
	L1, L2 time.Duration
	// Proof is the prover's worst-case budget.
	Proof time.Duration
	// Slack is how early the proven batch must reach L1 before the next L1
	// block.
	Slack time.Duration
}

// Validate reports a timing without a layout: L2 not whole seconds, K not an
// integer of at least 2, or no room for the proof and the slack before the
// next L1 block and within the K-1 blocks before the Sync block.
func (t Timing) Validate() error {
	invalid := func(format string, args ...any) error {
		return fmt.Errorf("%w: %s", ErrTiming, fmt.Sprintf(format, args...))
	}
	switch {
	case t.L1 <= 0 || t.L2 <= 0 || t.Proof <= 0:
		return invalid("L1 block time %s, L2 block time %s and proof time %s must be positive", t.L1, t.L2, t.Proof)
	case t.Slack < 0:
		return invalid("negative submission slack %s", t.Slack)
	case t.L2%time.Second != 0:
		// Block heights are whole-second timestamps divided by L2.
		return invalid("L2 block time %s is not a whole number of seconds", t.L2)
	case t.L1%t.L2 != 0:
		return invalid("L1 block time %s is not a multiple of L2 block time %s", t.L1, t.L2)
	case t.K() < 2:
		return invalid("K = %d; the Sync block needs Live blocks before it", t.K())
	case t.Proof+t.Slack >= t.L1:
		return invalid("proof time %s plus slack %s must be below L1 block time %s", t.Proof, t.Slack, t.L1)
	case t.Proof+t.Slack > time.Duration(t.K()-1)*t.L2:
		return invalid("proof time %s plus slack %s exceed the %d L2 blocks before the Sync block", t.Proof, t.Slack, t.K()-1)
	}
	return nil
}

// K is the number of L2 blocks per L1 block.
func (t Timing) K() uint64 { return uint64(t.L1 / t.L2) }

// Future is the number of blocks per slot produced ahead of their timestamps:
// ceil((Proof+Slack)/L2) - 1, so the proven batch reaches L1 Slack before
// the next L1 block.
func (t Timing) Future() uint64 {
	blocks := uint64((t.Proof + t.Slack + t.L2 - 1) / t.L2)
	return blocks - min(blocks, 1)
}

// Live is the number of blocks per slot produced on the L2 cadence.
func (t Timing) Live() uint64 { return t.K() - t.Future() - 1 }

// ProofWindowOpen is the delay after an L1 block's timestamp at which the
// slot's trigger fires: L1 - (Future+1)·L2, the end of the Live blocks.
func (t Timing) ProofWindowOpen() time.Duration {
	return t.L1 - time.Duration(t.Future()+1)*t.L2
}

// SubmissionDeadline is the delay after an L1 block's timestamp by which the
// slot's batch must reach L1.
func (t Timing) SubmissionDeadline() time.Duration { return t.L1 - t.Slack }

// Shape is the kind of a trigger's Composition.
type Shape uint8

const (
	// Idle produces nothing: the head is at or past the Sync height, or no
	// height on its grid is in reach.
	Idle Shape = iota
	// Slot produces Live, then Future blocks, then the Sync block.
	Slot
	// Catchup produces Live blocks, then a Sync block on the grid of the
	// target Sync height.
	Catchup
)

func (s Shape) String() string {
	switch s {
	case Slot:
		return "slot"
	case Catchup:
		return "catchup"
	}
	return "idle"
}

// Composition is the blocks one Sync-slot trigger produces.
type Composition struct {
	Shape        Shape
	Live, Future uint64
}

// Blocks is the number of blocks c produces, its Sync block included.
func (c Composition) Blocks() uint64 {
	if c.Shape == Idle {
		return 0
	}
	return c.Live + c.Future + 1
}

// Compose returns what a trigger produces on head toward the Sync block at
// height sync: the rest of the slot when one slot closes the gap, else a
// Catchup of at most maxCatchup blocks whose Sync block keeps sync's grid,
// stepping back whole slots from it.
func (t Timing) Compose(head, sync, maxCatchup uint64) Composition {
	if head >= sync {
		return Composition{}
	}
	future, k := t.Future(), t.K()
	liveEnd := sync - min(sync, future+1)
	if head >= liveEnd {
		// A previous trigger overshot into the Future blocks.
		return Composition{Shape: Slot, Future: future - min(future, head-liveEnd)}
	}
	if live := liveEnd - head; live <= k-min(k, future+1) {
		return Composition{Shape: Slot, Live: live, Future: future}
	}
	gap := sync - head
	snap := (gap - min(gap, maxCatchup) + k - 1) / k * k
	if snap >= sync || sync-snap <= head {
		return Composition{}
	}
	return Composition{Shape: Catchup, Live: sync - snap - head - 1}
}

// ChunkBoundary returns the Sync block of a settlement chunk bounded by cap:
// the highest block at most cursor+cap on sync's grid, if one lies strictly
// between cursor and sync.
func (t Timing) ChunkBoundary(cursor, sync, cap uint64) (uint64, bool) {
	k := t.K()
	over := sync - min(sync, cursor+cap)
	back := (over + k - 1) / k * k
	if back > sync {
		return 0, false
	}
	boundary := sync - back
	return boundary, boundary > cursor && boundary < sync
}
