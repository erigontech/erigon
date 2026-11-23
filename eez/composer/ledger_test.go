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
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
)

func posting(b byte) common.Hash { return common.Hash{b} }

func begin(l *Ledger[int], height uint64, p byte, txs ...int) {
	l.Begin(Posted[int]{Height: height, Post: posting(p), Txs: txs})
}

func blocking(t *testing.T, l *Ledger[int], cursor uint64) uint64 {
	t.Helper()
	h, ok := l.Blocking(cursor)
	if !ok {
		return 0
	}
	require.NotZero(t, h)
	return h
}

func TestLedger(t *testing.T) {
	t.Run("a settlement whose posting left L1 is pending again", func(t *testing.T) {
		l := NewLedger[int]()
		begin(l, 10, 0xa)
		repend := func(p byte) bool { _, ok := l.Repend(10, posting(p)); return ok }
		require.False(t, repend(0xa), "pending")
		l.MarkSettled(10, posting(0xa))
		require.False(t, repend(0xb), "another posting")
		require.True(t, repend(0xa))
		require.Equal(t, uint64(10), blocking(t, l, 5))
		require.Empty(t, l.SettledAbove(5))
		// Confirmed, then rolled out by an L1 reorg.
		l.MarkSettled(10, posting(0xa))
		l.ResolveBelowCursor(10)
		require.Len(t, l.RolledOut(5), 1)
		require.True(t, repend(0xa))
		require.Empty(t, l.RolledOut(5))
		require.Equal(t, uint64(10), blocking(t, l, 5))
		l.MarkFailed(10, posting(0xa), false)
		require.False(t, repend(0xa), "failed")
	})
	t.Run("only the latest watch reports", func(t *testing.T) {
		l := NewLedger[int]()
		begin(l, 10, 0xa)
		l.MarkSettled(10, posting(0xa))
		watch, ok := l.Repend(10, posting(0xa))
		require.True(t, ok)
		require.Equal(t, uint64(1), watch)
		l.Report(10, posting(0xa), 0, Failed, false)
		require.Equal(t, uint64(10), blocking(t, l, 5))
		_, ok = l.TakeFailed(5)
		require.False(t, ok, "the first watch's verdict is stale")
		l.Report(10, posting(0xa), watch, Failed, false)
		_, ok = l.TakeFailed(5)
		require.True(t, ok)
	})
	t.Run("finality hands out only landed postings as landed", func(t *testing.T) {
		landed := func(f func(l *Ledger[int])) bool {
			l := NewLedger[int]()
			begin(l, 10, 0xa)
			f(l)
			l.ResolveBelowCursor(10)
			if p, ok := l.PartialToDispose(10); ok {
				require.True(t, l.ClaimPartial(p.Height, p.Post, nil))
			}
			out := l.TakeFinalized(10)
			require.Len(t, out, 1)
			return out[0].Landed
		}
		require.True(t, landed(func(l *Ledger[int]) { l.MarkSettled(10, posting(0xa)) }))
		require.True(t, landed(func(l *Ledger[int]) { l.MarkSettledPartial(10, posting(0xa)) }))
		require.True(t, landed(func(l *Ledger[int]) {
			l.ResolveBelowCursor(10)
			l.MarkSettledPartial(10, posting(0xa))
		}), "a verdict after the cursor's")
		require.False(t, landed(func(*Ledger[int]) {}), "the cursor passed a pending posting")
		require.False(t, landed(func(l *Ledger[int]) { l.MarkFailed(10, posting(0xa), false) }))
		require.False(t, landed(func(l *Ledger[int]) {
			l.MarkSettled(10, posting(0xa))
			require.True(t, l.Unsettle(10, posting(0xa)))
		}), "L1 dropped it")
	})
	t.Run("the gate waits for the cursor even when settled", func(t *testing.T) {
		l := NewLedger[int]()
		begin(l, 10, 0xa, 1)
		require.Equal(t, uint64(10), blocking(t, l, 5))
		l.MarkSettled(10, posting(0xa))
		require.Equal(t, uint64(10), blocking(t, l, 5))
		require.Zero(t, blocking(t, l, 10))
	})
	t.Run("the cursor does not release a partial settlement", func(t *testing.T) {
		l := NewLedger[int]()
		begin(l, 10, 0xa, 1, 2)
		l.MarkSettledPartial(10, posting(0xa))
		require.Empty(t, l.ResolveBelowCursor(10))
		swept, ok := l.PartialToDispose(10)
		require.True(t, ok)
		require.Equal(t, uint64(10), swept.Height)
		require.Equal(t, []int{1, 2}, swept.Txs)
		require.False(t, swept.SlotSkipped)
		_, ok = l.PartialToDispose(10)
		require.True(t, ok, "a failed disposal retries")
		require.True(t, l.ClaimPartial(10, posting(0xa), nil))
		_, ok = l.PartialToDispose(10)
		require.False(t, ok, "disposed once")
	})
	t.Run("a partial settlement is not a failure", func(t *testing.T) {
		l := NewLedger[int]()
		begin(l, 10, 0xa, 1)
		l.MarkSettledPartial(10, posting(0xa))
		_, ok := l.TakeFailed(0)
		require.False(t, ok)
	})
	t.Run("a partial verdict does not override a settlement", func(t *testing.T) {
		l := NewLedger[int]()
		begin(l, 10, 0xa, 1)
		l.MarkSettled(10, posting(0xa))
		l.MarkSettledPartial(10, posting(0xa))
		_, ok := l.PartialToDispose(10)
		require.False(t, ok)
	})
	t.Run("an undisposed partial settlement rolls out", func(t *testing.T) {
		l := NewLedger[int]()
		begin(l, 10, 0xa, 1, 2)
		l.MarkSettledPartial(10, posting(0xa))
		require.Equal(t, []int{1, 2}, l.TakeRolledOut(9))
		require.Zero(t, blocking(t, l, 9))
		_, ok := l.PartialToDispose(10)
		require.False(t, ok)
	})
	t.Run("a verdict on a replaced batch is ignored", func(t *testing.T) {
		l := NewLedger[int]()
		begin(l, 10, 0xa, 1)
		begin(l, 10, 0xb, 2)
		l.MarkFailed(10, posting(0xa), false)
		l.MarkSettled(10, posting(0xa))
		l.MarkSettledPartial(10, posting(0xa))
		_, ok := l.TakeFailed(0)
		require.False(t, ok)
		_, ok = l.PartialToDispose(10)
		require.False(t, ok)
		l.MarkFailed(10, posting(0xb), false)
		failed, ok := l.TakeFailed(0)
		require.True(t, ok)
		require.Equal(t, posting(0xb), failed.Post)
		require.False(t, l.ClaimPartial(10, posting(0xa), nil), "taken")
	})
	t.Run("a partial settlement waits for derivation", func(t *testing.T) {
		l := NewLedger[int]()
		begin(l, 10, 0xa, 1)
		l.MarkSettledPartial(10, posting(0xa))
		_, ok := l.PartialToDispose(9)
		require.False(t, ok)
		require.Equal(t, uint64(10), blocking(t, l, 9))
		_, ok = l.PartialToDispose(10)
		require.True(t, ok)
	})
	t.Run("a failure is taken once and reinserted", func(t *testing.T) {
		l := NewLedger[int]()
		begin(l, 10, 0xa, 1, 2)
		l.MarkFailed(10, posting(0xa), false)
		require.Equal(t, uint64(10), blocking(t, l, 0))
		failed, ok := l.TakeFailed(0)
		require.True(t, ok)
		require.Equal(t, uint64(10), failed.Height)
		require.Equal(t, posting(0xa), failed.Post)
		require.Equal(t, []int{1, 2}, failed.Txs)
		_, ok = l.TakeFailed(0)
		require.False(t, ok)
		require.Zero(t, blocking(t, l, 0))
		l.ReinsertFailed(failed)
		require.Equal(t, uint64(10), blocking(t, l, 0))
		again, ok := l.TakeFailed(0)
		require.True(t, ok)
		require.Equal(t, failed, again)
	})
	t.Run("a failure keeps its skipped slot", func(t *testing.T) {
		l := NewLedger[int]()
		begin(l, 10, 0xa, 1, 2)
		l.MarkFailed(10, posting(0xa), true)
		failed, ok := l.TakeFailed(0)
		require.True(t, ok)
		require.True(t, failed.SlotSkipped)
	})
	t.Run("the cursor overrides a failure", func(t *testing.T) {
		l := NewLedger[int]()
		begin(l, 10, 0xa, 1)
		l.MarkFailed(10, posting(0xa), false)
		_, ok := l.TakeFailed(10)
		require.False(t, ok, "derivation passed it")
		require.Empty(t, l.ResolveBelowCursor(10))
		_, ok = l.TakeFailed(0)
		require.False(t, ok)
		_, ok = l.PartialToDispose(10)
		require.True(t, ok)
		require.Zero(t, blocking(t, l, 10))
	})
	t.Run("the cursor releases a settlement once", func(t *testing.T) {
		l := NewLedger[int]()
		begin(l, 10, 0xa, 1, 2)
		l.MarkSettled(10, posting(0xa))
		require.Equal(t, []int{1, 2}, l.ResolveBelowCursor(10))
		require.Empty(t, l.ResolveBelowCursor(10))
	})
	t.Run("a reorg rolls out only settlements", func(t *testing.T) {
		l := NewLedger[int]()
		begin(l, 10, 0xa, 1)
		l.MarkSettled(10, posting(0xa))
		begin(l, 15, 0xb, 2)
		require.Equal(t, []int{1}, l.TakeRolledOut(8))
		require.Equal(t, uint64(15), blocking(t, l, 8))
	})
	t.Run("a settlement L1 dropped is a failure", func(t *testing.T) {
		l := NewLedger[int]()
		begin(l, 10, 0xa, 1)
		begin(l, 14, 0xb, 2)
		require.Empty(t, l.SettledAbove(0), "pending")
		l.MarkSettled(10, posting(0xa))
		require.Equal(t, []uint64{10}, heightsOf(l.SettledAbove(0)))
		require.Empty(t, l.SettledAbove(10))
		require.False(t, l.Unsettle(10, posting(0xb)), "another posting")
		require.True(t, l.Unsettle(10, posting(0xa)))
		failed, ok := l.TakeFailed(0)
		require.True(t, ok)
		require.Equal(t, uint64(10), failed.Height)
		begin(l, 10, 0xc, 3)
		l.MarkSettled(10, posting(0xc))
		l.ResolveBelowCursor(10)
		require.False(t, l.Unsettle(10, posting(0xc)), "derivation confirmed it")
	})
	t.Run("finality hands settlements to the audit", func(t *testing.T) {
		l := NewLedger[int]()
		begin(l, 10, 0xa, 1)
		l.MarkSettled(10, posting(0xa))
		begin(l, 11, 0xc, 3)
		l.MarkSettledPartial(11, posting(0xc))
		begin(l, 15, 0xb, 2)
		final := l.TakeFinalized(12)
		require.Len(t, final, 1)
		require.Equal(t, uint64(10), final[0].Height)
		require.Equal(t, posting(0xa), final[0].Post)
		require.Equal(t, []int{1}, final[0].Txs)
		require.Empty(t, l.TakeFinalized(12))
		_, ok := l.PartialToDispose(12)
		require.True(t, ok, "disposal owns a partial settlement")
		require.Equal(t, []int{3}, l.TakeRolledOut(0))
		require.Equal(t, uint64(15), blocking(t, l, 12))
	})
}

// TestLedgerInvariants drives random sequences through the gate: at most one
// batch above the cursor is unresolved, the gate is the lowest height above
// the cursor, and a transaction is released at most once.
func TestLedgerInvariants(t *testing.T) {
	for seed := range uint64(192) {
		r := rand.New(rand.NewPCG(seed, 0))
		l := NewLedger[int]()
		var cursor uint64
		tag := 1
		released := map[int]bool{}
		release := func(txs []int) {
			for _, tx := range txs {
				require.False(t, released[tx], "seed %d: transaction %d released twice", seed, tx)
				released[tx] = true
			}
		}
		for range 1 + r.IntN(127) {
			gate, gated := l.Blocking(cursor)
			flag := r.IntN(2) == 0
			switch r.IntN(7) {
			case 0:
				if !gated {
					begin(l, cursor+1+uint64(tag%3), byte(tag), tag)
					tag++
				}
			case 1:
				if gated {
					l.MarkSettled(gate, l.entries[gate].Post)
				}
			case 2:
				if gated {
					l.MarkFailed(gate, l.entries[gate].Post, flag)
				}
			case 3:
				if gated {
					cursor = gate
					release(l.ResolveBelowCursor(cursor))
					for {
						p, ok := l.PartialToDispose(cursor)
						if !ok {
							break
						}
						release(p.Txs)
						require.True(t, l.ClaimPartial(p.Height, p.Post, p.Txs))
					}
				}
			case 4:
				if failed, ok := l.TakeFailed(cursor); ok && flag {
					l.ReinsertFailed(failed)
				}
			case 5:
				l.TakeFinalized(cursor)
			default:
				l.TakeRolledOut(cursor)
			}
			unresolved, lowest := 0, uint64(0)
			for h, e := range l.entries {
				if h <= cursor {
					continue
				}
				if e.resolution != Settled {
					unresolved++
				}
				if lowest == 0 || h < lowest {
					lowest = h
				}
			}
			require.LessOrEqual(t, unresolved, 1, "seed %d", seed)
			require.Equal(t, lowest, blocking(t, l, cursor), "seed %d", seed)
		}
	}
}

func heightsOf(ps []Posted[int]) []uint64 {
	out := make([]uint64, len(ps))
	for i, p := range ps {
		out[i] = p.Height
	}
	return out
}
