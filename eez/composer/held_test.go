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
	"math"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
)

func addr(b byte) common.Address { return common.BytesToAddress([]byte{b, b, b, b}) }

// held is transaction h of sender's chain in direction at nonce, paying h.
func held(sender common.Address, nonce uint64, direction Direction, h byte) *Held {
	tx := &Held{Raw: []byte{h, h, h, h}, Hash: common.Hash{h}, Sender: sender, Nonce: nonce, Direction: direction}
	tx.MaxFee.SetUint64(uint64(h))
	tx.Tip.SetUint64(uint64(h))
	return tx
}

func hashes(txs []*Held) []common.Hash {
	out := make([]common.Hash, len(txs))
	for i, tx := range txs {
		out[i] = tx.Hash
	}
	return out
}

func push(t *testing.T, p *HeldPool, tx *Held, onChain uint64) {
	t.Helper()
	require.NoError(t, p.Push(tx, onChain))
}

func TestHeldPoolFIFO(t *testing.T) {
	p := NewHeldPool()
	for i := byte(1); i <= 3; i++ {
		push(t, p, held(addr(i), uint64(i), Inbound, i), uint64(i))
	}
	require.Equal(t, []common.Hash{{1}, {2}, {3}}, hashes(p.PopN(math.MaxInt)))
	require.Zero(t, p.Len())
}

func TestHeldPoolDedupes(t *testing.T) {
	p := NewHeldPool()
	tx := held(addr(1), 1, Inbound, 1)
	push(t, p, tx, 1)
	push(t, p, tx, 1)
	require.Equal(t, 1, p.Len())
	drained := p.PopN(math.MaxInt)
	push(t, p, tx, 1)
	require.Zero(t, p.Len(), "in flight")
	other := held(addr(1), 1, Inbound, 9)
	require.Equal(t, &NonceError{Expected: 2, OnChain: 1}, p.Push(other, 1))
	p.Release(drained)
}

func TestHeldPoolReplacement(t *testing.T) {
	p := NewHeldPool()
	tx := held(addr(1), 1, Inbound, 1)
	tx.MaxFee.SetUint64(100)
	tx.Tip.SetUint64(10)
	push(t, p, tx, 1)
	replacement := held(addr(1), 1, Inbound, 9)
	replacement.MaxFee.SetUint64(110)
	replacement.Tip.SetUint64(10)
	require.Equal(t, &UnderpricedError{MaxFee: *uint256.NewInt(110), RequiredMaxFee: *uint256.NewInt(110), Tip: *uint256.NewInt(10), RequiredTip: *uint256.NewInt(11)},
		p.Push(replacement, 1))
	replacement.MaxFee.SetUint64(109)
	replacement.Tip.SetUint64(11)
	require.Error(t, p.Push(replacement, 1))
	replacement.MaxFee.SetUint64(110)
	push(t, p, replacement, 1)
	require.Equal(t, []common.Hash{{9}}, hashes(p.PopN(math.MaxInt)))
	t.Run("a zero fee rises by one", func(t *testing.T) {
		p := NewHeldPool()
		push(t, p, held(addr(2), 0, Inbound, 0), 0)
		bump := held(addr(2), 0, Inbound, 7)
		bump.MaxFee.Clear()
		bump.Tip.Clear()
		require.Error(t, p.Push(bump, 0))
		bump.MaxFee.SetUint64(1)
		bump.Tip.SetUint64(1)
		push(t, p, bump, 0)
		require.Equal(t, []common.Hash{{7}}, hashes(p.PopN(math.MaxInt)))
	})
}

func TestHeldPoolDirections(t *testing.T) {
	p := NewHeldPool()
	sender := addr(7)
	push(t, p, held(sender, 0, Inbound, 1), 0)
	push(t, p, held(sender, 1, Inbound, 2), 0)
	push(t, p, held(sender, 0, Outbound, 3), 0)
	push(t, p, held(sender, 1, Outbound, 4), 0)
	evicted := p.Evict(sender, Outbound, 1)
	require.Equal(t, []common.Hash{{4}}, hashes(evicted))
	require.Equal(t, []common.Hash{{1}, {2}, {3}}, hashes(p.PopN(math.MaxInt)))
}

// TestHeldPoolReplacementKeepsItsPlace: a replacement takes its predecessor's
// place in arrival order.
func TestHeldPoolReplacementKeepsItsPlace(t *testing.T) {
	p := NewHeldPool()
	sender, other := addr(9), addr(8)
	push(t, p, held(sender, 0, Inbound, 1), 0)
	push(t, p, held(other, 0, Inbound, 10), 0)
	push(t, p, held(sender, 1, Inbound, 2), 0)
	push(t, p, held(sender, 0, Outbound, 20), 0)
	push(t, p, held(sender, 2, Inbound, 3), 0)
	push(t, p, held(other, 1, Inbound, 11), 0)
	push(t, p, held(sender, 1, Outbound, 21), 0)
	push(t, p, held(sender, 1, Inbound, 9), 0)
	require.Equal(t, []common.Hash{{1}, {10}, {9}, {20}, {3}, {11}, {21}}, hashes(p.PopN(math.MaxInt)))
}

// TestHeldPoolInFlight: a nonce in flight cannot be replaced; one queued
// above it can.
func TestHeldPoolInFlight(t *testing.T) {
	p := NewHeldPool()
	sender := addr(9)
	for n := range uint64(3) {
		push(t, p, held(sender, n, Inbound, byte(n+1)), 0)
	}
	require.Equal(t, []common.Hash{{1}}, hashes(p.PopN(1)))
	push(t, p, held(sender, 1, Inbound, 9), 0)
	require.Equal(t, &NonceError{Expected: 3, OnChain: 0}, p.Push(held(sender, 0, Inbound, 8), 0))
	push(t, p, held(sender, 2, Inbound, 7), 0)
	require.Equal(t, []common.Hash{{9}, {7}}, hashes(p.PopN(math.MaxInt)))
}

// TestHeldPoolLandedReservation: a reservation L1 included but derivation has
// yet to see still extends its chain.
func TestHeldPoolLandedReservation(t *testing.T) {
	p := NewHeldPool()
	sender := addr(0xa)
	push(t, p, held(sender, 0, Inbound, 1), 0)
	drained := p.PopN(1)
	require.Zero(t, p.Len())
	push(t, p, held(sender, 1, Inbound, 2), 0)
	push(t, p, held(sender, 2, Inbound, 3), 1)
	require.Equal(t, &NonceError{Expected: 3, OnChain: 1}, p.Push(held(sender, 4, Inbound, 4), 1))
	p.Release(drained)
}

// TestHeldPoolSpentNonce: a nonce the chain has used cannot be replaced.
func TestHeldPoolSpentNonce(t *testing.T) {
	p := NewHeldPool()
	sender := addr(0xc)
	push(t, p, held(sender, 0, Inbound, 1), 0)
	require.Equal(t, &NonceError{Expected: 1, OnChain: 1}, p.Push(held(sender, 0, Inbound, 9), 1))
	require.Equal(t, []common.Hash{{1}}, hashes(p.PopN(math.MaxInt)))
}

func TestHeldPoolLastNonce(t *testing.T) {
	p := NewHeldPool()
	sender := addr(0xd)
	push(t, p, held(sender, math.MaxUint64, Inbound, 1), math.MaxUint64)
	push(t, p, held(sender, math.MaxUint64, Inbound, 2), math.MaxUint64)
	require.Equal(t, []common.Hash{{2}}, hashes(p.PopN(math.MaxInt)))
	require.Equal(t, &NonceError{Expected: math.MaxUint64, OnChain: math.MaxUint64}, p.Push(held(sender, math.MaxUint64, Inbound, 3), math.MaxUint64))
}

func TestHeldPoolEvictsQueuedAndInFlight(t *testing.T) {
	p := NewHeldPool()
	sender := addr(0xe)
	for n := range uint64(4) {
		push(t, p, held(sender, n, Inbound, byte(n+1)), 0)
	}
	require.Equal(t, []common.Hash{{1}, {2}, {3}}, hashes(p.PopN(3)))
	require.Equal(t, []common.Hash{{4}}, hashes(p.Evict(sender, Inbound, 1)))
	// The reservations from nonce 1 on went with the eviction.
	push(t, p, held(sender, 1, Inbound, 5), 0)
}

func TestHeldPoolRecovery(t *testing.T) {
	t.Run("a newer replacement wins over the recovered chain", func(t *testing.T) {
		p := NewHeldPool()
		sender := addr(0xf)
		push(t, p, held(sender, 0, Inbound, 1), 0)
		push(t, p, held(sender, 1, Inbound, 2), 0)
		old := p.PopN(2)
		p.Release(old)
		push(t, p, held(sender, 0, Inbound, 9), 0)
		p.PushFront(old)
		require.Equal(t, 1, p.Len())
		require.Empty(t, p.inFlight)
		require.Equal(t, []common.Hash{{9}}, hashes(p.PopN(math.MaxInt)))
	})
	t.Run("recovery is idempotent", func(t *testing.T) {
		p := NewHeldPool()
		push(t, p, held(addr(1), 1, Inbound, 1), 1)
		drained := p.PopN(1)
		p.PushFront(drained)
		p.PushFront(drained)
		require.Equal(t, 1, p.Len())
		require.Empty(t, p.inFlight)
	})
	t.Run("recovered transactions go first, in order", func(t *testing.T) {
		p := NewHeldPool()
		for i := byte(1); i <= 3; i++ {
			push(t, p, held(addr(i), 0, Inbound, i), 0)
		}
		drained := p.PopN(2)
		push(t, p, held(addr(4), 0, Inbound, 4), 0)
		p.PushFront(drained)
		require.Equal(t, []common.Hash{{1}, {2}, {3}, {4}}, hashes(p.PopN(math.MaxInt)))
	})
}

// TestHeldPoolReserve: the transactions of a settlement an L1 reorg rolled
// out take their reservations back, so their nonces stay theirs; a
// replacement queued since they were released wins, as in recovery.
func TestHeldPoolReserve(t *testing.T) {
	t.Run("reserved again", func(t *testing.T) {
		p := NewHeldPool()
		sender := addr(0xa)
		push(t, p, held(sender, 0, Inbound, 1), 0)
		push(t, p, held(sender, 1, Inbound, 2), 0)
		settled := p.PopN(2)
		p.Release(settled)
		p.Reserve(settled)
		require.Equal(t, &NonceError{Expected: 2, OnChain: 0}, p.Push(held(sender, 0, Inbound, 9), 0))
		push(t, p, held(sender, 2, Inbound, 3), 0)
		p.Release(settled)
		require.Empty(t, p.inFlight)
	})
	t.Run("a queued replacement wins", func(t *testing.T) {
		p := NewHeldPool()
		sender, other := addr(0xb), addr(0xc)
		push(t, p, held(sender, 0, Inbound, 1), 0)
		push(t, p, held(sender, 1, Inbound, 2), 0)
		push(t, p, held(other, 0, Inbound, 3), 0)
		settled := p.PopN(3)
		p.Release(settled)
		push(t, p, held(sender, 1, Inbound, 9), 1)
		p.Reserve(settled)
		require.Len(t, p.inFlight, 2)
		require.Contains(t, p.inFlight, common.Hash{1})
		require.Contains(t, p.inFlight, common.Hash{3})
		require.Equal(t, []common.Hash{{9}}, hashes(p.PopN(math.MaxInt)))
	})
	t.Run("a queued one is not reserved", func(t *testing.T) {
		p := NewHeldPool()
		push(t, p, held(addr(0xd), 0, Inbound, 1), 0)
		drained := p.PopN(1)
		p.PushFront(drained)
		p.Reserve(drained)
		require.Empty(t, p.inFlight)
		require.Equal(t, 1, p.Len())
	})
}

func TestHeldPoolPopN(t *testing.T) {
	p := NewHeldPool()
	a, b := addr(0xa), addr(0xb)
	push(t, p, held(a, 0, Inbound, 1), 0)
	push(t, p, held(a, 1, Inbound, 2), 0)
	push(t, p, held(a, 2, Inbound, 3), 0)
	push(t, p, held(b, 0, Inbound, 4), 0)
	require.Equal(t, []common.Hash{{1}, {2}, {3}}, hashes(p.PopN(3)))
	require.Equal(t, []common.Hash{{4}}, hashes(p.PopN(math.MaxInt)))
}

// TestHeldPoolInvariants drives random sequences: no transaction is in two
// states, none is lost or duplicated, and each chain stays in nonce order.
func TestHeldPoolInvariants(t *testing.T) {
	for seed := range uint64(192) {
		r := rand.New(rand.NewPCG(seed, 1))
		p := NewHeldPool()
		next := byte(1)
		var drained, released []*Held
		live := map[common.Hash]bool{}
		for range 1 + r.IntN(159) {
			switch r.IntN(5) {
			case 0:
				sender := addr(byte(1 + r.IntN(4)))
				direction := Direction(r.IntN(2))
				p.mu.Lock()
				nonce, ok := p.nextNonce(chainKey{sender, direction}, 0)
				p.mu.Unlock()
				if !ok {
					nonce = math.MaxUint64
				}
				tx := &Held{Hash: common.Hash{31: next}, Sender: sender, Nonce: nonce, Direction: direction}
				tx.MaxFee.SetUint64(1000)
				tx.Tip.SetUint64(100)
				next++
				if p.Push(tx, 0) == nil {
					live[tx.Hash] = true
				}
			case 1:
				drained = append(drained, p.PopN(r.IntN(8))...)
			case 2:
				p.PushFront(drained)
				drained = nil
			case 3:
				p.Release(drained)
				for _, tx := range drained {
					delete(live, tx.Hash)
				}
				released = append(released, drained...)
				drained = nil
			default:
				p.Reserve(released)
				p.mu.Lock()
				for _, tx := range released {
					if _, ok := p.inFlight[tx.Hash]; ok {
						live[tx.Hash] = true
					}
				}
				p.mu.Unlock()
				released = nil
			}
			p.mu.Lock()
			seen := map[common.Hash]bool{}
			for _, tx := range p.txs {
				require.False(t, seen[tx.Hash], "seed %d", seed)
				seen[tx.Hash] = true
			}
			for h := range p.inFlight {
				require.False(t, seen[h], "seed %d: %x queued and in flight", seed, h)
				seen[h] = true
			}
			require.Equal(t, live, seen, "seed %d", seed)
			chains := map[chainKey][]uint64{}
			for _, tx := range p.txs {
				k := chainKey{tx.Sender, tx.Direction}
				chains[k] = append(chains[k], tx.Nonce)
			}
			for k, nonces := range chains {
				require.True(t, slices.IsSorted(nonces) && !hasDup(nonces), "seed %d: %v %v", seed, k, nonces)
			}
			p.mu.Unlock()
		}
	}
}

func hasDup(s []uint64) bool {
	for i := 1; i < len(s); i++ {
		if s[i] == s[i-1] {
			return true
		}
	}
	return false
}
