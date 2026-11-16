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
	"fmt"
	"slices"
	"sync"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
)

// Direction is the way a held transaction crosses.
type Direction uint8

const (
	// Inbound is an L1 transaction calling into the rollup.
	Inbound Direction = iota
	// Outbound is a rollup transaction calling L1.
	Outbound
)

func (d Direction) String() string {
	if d == Outbound {
		return "outbound"
	}
	return "inbound"
}

// Held is a cross-chain transaction waiting for a Sync block.
type Held struct {
	// Raw is the signed transaction, EIP-2718 encoded; Hash its hash.
	Raw  []byte
	Hash common.Hash
	// Attempts counts the bundles that carried it and failed for it.
	Attempts  uint32
	Sender    common.Address
	Nonce     uint64
	Direction Direction
	MaxFee    uint256.Int
	Tip       uint256.Int
}

// NonceError refuses a transaction whose nonce does not extend its sender's
// chain: Expected is the next one, OnChain the sender's on its network.
type NonceError struct{ Expected, OnChain uint64 }

func (e *NonceError) Error() string {
	return fmt.Sprintf("nonce out of order: expected %d, %d on chain", e.Expected, e.OnChain)
}

// UnderpricedError refuses a replacement that does not raise both fees by
// replaceBump percent.
type UnderpricedError struct {
	MaxFee, RequiredMaxFee, Tip, RequiredTip uint256.Int
}

func (e *UnderpricedError) Error() string {
	return fmt.Sprintf("replacement underpriced: max fee %s, %s required; tip %s, %s required", &e.MaxFee, &e.RequiredMaxFee, &e.Tip, &e.RequiredTip)
}

type chainKey struct {
	sender    common.Address
	direction Direction
}

type reservation struct {
	chainKey
	nonce uint64
}

// HeldPool holds cross-chain transactions in arrival order, each sender's
// nonces contiguous per direction. A drained transaction stays reserved,
// in flight, until its batch resolves.
type HeldPool struct {
	mu       sync.Mutex
	txs      []*Held
	byHash   map[common.Hash]bool
	inFlight map[common.Hash]reservation
}

// replaceBump is the percentage a replacement raises both fees by.
const replaceBump = 10

// minReplacement is the least fee replacing one of fee.
func minReplacement(fee *uint256.Int) uint256.Int {
	bump := new(uint256.Int).Div(new(uint256.Int).Mul(fee, uint256.NewInt(replaceBump)), uint256.NewInt(100))
	if bump.IsZero() {
		bump.SetOne()
	}
	sum, overflow := new(uint256.Int).AddOverflow(fee, bump)
	if overflow {
		sum.SetAllOne()
	}
	return *sum
}

// NewHeldPool returns an empty pool.
func NewHeldPool() *HeldPool {
	return &HeldPool{byHash: map[common.Hash]bool{}, inFlight: map[common.Hash]reservation{}}
}

// Push admits tx, whose sender has nonce onChain on its network: the next
// nonce of its chain, or a replacement of a queued one raising both fees. A
// transaction already held is admitted again without effect.
func (p *HeldPool) Push(tx *Held, onChain uint64) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.inFlight[tx.Hash]; ok || p.byHash[tx.Hash] {
		return nil
	}
	k := chainKey{tx.Sender, tx.Direction}
	refuse := func() error {
		next, ok := p.nextNonce(k, onChain)
		if !ok {
			next = ^uint64(0)
		}
		return &NonceError{Expected: next, OnChain: onChain}
	}
	if tx.Nonce < onChain {
		return refuse()
	}
	for i, queued := range p.txs {
		if queued.chainKey() != k || queued.Nonce != tx.Nonce {
			continue
		}
		maxFee, tip := minReplacement(&queued.MaxFee), minReplacement(&queued.Tip)
		if tx.MaxFee.Lt(&maxFee) || tx.Tip.Lt(&tip) {
			return &UnderpricedError{MaxFee: tx.MaxFee, RequiredMaxFee: maxFee, Tip: tx.Tip, RequiredTip: tip}
		}
		delete(p.byHash, queued.Hash)
		p.byHash[tx.Hash] = true
		p.txs[i] = tx
		return nil
	}
	// A nonce in flight lies below the next one, so it is refused here.
	if next, ok := p.nextNonce(k, onChain); !ok || tx.Nonce != next {
		return refuse()
	}
	p.byHash[tx.Hash] = true
	p.txs = append(p.txs, tx)
	return nil
}

func (tx *Held) chainKey() chainKey { return chainKey{tx.Sender, tx.Direction} }

// PushFront returns recovered, drained earlier, to the front in their order,
// releasing their reservations. A queued replacement of one of them wins:
// that one and the rest of its chain are dropped.
func (p *HeldPool) PushFront(recovered []*Held) {
	p.mu.Lock()
	defer p.mu.Unlock()
	// The lowest recovered nonce of each chain a queued replacement owns.
	conflict := map[chainKey]uint64{}
	for _, tx := range recovered {
		k := chainKey{tx.Sender, tx.Direction}
		for _, queued := range p.txs {
			if queued.chainKey() == k && queued.Nonce == tx.Nonce && queued.Hash != tx.Hash {
				if n, ok := conflict[k]; !ok || tx.Nonce < n {
					conflict[k] = tx.Nonce
				}
			}
		}
	}
	front := make([]*Held, 0, len(recovered))
	for _, tx := range recovered {
		delete(p.inFlight, tx.Hash)
		if n, ok := conflict[tx.chainKey()]; ok && tx.Nonce >= n {
			continue
		}
		if !p.byHash[tx.Hash] {
			p.byHash[tx.Hash] = true
			front = append(front, tx)
		}
	}
	p.txs = append(front, p.txs...)
}

// Release ends the reservations of txs, whose batch settled.
func (p *HeldPool) Release(txs []*Held) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, tx := range txs {
		delete(p.inFlight, tx.Hash)
	}
}

// Evict removes the queued transactions and reservations of sender's chain
// in direction from nonce on, returning the queued ones.
func (p *HeldPool) Evict(sender common.Address, direction Direction, nonce uint64) []*Held {
	p.mu.Lock()
	defer p.mu.Unlock()
	k := chainKey{sender, direction}
	var evicted []*Held
	kept := p.txs[:0]
	for _, tx := range p.txs {
		if tx.chainKey() == k && tx.Nonce >= nonce {
			delete(p.byHash, tx.Hash)
			evicted = append(evicted, tx)
			continue
		}
		kept = append(kept, tx)
	}
	clear(p.txs[len(kept):])
	p.txs = kept
	for h, r := range p.inFlight {
		if r.chainKey == k && r.nonce >= nonce {
			delete(p.inFlight, h)
		}
	}
	return evicted
}

// PopN drains up to n transactions from the front, reserving them.
func (p *HeldPool) PopN(n int) []*Held {
	p.mu.Lock()
	defer p.mu.Unlock()
	n = min(max(n, 0), len(p.txs))
	drained := slices.Clone(p.txs[:n])
	p.txs = slices.Delete(p.txs, 0, n)
	for _, tx := range drained {
		delete(p.byHash, tx.Hash)
		if _, ok := p.inFlight[tx.Hash]; !ok {
			p.inFlight[tx.Hash] = reservation{tx.chainKey(), tx.Nonce}
		}
	}
	return drained
}

// Len is the number of queued transactions.
func (p *HeldPool) Len() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.txs)
}

// nextNonce is the nonce that extends k's chain on onChain: one past its
// highest reservation or queued transaction at or above onChain, else
// onChain; false past the last nonce.
func (p *HeldPool) nextNonce(k chainKey, onChain uint64) (uint64, bool) {
	highest, found := uint64(0), false
	take := func(n uint64) {
		if n >= onChain && (!found || n > highest) {
			highest, found = n, true
		}
	}
	for _, tx := range p.txs {
		if tx.chainKey() == k {
			take(tx.Nonce)
		}
	}
	for _, r := range p.inFlight {
		if r.chainKey == k {
			take(r.nonce)
		}
	}
	if !found {
		return onChain, true
	}
	return highest + 1, highest != ^uint64(0)
}
