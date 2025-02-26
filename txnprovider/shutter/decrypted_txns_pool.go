// Copyright 2025 The Erigon Authors
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

package shutter

import (
	"context"
	"sync"

	"github.com/erigontech/erigon/core/types"
)

type DecryptionMark struct {
	Slot uint64
	Eon  EonIndex
}

type TxnBatch struct {
	Transactions  []types.Transaction
	TotalGasLimit uint64
}

type DecryptedTxnsPool struct {
	decryptedTxns  map[DecryptionMark]TxnBatch
	decryptionCond *sync.Cond
}

func NewDecryptedTxnsPool() *DecryptedTxnsPool {
	var mu sync.Mutex
	return &DecryptedTxnsPool{
		decryptedTxns:  make(map[DecryptionMark]TxnBatch),
		decryptionCond: sync.NewCond(&mu),
	}
}

func (p *DecryptedTxnsPool) Wait(ctx context.Context, mark DecryptionMark) error {
	done := make(chan struct{})
	go func() {
		defer close(done)

		p.decryptionCond.L.Lock()
		defer p.decryptionCond.L.Unlock()

		for _, ok := p.decryptedTxns[mark]; !ok && ctx.Err() == nil; _, ok = p.decryptedTxns[mark] {
			p.decryptionCond.Wait()
		}
	}()

	select {
	case <-ctx.Done():
		// note the below will wake up all waiters prematurely, but thanks to the for loop condition
		// in the waiting goroutine the ones that still need to wait will go back to sleep
		p.decryptionCond.Broadcast()
	case <-done:
		// no-op
	}

	return ctx.Err()
}

func (p *DecryptedTxnsPool) DecryptedTxns(mark DecryptionMark) (TxnBatch, bool) {
	p.decryptionCond.L.Lock()
	defer p.decryptionCond.L.Unlock()
	txnBatch, ok := p.decryptedTxns[mark]
	return txnBatch, ok
}

func (p *DecryptedTxnsPool) AddDecryptedTxns(mark DecryptionMark, txnBatch TxnBatch) {
	p.decryptionCond.L.Lock()
	defer p.decryptionCond.L.Unlock()
	p.decryptedTxns[mark] = txnBatch
	p.decryptionCond.Broadcast()
}

func (p *DecryptedTxnsPool) DeleteDecryptedTxnsUpToSlot(slot uint64) uint64 {
	p.decryptionCond.L.Lock()
	defer p.decryptionCond.L.Unlock()

	var deletions uint64
	for mark := range p.decryptedTxns {
		if mark.Slot <= slot {
			deletions++
			delete(p.decryptedTxns, mark)
		}
	}

	return deletions
}
