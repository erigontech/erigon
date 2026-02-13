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
	"slices"
	"sync"

	"github.com/erigontech/erigon/execution/types"
)

type DecryptionMark struct {
	Slot uint64
	Eon  EonIndex
}

type TxnBatch struct {
	Transactions  []types.Transaction
	TotalGasLimit uint64
	TotalBytes    int64
}

type DecryptedTxnsPool struct {
	mu            sync.Mutex
	decryptedTxns map[DecryptionMark]TxnBatch
	// decryptionChange is closed and replaced each time txns are added.
	// Using a channel instead of sync.Cond for compatibility with testing/synctest.
	decryptionChange chan struct{}
}

func NewDecryptedTxnsPool() *DecryptedTxnsPool {
	return &DecryptedTxnsPool{
		decryptedTxns:    make(map[DecryptionMark]TxnBatch),
		decryptionChange: make(chan struct{}),
	}
}

func (p *DecryptedTxnsPool) Wait(ctx context.Context, mark DecryptionMark) error {
	for {
		p.mu.Lock()
		_, ok := p.decryptedTxns[mark]
		ch := p.decryptionChange
		p.mu.Unlock()

		if ok {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ch:
		}
	}
}

func (p *DecryptedTxnsPool) DecryptedTxns(mark DecryptionMark) (TxnBatch, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	txnBatch, ok := p.decryptedTxns[mark]
	return txnBatch, ok
}

func (p *DecryptedTxnsPool) AddDecryptedTxns(mark DecryptionMark, txnBatch TxnBatch) {
	p.mu.Lock()
	p.decryptedTxns[mark] = txnBatch
	close(p.decryptionChange)
	p.decryptionChange = make(chan struct{})
	p.mu.Unlock()

	txnsLen := float64(len(txnBatch.Transactions))
	decryptedTxnsPoolAdded.Add(txnsLen)
	decryptedTxnsPoolTotalCount.Add(txnsLen)
	decryptedTxnsPoolTotalBytes.Add(float64(txnBatch.TotalBytes))
}

func (p *DecryptedTxnsPool) DeleteDecryptedTxnsUpToSlot(slot uint64) (markDeletions, txnDeletions uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()

	var totalBytes int64
	for mark, txnBatch := range p.decryptedTxns {
		if mark.Slot <= slot {
			markDeletions++
			txnDeletions += uint64(len(txnBatch.Transactions))
			totalBytes += txnBatch.TotalBytes
			delete(p.decryptedTxns, mark)
		}
	}

	decryptedTxnsPoolDeleted.Add(float64(txnDeletions))
	decryptedTxnsPoolTotalCount.Sub(float64(txnDeletions))
	decryptedTxnsPoolTotalBytes.Sub(float64(totalBytes))
	return markDeletions, txnDeletions
}

func (p *DecryptedTxnsPool) AllDecryptedTxns() []types.Transaction {
	p.mu.Lock()
	defer p.mu.Unlock()
	var totalTxns int
	marks := make([]DecryptionMark, 0, len(p.decryptedTxns))
	for mark, txnBatch := range p.decryptedTxns {
		totalTxns += len(txnBatch.Transactions)
		marks = append(marks, mark)
	}
	slices.SortStableFunc(marks, func(a, b DecryptionMark) int {
		if a.Slot < b.Slot {
			return -1
		}
		if a.Slot > b.Slot {
			return 1
		}
		return 0
	})
	txns := make([]types.Transaction, 0, totalTxns)
	for _, mark := range marks {
		txns = append(txns, p.decryptedTxns[mark].Transactions...)
	}
	return txns
}
