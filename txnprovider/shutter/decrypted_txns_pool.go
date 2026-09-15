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
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/binary"
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
	decryptedTxns         map[DecryptionMark]TxnBatch
	decryptedTxnRevisions map[DecryptionMark]uint64
	decryptionCond        *sync.Cond
}

func NewDecryptedTxnsPool() *DecryptedTxnsPool {
	var mu sync.Mutex
	return &DecryptedTxnsPool{
		decryptedTxns:         make(map[DecryptionMark]TxnBatch),
		decryptedTxnRevisions: make(map[DecryptionMark]uint64),
		decryptionCond:        sync.NewCond(&mu),
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
	revision := txnBatchRevision(txnBatch)
	p.decryptionCond.L.Lock()
	defer p.decryptionCond.L.Unlock()
	p.decryptedTxns[mark] = txnBatch
	p.decryptedTxnRevisions[mark] = revision
	p.decryptionCond.Broadcast()
	txnsLen := float64(len(txnBatch.Transactions))
	decryptedTxnsPoolAdded.Add(txnsLen)
	decryptedTxnsPoolTotalCount.Add(txnsLen)
	decryptedTxnsPoolTotalBytes.Add(float64(txnBatch.TotalBytes))
}

func (p *DecryptedTxnsPool) DeleteDecryptedTxnsUpToSlot(slot uint64) (markDeletions, txnDeletions uint64) {
	p.decryptionCond.L.Lock()
	defer p.decryptionCond.L.Unlock()

	var totalBytes int64
	for mark, txnBatch := range p.decryptedTxns {
		if mark.Slot <= slot {
			markDeletions++
			txnDeletions += uint64(len(txnBatch.Transactions))
			totalBytes += txnBatch.TotalBytes
			delete(p.decryptedTxns, mark)
			delete(p.decryptedTxnRevisions, mark)
		}
	}

	decryptedTxnsPoolDeleted.Add(float64(txnDeletions))
	decryptedTxnsPoolTotalCount.Sub(float64(txnDeletions))
	decryptedTxnsPoolTotalBytes.Sub(float64(totalBytes))
	return markDeletions, txnDeletions
}

func (p *DecryptedTxnsPool) TransactionSetRevision(slot uint64) uint64 {
	p.decryptionCond.L.Lock()
	defer p.decryptionCond.L.Unlock()

	marks := make([]DecryptionMark, 0, len(p.decryptedTxns))
	for mark := range p.decryptedTxns {
		if mark.Slot == slot {
			marks = append(marks, mark)
		}
	}
	if len(marks) == 0 {
		return 0
	}
	slices.SortFunc(marks, func(a, b DecryptionMark) int { return cmp.Compare(a.Eon, b.Eon) })

	hasher := sha256.New()
	var word [8]byte
	for _, mark := range marks {
		binary.LittleEndian.PutUint64(word[:], uint64(mark.Eon))
		_, _ = hasher.Write(word[:])
		binary.LittleEndian.PutUint64(word[:], p.decryptedTxnRevisions[mark])
		_, _ = hasher.Write(word[:])
	}
	return binary.LittleEndian.Uint64(hasher.Sum(nil))
}

func txnBatchRevision(batch TxnBatch) uint64 {
	hasher := sha256.New()
	var word [8]byte
	binary.LittleEndian.PutUint64(word[:], batch.TotalGasLimit)
	_, _ = hasher.Write(word[:])
	binary.LittleEndian.PutUint64(word[:], uint64(batch.TotalBytes))
	_, _ = hasher.Write(word[:])
	binary.LittleEndian.PutUint64(word[:], uint64(len(batch.Transactions)))
	_, _ = hasher.Write(word[:])
	for _, txn := range batch.Transactions {
		if txn == nil {
			_, _ = hasher.Write([]byte{0})
			continue
		}
		_, _ = hasher.Write([]byte{1})
		hash := txn.Hash()
		_, _ = hasher.Write(hash[:])
	}
	return binary.LittleEndian.Uint64(hasher.Sum(nil))
}

func (p *DecryptedTxnsPool) AllDecryptedTxns() []types.Transaction {
	p.decryptionCond.L.Lock()
	defer p.decryptionCond.L.Unlock()
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
