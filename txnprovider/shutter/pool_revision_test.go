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

package shutter

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/txnprovider"
)

type revisionTxnProvider struct {
	revision atomic.Uint64
	provide  func(context.Context)
}

func (p *revisionTxnProvider) ProvideTxns(ctx context.Context, _ ...txnprovider.ProvideOption) ([]types.Transaction, error) {
	if p.provide != nil {
		p.provide(ctx)
	}
	txnprovider.ObserveTxnRevision(ctx, p.revision.Load())
	return nil, nil
}

func (p *revisionTxnProvider) TransactionSetRevision(uint64, uint64) uint64 {
	return p.revision.Load()
}

type revisionEonTracker struct{}

func (revisionEonTracker) Run(context.Context) error { return nil }
func (revisionEonTracker) CurrentEon() (Eon, bool)   { return Eon{Index: 1}, true }
func (revisionEonTracker) RecentEon(index EonIndex) (Eon, bool) {
	return Eon{Index: index}, true
}
func (revisionEonTracker) EonByBlockNum(uint64) (Eon, bool) { return Eon{Index: 1}, true }

func TestPoolTransactionSetRevisionIncludesBaseAndDecryptedTransactions(t *testing.T) {
	base := &revisionTxnProvider{}
	decrypted := NewDecryptedTxnsPool()
	pool := &Pool{
		baseTxnProvider:   base,
		decryptedTxnsPool: decrypted,
		slotCalculator:    NewBeaconChainSlotCalculator(0, 12),
		eonTracker:        revisionEonTracker{},
	}

	initial := pool.TransactionSetRevision(12, 100)
	base.revision.Add(1)
	baseChanged := pool.TransactionSetRevision(12, 100)
	require.NotEqual(t, initial, baseChanged)

	decrypted.AddDecryptedTxns(DecryptionMark{Slot: 1, Eon: 2}, TxnBatch{})
	require.Equal(t, baseChanged, pool.TransactionSetRevision(12, 100))
	decrypted.AddDecryptedTxns(DecryptionMark{Slot: 1, Eon: 3}, TxnBatch{})
	require.Equal(t, baseChanged, pool.TransactionSetRevision(12, 100))

	decrypted.AddDecryptedTxns(DecryptionMark{Slot: 1, Eon: 1}, TxnBatch{})
	require.NotEqual(t, baseChanged, pool.TransactionSetRevision(12, 100))
}

func TestPoolObservesBaseSnapshotWithSelectedDecryptedRevision(t *testing.T) {
	base := &revisionTxnProvider{}
	base.revision.Store(1)
	decrypted := NewDecryptedTxnsPool()
	pool := &Pool{
		baseTxnProvider:   base,
		decryptedTxnsPool: decrypted,
		slotCalculator:    NewBeaconChainSlotCalculator(0, 12),
		eonTracker:        revisionEonTracker{},
	}
	mark := DecryptionMark{Slot: 1, Eon: 1}
	decrypted.AddDecryptedTxns(mark, TxnBatch{TotalGasLimit: 1})
	selectedDecryptedRevision := decrypted.TransactionSetRevision(mark)

	base.provide = func(ctx context.Context) {
		base.revision.Store(2)
		txnprovider.ObserveTxnRevision(ctx, 2)
		decrypted.AddDecryptedTxns(mark, TxnBatch{TotalGasLimit: 2})
	}
	var observed atomic.Uint64
	ctx := txnprovider.WithTxnRevisionObserver(t.Context(), observed.Store)
	_, err := pool.provideBaseTxns(ctx, selectedDecryptedRevision, txnprovider.WithBlockTime(12), txnprovider.WithParentBlockNum(100))
	require.NoError(t, err)
	require.Equal(t, combineTransactionSetRevisions(2, selectedDecryptedRevision), observed.Load())

	current := pool.TransactionSetRevision(12, 100)
	require.NotEqual(t, observed.Load(), current)
	decrypted.AddDecryptedTxns(DecryptionMark{Slot: 1, Eon: 2}, TxnBatch{TotalGasLimit: 3})
	require.Equal(t, current, pool.TransactionSetRevision(12, 100))
}
