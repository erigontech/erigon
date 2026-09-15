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
}

func (p *revisionTxnProvider) ProvideTxns(context.Context, ...txnprovider.ProvideOption) ([]types.Transaction, error) {
	return nil, nil
}

func (p *revisionTxnProvider) TransactionSetRevision(uint64) uint64 {
	return p.revision.Load()
}

func TestPoolTransactionSetRevisionIncludesBaseAndDecryptedTransactions(t *testing.T) {
	base := &revisionTxnProvider{}
	decrypted := NewDecryptedTxnsPool()
	pool := &Pool{
		baseTxnProvider:   base,
		decryptedTxnsPool: decrypted,
		slotCalculator:    NewBeaconChainSlotCalculator(0, 12),
	}

	initial := pool.TransactionSetRevision(12)
	base.revision.Add(1)
	baseChanged := pool.TransactionSetRevision(12)
	require.NotEqual(t, initial, baseChanged)

	decrypted.AddDecryptedTxns(DecryptionMark{Slot: 2, Eon: 1}, TxnBatch{})
	require.Equal(t, baseChanged, pool.TransactionSetRevision(12))
	decrypted.AddDecryptedTxns(DecryptionMark{Slot: 3, Eon: 1}, TxnBatch{})
	require.Equal(t, baseChanged, pool.TransactionSetRevision(12))

	decrypted.AddDecryptedTxns(DecryptionMark{Slot: 1, Eon: 1}, TxnBatch{})
	require.NotEqual(t, baseChanged, pool.TransactionSetRevision(12))
}
