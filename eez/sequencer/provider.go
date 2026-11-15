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

package sequencer

import (
	"context"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/eez/protocol"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/txnprovider"
)

const (
	// maxBatchGas is the EIP-7825 cap on a transaction's gas, which bounds
	// the posting of a batch.
	maxBatchGas = 16_777_216
	// batchReserve is the gas a batch spends besides its blocks' transactions:
	// the transaction itself, its entries, proofs and encoding.
	batchReserve = 1_000_000
	// floorGasPerByte is the EIP-7623 floor price of a non-zero calldata byte,
	// the most a byte of a batch can cost.
	floorGasPerByte = 40
	// txOverhead is what a transaction adds to the DA payload besides its
	// encoding: its length as a uvarint32.
	txOverhead = 5
	// minTxDA is below the smallest ECDSA-signed transaction: r and s alone
	// take about 66 bytes.
	minTxDA = 64
	// maxEmptiedPages bounds the walk of a pool whose pending transactions
	// all exceed the budget left.
	maxEmptiedPages = 8
)

// DABudget is the DA bytes of transactions one block may carry, so that a
// slot of k blocks always settles in one batch.
func DABudget(k uint64) int {
	return int((maxBatchGas - batchReserve) / floorGasPerByte / k)
}

// poolTxns provides one block's transactions from the pool: none that EEZ
// refuses, and at most budget bytes of DA.
type poolTxns struct {
	pool   txnprovider.TxnProvider
	budget int
	logger log.Logger
}

// newPoolTxns returns a provider of one block of pool transactions.
func newPoolTxns(pool txnprovider.TxnProvider, budget int, logger log.Logger) *poolTxns {
	return &poolTxns{pool: pool, budget: budget, logger: logger}
}

func (p *poolTxns) ProvideTxns(ctx context.Context, opts ...txnprovider.ProvideOption) ([]types.Transaction, error) {
	// The pool marks what it yields in the caller's filter, so a page this
	// provider empties is not the pool running dry: the next holds others.
	pages := txnprovider.ApplyProvideOptions(opts...).TxnIdsFilter != nil
	for emptied := 1; ; emptied++ {
		txs, err := p.pool.ProvideTxns(ctx, opts...)
		if err != nil {
			return nil, err
		}
		kept := p.keep(txs)
		if len(kept) > 0 || len(txs) == 0 || !pages || p.budget < minTxDA || emptied == maxEmptiedPages {
			return kept, nil
		}
	}
}

// keep returns the transactions of txs a block may carry, within the budget
// left, which it takes from.
func (p *poolTxns) keep(txs []types.Transaction) []types.Transaction {
	kept := txs[:0]
	for _, tx := range txs {
		if reason := refused(tx); reason != "" {
			p.logger.Debug("[eez] pool transaction refused", "event", "eez.sequencer.pool.refused", "tx", tx.Hash(), "reason", reason)
			continue
		}
		// A transaction over the remaining budget leaves room for smaller ones
		// after it.
		if size := tx.EncodingSize() + txOverhead; size <= p.budget {
			p.budget -= size
			kept = append(kept, tx)
		}
	}
	return kept
}

// noTxns provides no transactions.
type noTxns struct{}

func (noTxns) ProvideTxns(context.Context, ...txnprovider.ProvideOption) ([]types.Transaction, error) {
	return nil, nil
}

var systemAddress = accounts.InternAddress(protocol.SystemAddress)

// refused returns why EEZ refuses tx from a block, empty when it does not:
// blob transactions, which no batch carries, and the EEZ system types and
// sender, which only derivation places.
func refused(tx types.Transaction) string {
	switch tx.Type() {
	case types.BlobTxType:
		return "blob transaction"
	case protocol.SystemTxType, protocol.NativeTxType, protocol.HistoryTxType:
		return "system transaction type"
	}
	if sender, ok := tx.GetSender(); ok && sender == systemAddress {
		return "system sender"
	}
	return ""
}
