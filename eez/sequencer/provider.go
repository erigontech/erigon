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
	"errors"

	"github.com/erigontech/erigon/common"
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
	// all exceed the budget left, though an emptier block would carry them.
	// Transactions no block can carry do not count: the pool's pending limit
	// bounds a walk past them.
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
	pool txnprovider.TxnProvider
	// full is the block's budget, budget what is left of it.
	full, budget int
	// charged holds the transactions the budget paid for: the builder has
	// the pool yield again one it set aside for its nonce.
	charged map[common.Hash]struct{}
	// stalled holds, per sender, the lowest nonce left out of the block: the
	// sender's later transactions cannot execute in it.
	stalled map[accounts.Address]uint64
	logger  log.Logger
}

// newPoolTxns returns a provider of one block of pool transactions.
func newPoolTxns(pool txnprovider.TxnProvider, budget int, logger log.Logger) *poolTxns {
	return &poolTxns{pool: pool, full: budget, budget: budget, charged: map[common.Hash]struct{}{}, stalled: map[accounts.Address]uint64{}, logger: logger}
}

func (p *poolTxns) ProvideTxns(ctx context.Context, opts ...txnprovider.ProvideOption) ([]types.Transaction, error) {
	// A pool yet to start yields nothing, which the block would take for a
	// dry pool.
	if s, ok := p.pool.(interface{ Started() bool }); ok && !s.Started() {
		return nil, errPoolNotStarted
	}
	// The pool marks what it yields in the caller's filter, so a page this
	// provider empties is not the pool running dry: the next holds others.
	pages := txnprovider.ApplyProvideOptions(opts...).TxnIdsFilter != nil
	emptied := 0
	for {
		txs, err := p.pool.ProvideTxns(ctx, opts...)
		if err != nil {
			return nil, err
		}
		kept, overBudget := p.keep(txs)
		if overBudget {
			emptied++
		}
		if len(kept) > 0 || len(txs) == 0 || !pages || p.budget < minTxDA || emptied == maxEmptiedPages {
			return kept, nil
		}
	}
}

// keep returns the transactions of txs a block may carry, within the budget
// left, which it takes from, and whether one an emptier block would carry
// exceeded it. The pool yields a sender's transactions in nonce order and
// withholds one only for the gas or space the block lacks, so a sender left
// out is one keep has seen.
func (p *poolTxns) keep(txs []types.Transaction) (kept []types.Transaction, overBudget bool) {
	kept = txs[:0]
	for _, tx := range txs {
		if reason := refused(tx); reason != "" {
			p.logger.Debug("[eez] pool transaction refused", "event", "eez.sequencer.pool.refused", "tx", tx.Hash(), "reason", reason)
			p.stall(tx)
			continue
		}
		if _, ok := p.charged[tx.Hash()]; ok {
			kept = append(kept, tx)
			continue
		}
		if sender, ok := tx.GetSender(); ok {
			if nonce, ok := p.stalled[sender]; ok && nonce < tx.GetNonce() {
				continue
			}
		}
		// A transaction over the remaining budget leaves room for smaller ones
		// after it.
		switch size := tx.EncodingSize() + txOverhead; {
		case size <= p.budget:
			p.budget -= size
			p.charged[tx.Hash()] = struct{}{}
			kept = append(kept, tx)
		case size <= p.full:
			overBudget = true
			p.stall(tx)
		default:
			p.stall(tx)
		}
	}
	return kept, overBudget
}

// stall records that tx is left out of the block.
func (p *poolTxns) stall(tx types.Transaction) {
	sender, ok := tx.GetSender()
	if !ok {
		return
	}
	if nonce, ok := p.stalled[sender]; !ok || tx.GetNonce() < nonce {
		p.stalled[sender] = tx.GetNonce()
	}
}

// noTxns provides no transactions.
type noTxns struct{}

func (noTxns) ProvideTxns(context.Context, ...txnprovider.ProvideOption) ([]types.Transaction, error) {
	return nil, nil
}

var errPoolNotStarted = errors.New("txpool not started")

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
