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
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"slices"
	"time"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/eez/protocol"
	"github.com/erigontech/erigon/execution/exec"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/txnprovider"
	"github.com/erigontech/erigon/txnprovider/txpool"
)

const (
	// MaxBatchGas is the EIP-7825 cap on a transaction's gas, which bounds
	// the posting of a batch.
	MaxBatchGas = 16_777_216
	// batchReserve is the gas a batch spends besides its blocks' transactions:
	// the transaction itself, its entries, proofs and encoding.
	batchReserve = 1_000_000
	// floorGasPerByte is the EIP-7623 floor price of a non-zero calldata byte,
	// the most a byte of a batch can cost.
	floorGasPerByte = 40
)

// DABudget is the bytes one block's transactions may add to a span, so that
// a slot of k blocks always settles in one batch of at most gas.
func DABudget(k, gas uint64) int {
	if gas <= batchReserve {
		return 0
	}
	return int((gas - batchReserve) / floorGasPerByte / k)
}

// MaxTxnSize is the largest transaction encoding a block of DA budget budget
// can carry.
func MaxTxnSize(budget int) uint64 {
	size := budget
	for size+uvarintLen(uint64(size)) > budget {
		size--
	}
	return uint64(size)
}

// daSize is the bytes tx adds to a span: its encoding and that encoding's
// length as a uvarint.
func daSize(tx types.Transaction) int {
	var n counter
	if err := tx.MarshalBinary(&n); err != nil {
		return math.MaxInt
	}
	return int(n) + uvarintLen(uint64(n))
}

func uvarintLen(n uint64) int {
	var buf [binary.MaxVarintLen64]byte
	return binary.PutUvarint(buf[:], n)
}

// counter counts the bytes written to it.
type counter int

func (c *counter) Write(p []byte) (int, error) {
	*c += counter(len(p))
	return len(p), nil
}

// poolTxns provides one block's transactions from the pool, none that EEZ
// refuses, for a block that takes them within budget bytes of DA.
type poolTxns struct {
	pool   txnprovider.TxnProvider
	parent common.Hash
	budget int
	logger log.Logger
}

// newPoolTxns returns a provider of pool transactions for one block on
// parent.
func newPoolTxns(pool txnprovider.TxnProvider, parent common.Hash, budget int, logger log.Logger) *poolTxns {
	return &poolTxns{pool: pool, parent: parent, budget: budget, logger: logger}
}

// TxnsBudget bounds the DA of the transactions the block takes, not of those
// it is offered: one the builder sets aside for its nonce or drops spends
// none of it.
func (p *poolTxns) TxnsBudget() *exec.TxnsBudget {
	return &exec.TxnsBudget{Size: daSize, Limit: p.budget}
}

// poolWait bounds the wait for the pool to see a block's parent.
var poolWait = time.Minute

// viewWait bounds the build's wait for the pool to see the parent's hash. By
// then the pool has seen its height and has only to apply the head changes
// made before the build, which takes milliseconds.
var viewWait = 5 * time.Second

// poolView is a pool that tells when its view of the chain reaches a block.
type poolView interface {
	PeekBest(ctx context.Context, n int, txns *txpool.TxnsRlp, onTopOf uint64) (bool, error)
}

// await returns once the pool has seen block parent's height. The build
// waits for it too, but on the goroutine derivation shares, where it also
// waits for the parent's hash, within viewWait: there the head stays at the
// parent, while here it may move on, past a view the pool would never reach.
func (p *poolTxns) await(ctx context.Context, parent uint64) error {
	v, ok := p.pool.(poolView)
	if !ok || !p.started() {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, poolWait)
	defer cancel()
	if _, err := v.PeekBest(ctx, 0, &txpool.TxnsRlp{}, parent); err != nil {
		return fmt.Errorf("txpool has not seen block %d: %w", parent, err)
	}
	return nil
}

// started reports whether the pool runs. One yet to start yields nothing,
// which the block would take for a dry pool.
func (p *poolTxns) started() bool {
	s, ok := p.pool.(interface{ Started() bool })
	return !ok || s.Started()
}

func (p *poolTxns) ProvideTxns(ctx context.Context, opts ...txnprovider.ProvideOption) ([]types.Transaction, error) {
	if !p.started() {
		return nil, errPoolNotStarted
	}
	// The pool marks what it yields in the caller's filter, so a page this
	// provider empties is not the pool running dry: the next holds others.
	pages := txnprovider.ApplyProvideOptions(opts...).TxnIdsFilter != nil
	// A pool that has seen the parent's height may hold another branch.
	opts = append(slices.Clip(opts), txnprovider.WithParentBlockHash(p.parent))
	for {
		txs, err := p.provide(ctx, opts)
		if err != nil {
			return nil, err
		}
		kept := p.keep(txs)
		if len(kept) > 0 || len(txs) == 0 || !pages {
			return kept, nil
		}
	}
}

// provide asks the pool for a page of transactions, within viewWait.
func (p *poolTxns) provide(ctx context.Context, opts []txnprovider.ProvideOption) ([]types.Transaction, error) {
	wctx, cancel := context.WithTimeout(ctx, viewWait)
	defer cancel()
	txs, err := p.pool.ProvideTxns(wctx, opts...)
	if err != nil && ctx.Err() == nil && errors.Is(err, context.DeadlineExceeded) {
		return nil, fmt.Errorf("txpool has not seen block %s: %w", p.parent, err)
	}
	return txs, err
}

// keep returns the transactions of txs a block may carry.
func (p *poolTxns) keep(txs []types.Transaction) []types.Transaction {
	kept := txs[:0]
	for _, tx := range txs {
		if reason := refused(tx); reason != "" {
			p.logger.Debug("[eez] pool transaction refused", "event", "eez.sequencer.pool.refused", "tx", tx.Hash(), "reason", reason)
			continue
		}
		kept = append(kept, tx)
	}
	return kept
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
