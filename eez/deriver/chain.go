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

package deriver

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/dbservices"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/builder"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/exec"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/p2p/protocols/eth"
	"github.com/erigontech/erigon/rpc/jsonrpc/receipts"
	"github.com/erigontech/erigon/rpc/rpchelper"
	"github.com/erigontech/erigon/txnprovider"
)

// ErrRejected reports a block the local chain refuses to build or validate.
var ErrRejected = errors.New("block rejected by the local chain")

// Chain is the local L2 chain that derivation reads and extends.
type Chain interface {
	HashReader
	// HeaderByNumber returns the canonical header at number, nil when there
	// is none.
	HeaderByNumber(ctx context.Context, number uint64) (*types.Header, error)
	// BlockByNumber returns the canonical block at number, nil when there is
	// none.
	BlockByNumber(ctx context.Context, number uint64) (*types.Block, error)
	// Nonce returns the nonce of addr in the state after canonical block
	// number.
	Nonce(ctx context.Context, number uint64, addr common.Address) (uint64, error)
	// Receipts returns the receipts of a canonical block.
	Receipts(ctx context.Context, block *types.Block) (types.Receipts, error)
	// Extend builds the block of exactly txs on parent with header inputs in,
	// inserts it and makes it the head; safe and finalized must not be above
	// parent.
	Extend(ctx context.Context, parent *types.Header, in HeaderInputs, txs []types.Transaction, safe, finalized common.Hash) (*types.BlockWithReceipts, error)
	// ForkChoice sets the head, safe and finalized blocks.
	ForkChoice(ctx context.Context, head, safe, finalized common.Hash) error
}

// ExecChain is the Chain of an execution module that derivation drives
// alone.
type ExecChain struct {
	exec     *execmodule.ExecModule
	db       kv.TemporalRoDB
	reader   dbservices.FullBlockReader
	receipts *receipts.Generator
	config   *chain.Config
}

// NewExecChain returns the Chain of exec, read through db and reader.
func NewExecChain(exec *execmodule.ExecModule, db kv.TemporalRoDB, reader dbservices.FullBlockReader, receipts *receipts.Generator, config *chain.Config) *ExecChain {
	return &ExecChain{exec: exec, db: db, reader: reader, receipts: receipts, config: config}
}

func (c *ExecChain) CanonicalHash(ctx context.Context, number uint64) (hash common.Hash, ok bool, err error) {
	err = c.db.View(ctx, func(tx kv.Tx) error {
		hash, ok, err = c.reader.CanonicalHash(ctx, tx, number)
		return err
	})
	return hash, ok, err
}

func (c *ExecChain) CanonicalNumber(ctx context.Context, hash common.Hash) (number uint64, ok bool, err error) {
	err = c.db.View(ctx, func(tx kv.Tx) error {
		n, err := c.reader.HeaderNumber(ctx, tx, hash)
		if err != nil || n == nil {
			return err
		}
		// The number index keeps replaced siblings.
		canonical, found, err := c.reader.CanonicalHash(ctx, tx, *n)
		number, ok = *n, found && canonical == hash
		return err
	})
	return number, ok, err
}

func (c *ExecChain) HeaderByNumber(ctx context.Context, number uint64) (h *types.Header, err error) {
	err = c.db.View(ctx, func(tx kv.Tx) error {
		h, err = c.reader.HeaderByNumber(ctx, tx, number)
		return err
	})
	return h, err
}

func (c *ExecChain) BlockByNumber(ctx context.Context, number uint64) (b *types.Block, err error) {
	err = c.db.View(ctx, func(tx kv.Tx) error {
		b, err = c.reader.BlockByNumber(ctx, tx, number)
		return err
	})
	return b, err
}

func (c *ExecChain) Nonce(ctx context.Context, number uint64, addr common.Address) (nonce uint64, err error) {
	err = c.db.ViewTemporal(ctx, func(tx kv.TemporalTx) error {
		executed, err := stages.GetStageProgress(tx, stages.Execution)
		if err != nil {
			return err
		}
		if number > executed {
			return fmt.Errorf("state after block %d: executed up to %d", number, executed)
		}
		r, err := rpchelper.CreateUncachedStateReaderFromBlockNumber(ctx, tx, number, number == executed, 0, c.reader.TxnumReader())
		if err != nil {
			return err
		}
		a, err := r.ReadAccountData(accounts.InternAddress(addr))
		if err != nil || a == nil {
			return err
		}
		nonce = a.Nonce
		return nil
	})
	return nonce, err
}

func (c *ExecChain) Receipts(ctx context.Context, block *types.Block) (rs types.Receipts, err error) {
	err = c.db.ViewTemporal(ctx, func(tx kv.TemporalTx) error {
		rs, err = c.receipts.GetReceipts(ctx, c.config, tx, block, eth.ReceiptsOpts{})
		return err
	})
	return rs, err
}

func (c *ExecChain) Extend(ctx context.Context, parent *types.Header, in HeaderInputs, txs []types.Transaction, safe, finalized common.Hash) (*types.BlockWithReceipts, error) {
	// The builder builds on the head only.
	fc, err := c.exec.GetForkChoice(ctx)
	if err != nil {
		return nil, err
	}
	if fc.HeadHash != parent.Hash() {
		if err := c.ForkChoice(ctx, parent.Hash(), safe, finalized); err != nil {
			return nil, err
		}
	}
	extra := in.Extra
	if extra == nil {
		// nil selects the builder's own extra data.
		extra = []byte{}
	}
	gasLimit := in.GasLimit
	params := &builder.Parameters{
		ParentHash:            parent.Hash(),
		Timestamp:             in.Time,
		SuggestedFeeRecipient: in.Coinbase,
		TargetGasLimit:        &gasLimit,
		ExtraData:             extra,
		CustomTxnProvider:     staticTxns(txs),
		Strict:                true,
	}
	if c.config.IsShanghai(in.Time) {
		params.Withdrawals = []*types.Withdrawal{}
	}
	if c.config.IsCancun(in.Time) {
		params.ParentBeaconBlockRoot = &common.Hash{}
	}
	id, err := retryBusy(ctx, func() (uint64, bool, error) {
		r, err := c.exec.AssembleBlock(ctx, params)
		return r.PayloadID, r.Busy, err
	})
	if err != nil {
		return nil, fmt.Errorf("assemble block %d: %w", in.Number, err)
	}
	built, err := retryBusy(ctx, func() (*types.BlockWithReceipts, bool, error) {
		r, err := c.exec.GetAssembledBlock(ctx, id)
		if err == nil && r.Unknown {
			err = errors.New("payload dropped")
		}
		return r.Block, r.Busy, err
	})
	switch {
	case errors.Is(err, exec.ErrTxnNotIncluded):
		return nil, fmt.Errorf("%w: block %d: %w", ErrRejected, in.Number, err)
	case err != nil:
		return nil, fmt.Errorf("assemble block %d: %w", in.Number, err)
	case built == nil:
		return nil, fmt.Errorf("assemble block %d: no block", in.Number)
	}
	block := built.Block
	if !in.Matches(block.Header()) {
		return nil, fmt.Errorf("%w: block %d: built header differs from its inputs", ErrRejected, in.Number)
	}
	status, err := retryBusy(ctx, func() (execmodule.ExecutionStatus, bool, error) {
		s, err := c.exec.InsertBlocks(ctx, []*types.Block{block})
		return s, s == execmodule.ExecutionStatusBusy, err
	})
	if err != nil {
		return nil, fmt.Errorf("insert block %d: %w", in.Number, err)
	}
	if status != execmodule.ExecutionStatusSuccess {
		return nil, fmt.Errorf("insert block %d: %s", in.Number, status)
	}
	v, err := retryBusy(ctx, func() (execmodule.ValidationResult, bool, error) {
		v, err := c.exec.ValidateChain(ctx, block.Hash(), in.Number)
		return v, v.ValidationStatus == execmodule.ExecutionStatusBusy, err
	})
	switch {
	case err != nil:
		return nil, fmt.Errorf("validate block %d: %w", in.Number, err)
	case v.ValidationStatus == execmodule.ExecutionStatusBadBlock:
		return nil, fmt.Errorf("%w: block %d: %s", ErrRejected, in.Number, v.ValidationError)
	case v.ValidationStatus != execmodule.ExecutionStatusSuccess:
		return nil, fmt.Errorf("validate block %d: %s", in.Number, v.ValidationStatus)
	}
	if err := c.ForkChoice(ctx, block.Hash(), safe, finalized); err != nil {
		return nil, err
	}
	return built, nil
}

func (c *ExecChain) ForkChoice(ctx context.Context, head, safe, finalized common.Hash) error {
	r, err := retryBusy(ctx, func() (execmodule.ForkChoiceResult, bool, error) {
		r, err := c.exec.UpdateForkChoice(ctx, head, safe, finalized)
		return r, r.Status == execmodule.ExecutionStatusBusy, err
	})
	if err != nil {
		return fmt.Errorf("fork choice %x: %w", head, err)
	}
	if r.Status != execmodule.ExecutionStatusSuccess {
		return fmt.Errorf("fork choice %x: %s %s", head, r.Status, r.ValidationError)
	}
	// The module commits after it answers; reads go to the database.
	c.exec.WaitIdle(ctx)
	return ctx.Err()
}

// busyRetry is the pause before retrying an execution module that is busy
// with work of its own, such as pruning after a fork choice.
const busyRetry = 5 * time.Millisecond

func retryBusy[T any](ctx context.Context, f func() (T, bool, error)) (T, error) {
	for {
		r, busy, err := f()
		if err != nil || !busy {
			return r, err
		}
		select {
		case <-ctx.Done():
			return r, ctx.Err()
		case <-time.After(busyRetry):
		}
	}
}

// staticTxns provides the same transactions to every request.
type staticTxns []types.Transaction

func (s staticTxns) ProvideTxns(context.Context, ...txnprovider.ProvideOption) ([]types.Transaction, error) {
	return s, nil
}
