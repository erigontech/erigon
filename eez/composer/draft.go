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
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/chain"
	core "github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
)

// ErrStateRead reports a failed state read: the node's failure, not the
// transaction's, so the transaction waits.
var ErrStateRead = errors.New("state read failed")

// PreState is what a block executes on: its parent's state, the header
// chain the engine reads, and the hashes BLOCKHASH serves.
type PreState struct {
	Reader    state.StateReader
	Headers   rules.ChainHeaderReader
	BlockHash func(uint64) (common.Hash, error)
}

// Draft is a block under construction: its transactions executed on the
// parent's state, nothing committed. A transaction is final once applied,
// so a fork replays the draft's transactions on a fresh state.
type Draft struct {
	config     *chain.Config
	engine     rules.Engine
	header     *types.Header
	pre        PreState
	reads      *reads
	blockHash  func(uint64) (common.Hash, error)
	chainRules *chain.Rules
	ibs        *state.IntraBlockState
	gp         *core.GasPool
	gasUsed    core.GasUsed
	txs        []types.Transaction
	receipts   types.Receipts
	// ctx, when set, ends the draft's runs: a run it ends fails with its
	// error.
	ctx context.Context
}

// NewDraft opens the block of header, running the engine's opening system
// calls on pre.
func NewDraft(config *chain.Config, engine rules.Engine, header *types.Header, pre PreState) (*Draft, error) {
	r := &reads{StateReader: pre.Reader}
	d := &Draft{config: config, engine: engine, header: types.CopyHeader(header), pre: pre, reads: r, blockHash: r.blockHash(pre.BlockHash), ibs: state.New(r)}
	blockCtx := core.NewEVMBlockContext(d.header, d.blockHash, engine, accounts.InternAddress(d.header.Coinbase), config)
	d.chainRules = blockCtx.Rules(config)
	d.gp = new(core.GasPool).AddGas(header.GasLimit).AddBlobGas(config.GetMaxBlobGasPerBlock(header.Time))
	sysCall := func(contract accounts.Address, data []byte, ibs *state.IntraBlockState, header *types.Header, constCall bool) ([]byte, error) {
		return core.SysCallContract(contract, data, config, ibs, header, engine, constCall, vm.Config{})
	}
	err := engine.Initialize(config, pre.Headers, d.header, d.ibs, sysCall, log.Root(), nil)
	// A system call that failed to read is not reported by the engine.
	if err = errors.Join(r.failed(), err); err != nil {
		return nil, fmt.Errorf("open block %d: %w", header.Number.Uint64(), err)
	}
	if err := d.ibs.FinalizeTx(d.rules(), state.NewNoopWriter()); err != nil {
		return nil, err
	}
	return d, nil
}

func (d *Draft) rules() *chain.Rules { return d.chainRules }

// Apply executes tx at the end of the draft, observed by tracer when set.
// A transaction the block cannot hold fails Apply and leaves the draft as
// it was. A failed state read fails it with ErrStateRead, and the end of
// the draft's context with the context's error; either leaves the draft
// unusable.
func (d *Draft) Apply(tx types.Transaction, tracer *tracing.Hooks) (*types.Receipt, error) {
	i := len(d.txs)
	d.ibs.SetTxContext(d.header.Number.Uint64(), i)
	evm := core.CreateEVM(d.config, d.blockHash, d.engine, accounts.InternAddress(d.header.Coinbase), d.ibs, d.header, vm.Config{Tracer: tracer})
	if d.ctx != nil {
		if err := d.ctx.Err(); err != nil {
			return nil, err
		}
		defer context.AfterFunc(d.ctx, evm.Cancel)()
	}
	txs := append(slices.Clone(d.txs), tx)
	receipt, err := core.ApplyTransactionWithEVMPostProcessed(d.config, d.engine, d.gp, d.ibs, state.NewNoopWriter(), d.header, tx, &d.gasUsed,
		vm.Config{Tracer: tracer}, evm, core.TxPostProcess(d.engine, d.config, d.ibs, txs, i))
	// A cancelled EVM stops as if at a STOP: the context tells a cut-short run.
	if d.ctx != nil && d.ctx.Err() != nil {
		return nil, d.ctx.Err()
	}
	if failed := d.reads.failed(); failed != nil {
		return nil, failed
	}
	if err != nil {
		return nil, err
	}
	d.txs, d.receipts = txs, append(d.receipts, receipt)
	return receipt, nil
}

// Fork returns a draft with the same transactions, on a fresh state, ended
// by the same context.
func (d *Draft) Fork() (*Draft, error) {
	f, err := NewDraft(d.config, d.engine, d.header, d.pre)
	if err != nil {
		return nil, err
	}
	f.ctx = d.ctx
	for i, tx := range d.txs {
		if _, err := f.Apply(tx, nil); err != nil {
			return nil, fmt.Errorf("replay transaction %d: %w", i, err)
		}
	}
	return f, nil
}

// Nonce returns addr's nonce in the draft.
func (d *Draft) Nonce(addr common.Address) (uint64, error) {
	n, err := d.ibs.GetNonce(accounts.InternAddress(addr))
	if failed := d.reads.failed(); failed != nil {
		return 0, failed
	}
	return n, err
}

// Txs returns the draft's transactions.
func (d *Draft) Txs() []types.Transaction { return slices.Clone(d.txs) }

// Receipts returns the receipts of the draft's transactions.
func (d *Draft) Receipts() types.Receipts { return slices.Clone(d.receipts) }

// GasUsed is the gas the draft's transactions used.
func (d *Draft) GasUsed() uint64 { return d.gasUsed.BlockGasUsed() }
