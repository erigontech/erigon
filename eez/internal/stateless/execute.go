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
// Package stateless executes EEZ blocks on reth-shaped execution witnesses.
package stateless

import (
	"errors"
	"fmt"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
)

var (
	// ErrWitnessHeaders reports witness headers that do not chain to the
	// block's parent.
	ErrWitnessHeaders = errors.New("witness headers do not chain to the parent")
	// ErrStateRoot reports a post-state root other than the header's.
	ErrStateRoot = fmt.Errorf("%w: post-state root mismatch", rules.ErrInvalidBlock)
	// ErrBodyMismatch reports a body other than the one the header commits to.
	ErrBodyMismatch = fmt.Errorf("%w: body does not match the header", rules.ErrInvalidBlock)
)

// Witness is an execution witness in the shape of reth's
// debug_executionWitness: trie nodes and bytecodes in any order, and RLP
// headers from the oldest BLOCKHASH target to the parent.
type Witness struct {
	State   [][]byte
	Codes   [][]byte
	Headers [][]byte
}

// Result is the outcome of executing a block on its witness.
type Result struct {
	StateRoot common.Hash
	// TxStateRoots holds the state root after each transaction, with
	// WithTxStateRoots.
	TxStateRoots []common.Hash
	Receipts     types.Receipts
}

// Option configures Execute.
type Option func(*options)

type options struct {
	txStateRoots bool
}

// WithTxStateRoots records the state root after each transaction.
func WithTxStateRoots() Option {
	return func(o *options) { o.txStateRoots = true }
}

// Verify executes block on its witness and requires the header's state root.
func Verify(chainConfig *chain.Config, engine rules.Engine, block *types.Block, w *Witness, opts ...Option) (*Result, error) {
	res, err := Execute(chainConfig, engine, block, w, opts...)
	if err != nil {
		return nil, err
	}
	if res.StateRoot != block.Root() {
		return res, fmt.Errorf("%w: block %d: got %x, header %x", ErrStateRoot, block.NumberU64(), res.StateRoot, block.Root())
	}
	return res, nil
}

// Execute runs block on the parent state its witness proves: header
// verification, block initialization, the transactions, the post-execution
// checks and finalization. It returns the post-state root.
func Execute(chainConfig *chain.Config, engine rules.Engine, block *types.Block, w *Witness, opts ...Option) (*Result, error) {
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	header := block.HeaderNoCopy()
	headers, err := newHeaderReader(chainConfig, block, w.Headers)
	if err != nil {
		return nil, err
	}
	if err := engine.VerifyHeader(headers, header, true); err != nil {
		return nil, err
	}
	if err := block.Body().MatchesHeader(header); err != nil {
		return nil, fmt.Errorf("%w: block %d: %w", ErrBodyMismatch, block.NumberU64(), err)
	}
	ws, err := newWitnessState(w.State, w.Codes, headers.parent.Root)
	if err != nil {
		return nil, err
	}
	ibs := state.New(ws)
	defer ibs.Close()

	logger := log.Root()
	evm := protocol.CreateEVM(chainConfig, headers.blockHash, engine, accounts.InternAddress(header.Coinbase), ibs, header, vm.Config{})
	chainRules := evm.ChainRules()
	sysCall := func(contract accounts.Address, data []byte, ibs *state.IntraBlockState, header *types.Header, constCall bool) ([]byte, error) {
		return protocol.SysCallContract(contract, data, chainConfig, ibs, header, engine, constCall, vm.Config{})
	}
	if err := engine.Initialize(chainConfig, headers, header, ibs, sysCall, logger, nil); err != nil {
		return nil, fmt.Errorf("block %d: initialize: %w", block.NumberU64(), err)
	}
	if err := ibs.FinalizeTx(chainRules, ws); err != nil {
		return nil, err
	}

	res := &Result{}
	gp := new(protocol.GasPool).AddGas(header.GasLimit).AddBlobGas(chainConfig.GetMaxBlobGasPerBlock(header.Time))
	var gasUsed protocol.GasUsed
	for i, txn := range block.Transactions() {
		ibs.SetTxContext(block.NumberU64(), i)
		receipt, err := protocol.ApplyTransactionWithEVM(chainConfig, engine, gp, ibs, ws, header, txn, &gasUsed, vm.Config{}, evm)
		if err != nil {
			return nil, fmt.Errorf("block %d: transaction %d: %w", block.NumberU64(), i, err)
		}
		res.Receipts = append(res.Receipts, receipt)
		if o.txStateRoots {
			root, err := ws.flush()
			if err != nil {
				return nil, err
			}
			res.TxStateRoots = append(res.TxStateRoots, root)
		}
	}
	if err := engine.ValidateBlockPostExecution(chainConfig, header, gasUsed.BlockGasUsed(), gasUsed.Blob, true, true,
		res.Receipts, block.Transactions(), logger); err != nil {
		return nil, err
	}

	syscall := func(contract accounts.Address, data []byte) ([]byte, error) {
		return sysCall(contract, data, ibs, header, false)
	}
	if _, err := engine.Finalize(chainConfig, types.CopyHeader(header), ibs, block.Uncles(), res.Receipts, block.Withdrawals(),
		nil, syscall, false, logger); err != nil {
		return nil, fmt.Errorf("block %d: finalize: %w", block.NumberU64(), err)
	}
	if err := ibs.CommitBlock(chainRules, ws); err != nil {
		return nil, err
	}
	if res.StateRoot, err = ws.flush(); err != nil {
		return nil, err
	}
	return res, nil
}

// headerReader serves the witness headers, the BLOCKHASH sources of a block.
type headerReader struct {
	rules.ChainHeaderReader
	config   *chain.Config
	parent   *types.Header
	byHash   map[common.Hash]*types.Header
	byNumber map[uint64]*types.Header
}

func newHeaderReader(chainConfig *chain.Config, block *types.Block, encoded [][]byte) (*headerReader, error) {
	r := &headerReader{config: chainConfig, byHash: map[common.Hash]*types.Header{}, byNumber: map[uint64]*types.Header{}}
	for _, enc := range encoded {
		h := new(types.Header)
		if err := rlp.DecodeBytes(enc, h); err != nil {
			return nil, fmt.Errorf("witness header: %w", err)
		}
		if r.parent != nil && h.ParentHash != r.parent.Hash() {
			return nil, fmt.Errorf("%w: header %d", ErrWitnessHeaders, h.Number.Uint64())
		}
		r.parent = h
		r.byHash[h.Hash()] = h
		r.byNumber[h.Number.Uint64()] = h
	}
	if r.parent == nil || r.parent.Hash() != block.ParentHash() {
		return nil, fmt.Errorf("%w: block %d", ErrWitnessHeaders, block.NumberU64())
	}
	return r, nil
}

func (r *headerReader) Config() *chain.Config { return r.config }

func (r *headerReader) GetHeader(hash common.Hash, number uint64) *types.Header {
	if h := r.byHash[hash]; h != nil && h.Number.Uint64() == number {
		return h
	}
	return nil
}

func (r *headerReader) GetHeaderByNumber(number uint64) *types.Header { return r.byNumber[number] }

func (r *headerReader) GetHeaderByHash(hash common.Hash) *types.Header { return r.byHash[hash] }

func (r *headerReader) blockHash(number uint64) (common.Hash, error) {
	if h := r.byNumber[number]; h != nil {
		return h.Hash(), nil
	}
	return common.Hash{}, fmt.Errorf("%w: no header %d for BLOCKHASH", ErrIncompleteWitness, number)
}
