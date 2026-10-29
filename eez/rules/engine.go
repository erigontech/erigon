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
// Package rules is the rules engine of EEZ chains: post-merge Ethereum rules
// on a fixed block cadence, without a beacon chain.
package rules

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/eez/config"
	"github.com/erigontech/erigon/eez/protocol"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/misc"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/protocol/rules/ethash"
	"github.com/erigontech/erigon/execution/protocol/rules/merge"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/node/rulesconfig"
)

// ErrInvalidHeader reports a header that breaks an EEZ rule. It wraps
// rules.ErrInvalidBlock.
var ErrInvalidHeader = fmt.Errorf("%w: invalid eez header", rules.ErrInvalidBlock)

var registerOnce sync.Once

// Register makes CreateRulesEngine build this engine for EEZ chains.
func Register() {
	registerOnce.Do(func() {
		rulesconfig.RegisterL2Engine(config.Name, func(_ context.Context, chainConfig *chain.Config, _ log.Logger) rules.Engine {
			e, err := New(chainConfig)
			if err != nil {
				panic(fmt.Sprintf("eez rules engine: %v", err))
			}
			return e
		})
	})
}

// Engine is the EEZ rules engine. The embedded Merge engine provides the
// post-merge block processing; the overrides drop its total-difficulty
// gating, since an EEZ chain is proof-of-stake from genesis.
type Engine struct {
	*merge.Merge
	l2 *config.L2Config
}

var (
	_ rules.Engine          = (*Engine)(nil)
	_ rules.TxPostProcessor = (*Engine)(nil)

	systemAddress       = accounts.InternAddress(protocol.SystemAddress)
	nativeSystemAddress = accounts.InternAddress(protocol.NativeSystemAddress)
)

// New returns the engine of an EEZ chain configuration.
func New(chainConfig *chain.Config) (*Engine, error) {
	l2, ok := chainConfig.L2.(*config.L2Config)
	if !ok {
		return nil, fmt.Errorf("chain config l2 section is %T, want %T", chainConfig.L2, l2)
	}
	if err := l2.Validate(); err != nil {
		return nil, err
	}
	if err := l2.ValidateChain(chainConfig); err != nil {
		return nil, err
	}
	m := merge.New(ethash.NewFaker())
	m.SkipConsensusLayer = true
	return &Engine{Merge: m, l2: l2}, nil
}

// VerifyHeader checks header against its parent. Beyond the post-merge
// Ethereum rules it requires the configured interval and gas limit, a zero
// beacon root, no blob gas, and empty withdrawals and requests. Under rollup0
// prevRandao is zero; under rollupX derivation fixes it from L1.
func (e *Engine) VerifyHeader(chain rules.ChainHeaderReader, header *types.Header, _ bool) error {
	if !header.Number.IsUint64() {
		return fmt.Errorf("%w: block number %d beyond uint64", ErrInvalidHeader, &header.Number)
	}
	number := header.Number.Uint64()
	if number == 0 {
		return fmt.Errorf("%w: genesis has no parent", ErrInvalidHeader)
	}
	parent := chain.GetHeader(header.ParentHash, number-1)
	if parent == nil {
		return rules.ErrUnknownAncestor
	}
	return e.verifyHeader(chain.Config(), header, parent)
}

func (e *Engine) verifyHeader(config *chain.Config, header, parent *types.Header) error {
	invalid := func(format string, args ...any) error {
		return fmt.Errorf("%w: block %d: %s", ErrInvalidHeader, header.Number.Uint64(), fmt.Sprintf(format, args...))
	}
	switch {
	case !header.Number.Eq(new(uint256.Int).AddUint64(&parent.Number, 1)):
		return rules.ErrInvalidNumber
	case header.Time != parent.Time+e.l2.BlockInterval:
		return invalid("timestamp %d, want parent %d + %d", header.Time, parent.Time, e.l2.BlockInterval)
	case header.GasLimit != e.l2.BlockGasLimit:
		return invalid("gas limit %d, want %d", header.GasLimit, e.l2.BlockGasLimit)
	case header.GasUsed > header.GasLimit:
		return invalid("gas used %d above gas limit %d", header.GasUsed, header.GasLimit)
	case uint64(len(header.Extra)) > params.MaximumExtraDataSize:
		return invalid("extra data of %d bytes", len(header.Extra))
	case !header.Difficulty.IsZero():
		return invalid("difficulty %d", &header.Difficulty)
	case header.Nonce != (types.BlockNonce{}):
		return invalid("nonce %x", header.Nonce)
	case e.l2.RollupX == nil && header.MixDigest != (common.Hash{}):
		return invalid("prevRandao %x", header.MixDigest)
	case header.UncleHash != empty.UncleHash:
		return invalid("uncle hash %x", header.UncleHash)
	case !config.IsPrague(header.Time) || config.IsAmsterdam(header.Time):
		return invalid("EEZ blocks follow Prague to Osaka")
	}
	if err := misc.VerifyEip1559Header(config, parent, header); err != nil {
		return err
	}
	if err := misc.VerifyPresenceOfCancunHeaderFields(header); err != nil {
		return err
	}
	switch {
	case header.WithdrawalsHash == nil || *header.WithdrawalsHash != empty.RootHash:
		return invalid("withdrawals root is not the empty root")
	case *header.BlobGasUsed != 0:
		return invalid("blob gas used %d", *header.BlobGasUsed)
	case *header.ExcessBlobGas != misc.CalcExcessBlobGas(config, parent, header.Time):
		return invalid("excess blob gas %d", *header.ExcessBlobGas)
	case *header.ParentBeaconBlockRoot != (common.Hash{}):
		return invalid("parent beacon block root %x", *header.ParentBeaconBlockRoot)
	case header.RequestsHash == nil || *header.RequestsHash != empty.RequestsHash:
		return invalid("requests hash is not the empty requests hash")
	case header.SlotNumber != nil || header.BlockAccessListHash != nil:
		return invalid("Amsterdam fields")
	}
	return nil
}

// Initialize runs the block-start system calls. Under rollupX v0.0.1 it then
// burns the balances of EEZL2 and the system address: EEZL2's is mint its
// last block did not consume, and the system address only receives.
func (e *Engine) Initialize(cc *chain.Config, chain rules.ChainHeaderReader, header *types.Header,
	ibs *state.IntraBlockState, syscall rules.SysCallCustom, logger log.Logger, tracer *tracing.Hooks,
) error {
	if err := e.Merge.Initialize(cc, chain, header, ibs, syscall, logger, tracer); err != nil {
		return err
	}
	if e.l2.RuleSet() != config.RollupX {
		return nil
	}
	return burn(ibs, accounts.InternAddress(e.l2.EEZL2()), systemAddress)
}

// burn zeroes the balances of addrs; accounts without one stay untouched.
func burn(ibs *state.IntraBlockState, addrs ...accounts.Address) error {
	for _, addr := range addrs {
		balance, err := ibs.GetBalance(addr)
		if err != nil {
			return err
		}
		if !balance.IsZero() {
			if err := ibs.SetBalance(addr, uint256.Int{}, tracing.BalanceChangeUnspecified); err != nil {
				return err
			}
		}
	}
	return nil
}

// burnProtocolBalances is the rollupX v0.1 cleanup after a cross-chain group
// and at the end of every block: it burns what EEZL2 and the native system
// address hold.
func (e *Engine) burnProtocolBalances(ibs *state.IntraBlockState) error {
	return burn(ibs, accounts.InternAddress(protocol.RollupXEEZL2Address), nativeSystemAddress)
}

// PostProcessTx burns the protocol balances under rollupX v0.1 once
// txs[i] completes a cross-chain group: a delivery on its own, or the user
// transaction after a load.
func (e *Engine) PostProcessTx(_ *chain.Config, ibs *state.IntraBlockState, txs types.Transactions, i int) error {
	if e.l2.RuleSet() != config.RollupXV01 || !completesGroup(txs, i) {
		return nil
	}
	return e.burnProtocolBalances(ibs)
}

func completesGroup(txs types.Transactions, i int) bool {
	switch txs[i].Type() {
	case protocol.NativeTxType:
		return nativeCall(txs[i]) == protocol.ExecuteIncomingCrossChainCallSelector
	case protocol.HistoryTxType, protocol.SystemTxType:
		return false
	}
	return i > 0 && txs[i-1].Type() == protocol.NativeTxType && nativeCall(txs[i-1]) == protocol.LoadExecutionTableSelector
}

// nativeCall returns the selector of a native transaction's call to EEZL2.
func nativeCall(txn types.Transaction) [4]byte {
	var selector [4]byte
	copy(selector[:], txn.GetData())
	return selector
}

// Finalize rejects withdrawals, which only a beacon chain could issue, burns
// the protocol balances under rollupX v0.1, and collects the requests of the
// contracts present in state.
func (e *Engine) Finalize(config *chain.Config, header *types.Header, ibs *state.IntraBlockState,
	uncles []*types.Header, receipts types.Receipts, withdrawals []*types.Withdrawal,
	chain rules.ChainReader, syscall rules.SystemCall, skipReceiptsEval bool, logger log.Logger,
) (types.FlatRequests, error) {
	if err := e.finalize(ibs, withdrawals); err != nil {
		return nil, err
	}
	return e.Merge.Finalize(config, header, ibs, uncles, receipts, withdrawals, chain, syscall, skipReceiptsEval, logger)
}

func (e *Engine) FinalizeAndAssemble(config *chain.Config, header *types.Header, ibs *state.IntraBlockState,
	txs types.Transactions, uncles []*types.Header, receipts types.Receipts, withdrawals []*types.Withdrawal,
	chain rules.ChainReader, syscall rules.SystemCall, call rules.Call, logger log.Logger,
) (*types.Block, types.FlatRequests, error) {
	if err := e.finalize(ibs, withdrawals); err != nil {
		return nil, nil, err
	}
	return e.Merge.FinalizeAndAssemble(config, header, ibs, txs, uncles, receipts, withdrawals, chain, syscall, call, logger)
}

func (e *Engine) finalize(ibs *state.IntraBlockState, withdrawals []*types.Withdrawal) error {
	if len(withdrawals) > 0 {
		return fmt.Errorf("%w: %w: %d withdrawals", rules.ErrInvalidBlock, rules.ErrUnexpectedWithdrawals, len(withdrawals))
	}
	if e.l2.RuleSet() == config.RollupXV01 {
		return e.burnProtocolBalances(ibs)
	}
	return nil
}

// IsServiceTransaction reports whether sender is the system address of type
// 0x76, whose transactions execute fee-free. Types 0x45 and 0x46 are
// fee-free by their messages.
func (e *Engine) IsServiceTransaction(sender accounts.Address, _ rules.SystemCall) bool {
	return e.l2.RuleSet() != config.RollupXV01 && sender == systemAddress
}

// ValidateBlockPostExecution applies the default checks and rejects blob
// transactions, which EEZ chains do not support, and under rollupX v0.1 the
// misplaced system transactions.
func (e *Engine) ValidateBlockPostExecution(chainConfig *chain.Config, header *types.Header, gasUsed, blobGasUsed uint64,
	checkReceipts, checkBloom bool, receipts types.Receipts, txns types.Transactions, logger log.Logger,
) error {
	for i, txn := range txns {
		if txn.Type() == types.BlobTxType {
			return fmt.Errorf("%w: block %d: blob transaction %d", rules.ErrInvalidBlock, header.Number.Uint64(), i)
		}
	}
	if e.l2.RuleSet() == config.RollupXV01 {
		if err := e.validateV01(header, receipts, txns); err != nil {
			return fmt.Errorf("%w: block %d: %w", rules.ErrInvalidBlock, header.Number.Uint64(), err)
		}
	}
	return e.Merge.ValidateBlockPostExecution(chainConfig, header, gasUsed, blobGasUsed, checkReceipts, checkBloom, receipts, txns, logger)
}

// validateV01 checks the positions of the rollupX v0.1 system transactions:
// a history update only first and in an update block, succeeding without
// logs; a native transaction with the gas its block has left, calling
// loadExecutionTable or executeIncomingCrossChainCall; and every load
// followed by an ordinary transaction.
func (e *Engine) validateV01(header *types.Header, receipts types.Receipts, txns types.Transactions) error {
	withReceipts := len(receipts) == len(txns)
	for i, txn := range txns {
		var cumulative uint64
		if withReceipts && i > 0 {
			cumulative = receipts[i-1].CumulativeGasUsed
		}
		switch txn.Type() {
		case protocol.HistoryTxType:
			switch {
			case i > 0:
				return fmt.Errorf("history update at %d", i)
			case !e.l2.RollupX.HistoryUpdate(header.Number.Uint64()):
				return errors.New("history update off schedule")
			case withReceipts && (receipts[i].Status != types.ReceiptStatusSuccessful || len(receipts[i].Logs) > 0):
				return errors.New("history update failed or logged")
			}
		case protocol.NativeTxType:
			switch call := nativeCall(txn); {
			case call != protocol.LoadExecutionTableSelector && call != protocol.ExecuteIncomingCrossChainCallSelector:
				return fmt.Errorf("native transaction %d calls %x", i, call)
			case withReceipts && txn.GetGasLimit() != protocol.NativeTxGasLimit(header.GasLimit, cumulative):
				return fmt.Errorf("native transaction %d has gas limit %d, its position %d", i, txn.GetGasLimit(), protocol.NativeTxGasLimit(header.GasLimit, cumulative))
			case call == protocol.LoadExecutionTableSelector && (i+1 == len(txns) || systemType(txns[i+1].Type())):
				return fmt.Errorf("load %d without its user transaction", i)
			}
		case protocol.SystemTxType:
			return fmt.Errorf("type 0x76 transaction %d", i)
		}
	}
	return nil
}

func systemType(t byte) bool {
	return t == protocol.NativeTxType || t == protocol.HistoryTxType || t == protocol.SystemTxType
}

// Prepare sets the proof-of-stake difficulty.
func (e *Engine) Prepare(_ rules.ChainHeaderReader, header *types.Header, _ *state.IntraBlockState) error {
	header.Difficulty.Clear()
	return nil
}

// CalcDifficulty returns zero: every EEZ block is a proof-of-stake block.
func (e *Engine) CalcDifficulty(rules.ChainHeaderReader, uint64, uint64, uint256.Int, uint64, common.Hash, common.Hash, uint64) uint256.Int {
	return uint256.Int{}
}
