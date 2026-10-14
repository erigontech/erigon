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
	_ rules.Engine = (*Engine)(nil)

	systemAddress = accounts.InternAddress(protocol.SystemAddress)
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
	return &Engine{Merge: merge.New(ethash.NewFaker(), merge.WithOptionalRequestSources()), l2: l2}, nil
}

// VerifyHeader checks header against its parent. Beyond the post-merge
// Ethereum rules it requires the configured interval and gas limit, a zero
// prevRandao and beacon root, no blob gas, and empty withdrawals and requests.
func (e *Engine) VerifyHeader(chain rules.ChainHeaderReader, header *types.Header, _ bool) error {
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
	case header.Number.Uint64() != parent.Number.Uint64()+1:
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
	case header.MixDigest != (common.Hash{}):
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

// Finalize rejects withdrawals, which only a beacon chain could issue, and
// collects the requests of the contracts present in state.
func (e *Engine) Finalize(config *chain.Config, header *types.Header, ibs *state.IntraBlockState,
	uncles []*types.Header, receipts types.Receipts, withdrawals []*types.Withdrawal,
	chain rules.ChainReader, syscall rules.SystemCall, skipReceiptsEval bool, logger log.Logger,
) (types.FlatRequests, error) {
	if len(withdrawals) > 0 {
		return nil, fmt.Errorf("%w: %w: %d withdrawals", rules.ErrInvalidBlock, rules.ErrUnexpectedWithdrawals, len(withdrawals))
	}
	return e.Merge.Finalize(config, header, ibs, uncles, receipts, withdrawals, chain, syscall, skipReceiptsEval, logger)
}

func (e *Engine) FinalizeAndAssemble(config *chain.Config, header *types.Header, ibs *state.IntraBlockState,
	txs types.Transactions, uncles []*types.Header, receipts types.Receipts, withdrawals []*types.Withdrawal,
	chain rules.ChainReader, syscall rules.SystemCall, call rules.Call, logger log.Logger,
) (*types.Block, types.FlatRequests, error) {
	if len(withdrawals) > 0 {
		return nil, nil, fmt.Errorf("%w: %w: %d withdrawals", rules.ErrInvalidBlock, rules.ErrUnexpectedWithdrawals, len(withdrawals))
	}
	return e.Merge.FinalizeAndAssemble(config, header, ibs, txs, uncles, receipts, withdrawals, chain, syscall, call, logger)
}

// IsServiceTransaction reports whether sender is the EEZ system address,
// whose transactions execute fee-free.
func (e *Engine) IsServiceTransaction(sender accounts.Address, _ rules.SystemCall) bool {
	return sender == systemAddress
}

// ValidateBlockPostExecution applies the default checks and rejects blob
// transactions, which EEZ chains do not support.
func (e *Engine) ValidateBlockPostExecution(chainConfig *chain.Config, header *types.Header, gasUsed, blobGasUsed uint64,
	checkReceipts, checkBloom bool, receipts types.Receipts, txns types.Transactions, logger log.Logger,
) error {
	for i, txn := range txns {
		if txn.Type() == types.BlobTxType {
			return fmt.Errorf("%w: block %d: blob transaction %d", rules.ErrInvalidBlock, header.Number.Uint64(), i)
		}
	}
	return e.Merge.ValidateBlockPostExecution(chainConfig, header, gasUsed, blobGasUsed, checkReceipts, checkBloom, receipts, txns, logger)
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
