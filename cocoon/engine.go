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

package cocoon

import (
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/misc"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
	"github.com/erigontech/erigon/rpc"
)

// Engine is the rules.Engine for a sovereign chain whose blocks are produced
// and trusted by the embedder: it neither seals nor validates consensus, and
// finalization applies nothing beyond what transaction execution already did.
type Engine struct{}

var _ rules.Engine = (*Engine)(nil)

func (*Engine) Author(header *types.Header) (accounts.Address, error) {
	return accounts.InternAddress(header.Coinbase), nil
}

func (*Engine) TxDependencies(*types.Header) [][]int { return nil }

func (*Engine) IsServiceTransaction(accounts.Address, rules.SystemCall) bool { return false }

func (*Engine) Type() chain.RulesName { return chain.CocoonRules }

func (*Engine) CalculateRewards(*chain.Config, *types.Header, []*types.Header, rules.SystemCall,
) ([]rules.Reward, error) {
	return nil, nil
}

func (*Engine) GetTransferFunc() evmtypes.TransferFunc { return misc.Transfer }

func (*Engine) GetPostApplyMessageFunc() evmtypes.PostApplyMessageFunc {
	return misc.LogSelfDestructedAccounts // EIP-7708, same as an L1 post-merge chain
}

func (*Engine) GetStartTxFunc() evmtypes.StartTxFunc { return nil }

func (*Engine) GetGasChargingFunc() evmtypes.GasChargingFunc { return nil }

func (*Engine) GetComputeRefundFunc() evmtypes.ComputeRefundFunc { return nil }

func (*Engine) AmendBlockContext(*evmtypes.BlockContext, *types.Header) {}

func (*Engine) ValidateBlockPostExecution(chainConfig *chain.Config, header *types.Header,
	gasUsed, blobGasUsed uint64, checkReceipts, checkBloom bool,
	receipts types.Receipts, txns types.Transactions, logger log.Logger) error {
	return rules.DefaultBlockPostValidation(chainConfig, header, gasUsed, blobGasUsed, checkReceipts, checkBloom, receipts, txns, logger)
}

func (*Engine) Close() error { return nil }

func (*Engine) VerifyHeader(rules.ChainHeaderReader, *types.Header, bool) error { return nil }

func (*Engine) VerifyUncles(rules.ChainReader, *types.Header, []*types.Header) error { return nil }

func (*Engine) Prepare(rules.ChainHeaderReader, *types.Header, *state.IntraBlockState) error {
	return nil
}

func (*Engine) Initialize(*chain.Config, rules.ChainHeaderReader, *types.Header,
	*state.IntraBlockState, rules.SysCallCustom, log.Logger, *tracing.Hooks) error {
	return nil
}

func (*Engine) Finalize(*chain.Config, *types.Header, *state.IntraBlockState,
	[]*types.Header, types.Receipts, []*types.Withdrawal, rules.ChainReader, rules.SystemCall, bool, log.Logger,
) (types.FlatRequests, error) {
	return nil, nil
}

func (*Engine) FinalizeAndAssemble(_ *chain.Config, header *types.Header, _ *state.IntraBlockState,
	txs types.Transactions, uncles []*types.Header, receipts types.Receipts, withdrawals []*types.Withdrawal,
	_ rules.ChainReader, _ rules.SystemCall, _ rules.Call, _ log.Logger,
) (*types.Block, types.FlatRequests, error) {
	return types.NewBlockForAsembling(header, txs, uncles, receipts, withdrawals), nil, nil
}

// Seal treats the block as already produced by the embedder, mirroring how
// the Merge engine handles externally-produced (post-merge) blocks: it does
// not seal anything, it only forwards the block to results.
func (*Engine) Seal(_ rules.ChainHeaderReader, blockWithReceipts *types.BlockWithReceipts, results chan<- *types.BlockWithReceipts, _ <-chan struct{}) error {
	block := blockWithReceipts.Block
	header := block.Header()
	header.Nonce = types.BlockNonce{}

	select {
	case results <- &types.BlockWithReceipts{
		Block:           block.WithSeal(header),
		Receipts:        blockWithReceipts.Receipts,
		Requests:        blockWithReceipts.Requests,
		BlockAccessList: blockWithReceipts.BlockAccessList,
	}:
	default:
		log.Warn("Sealing result is not read", "sealhash", block.Hash())
	}
	return nil
}

func (*Engine) SealHash(header *types.Header) common.Hash { return header.Hash() }

func (*Engine) CalcDifficulty(rules.ChainHeaderReader, uint64, uint64, uint256.Int, uint64,
	common.Hash, common.Hash, uint64) uint256.Int {
	return uint256.Int{}
}

func (*Engine) APIs(rules.ChainHeaderReader) []rpc.API { return nil }
