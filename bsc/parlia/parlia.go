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

// Package parlia implements BSC's Parlia (PoSA) consensus engine.
package parlia

import (
	"errors"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/misc"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
	"github.com/erigontech/erigon/rpc"
)

var errNotSupported = errors.New("parlia: block production/execution not supported by the download-only stub")

// bscSystemContracts are the addresses a Parlia system transaction may target.
var bscSystemContracts = map[common.Address]struct{}{
	common.HexToAddress("0x0000000000000000000000000000000000001000"): {}, // Validator
	common.HexToAddress("0x0000000000000000000000000000000000001001"): {}, // Slash
	common.HexToAddress("0x0000000000000000000000000000000000001002"): {}, // SystemReward
	common.HexToAddress("0x0000000000000000000000000000000000001003"): {}, // LightClient
	common.HexToAddress("0x0000000000000000000000000000000000001004"): {}, // TokenHub
	common.HexToAddress("0x0000000000000000000000000000000000001005"): {}, // RelayerIncentivize
	common.HexToAddress("0x0000000000000000000000000000000000001006"): {}, // RelayerHub
	common.HexToAddress("0x0000000000000000000000000000000000001007"): {}, // GovHub
	common.HexToAddress("0x0000000000000000000000000000000000002000"): {}, // CrossChain
	common.HexToAddress("0x0000000000000000000000000000000000002002"): {}, // StakeHub
	common.HexToAddress("0x0000000000000000000000000000000000002004"): {}, // Governor
	common.HexToAddress("0x0000000000000000000000000000000000002005"): {}, // GovToken
	common.HexToAddress("0x0000000000000000000000000000000000002006"): {}, // Timelock
	common.HexToAddress("0x0000000000000000000000000000000000003000"): {}, // TokenRecoverPortal
}

// Parlia is the permissive stub engine. See package docs.
type Parlia struct {
	chainConfig *chain.Config
	signer      *types.Signer
	logger      log.Logger
}

func New(chainConfig *chain.Config, logger log.Logger) *Parlia {
	return &Parlia{chainConfig: chainConfig, signer: types.LatestSigner(chainConfig), logger: logger}
}

// IsSystemTransaction reports whether tx is a Parlia system transaction: a
// gas-price-zero call to a system contract from the block validator.
func (p *Parlia) IsSystemTransaction(tx types.Transaction, header *types.Header) (bool, error) {
	to := tx.GetTo()
	if to == nil {
		return false, nil
	}
	if _, ok := bscSystemContracts[*to]; !ok {
		return false, nil
	}
	if !tx.GetTipCap().IsZero() {
		return false, nil
	}
	sender, err := tx.Sender(*p.signer)
	if err != nil {
		return false, errors.New("parlia: unauthorized system transaction")
	}
	return sender.Value() == header.Coinbase, nil
}

// ApplySystemTx performs the consensus state effect for a system transaction and
// runs it. distributeToSystem/distributeToValidator move the reward from
// SystemAddress to the validator before it is forwarded on-chain as the tx value.
func (p *Parlia) ApplySystemTx(tx types.Transaction, ibs *state.IntraBlockState, header *types.Header, run rules.SystemTxRun) (uint64, error) {
	if value := tx.GetValue(); !value.IsZero() {
		if err := ibs.SubBalance(params.SystemAddress, *value, tracing.BalanceChangeUnspecified); err != nil {
			return 0, err
		}
		if err := ibs.AddBalance(accounts.InternAddress(header.Coinbase), *value, tracing.BalanceChangeUnspecified); err != nil {
			return 0, err
		}
	}
	return run(ibs)
}

// --- EngineReader ---

// Author returns the header coinbase, which on BSC is the block's validator.
// The stub does not ecrecover from the seal.
func (p *Parlia) Author(header *types.Header) (accounts.Address, error) {
	return accounts.InternAddress(header.Coinbase), nil
}

func (p *Parlia) TxDependencies(header *types.Header) [][]int { return nil }

func (p *Parlia) IsServiceTransaction(sender accounts.Address, syscall rules.SystemCall) bool {
	return false
}

func (p *Parlia) Type() chain.RulesName { return chain.ParliaRules }

func (p *Parlia) CalculateRewards(config *chain.Config, header *types.Header, uncles []*types.Header,
	syscall rules.SystemCall) ([]rules.Reward, error) {
	return nil, nil
}

func (p *Parlia) GetTransferFunc() evmtypes.TransferFunc { return misc.Transfer }

func (p *Parlia) GetPostApplyMessageFunc() evmtypes.PostApplyMessageFunc { return nil }

func (p *Parlia) ValidateBlockPostExecution(chainConfig *chain.Config, header *types.Header,
	gasUsed, blobGasUsed uint64, checkReceipts, checkBloom bool,
	receipts types.Receipts, txns types.Transactions, logger log.Logger) error {
	return nil
}

func (p *Parlia) Close() error { return nil }

// --- EngineWriter ---

func (p *Parlia) VerifyHeader(chain rules.ChainHeaderReader, header *types.Header, seal bool) error {
	return nil
}

func (p *Parlia) VerifyUncles(chain rules.ChainReader, header *types.Header, uncles []*types.Header) error {
	return nil
}

func (p *Parlia) Prepare(chain rules.ChainHeaderReader, header *types.Header, state *state.IntraBlockState) error {
	return nil
}

func (p *Parlia) Initialize(config *chain.Config, chain rules.ChainHeaderReader, header *types.Header,
	state *state.IntraBlockState, syscall rules.SysCallCustom, logger log.Logger, tracer *tracing.Hooks) error {
	return nil
}

func (p *Parlia) Finalize(config *chain.Config, header *types.Header, state *state.IntraBlockState,
	uncles []*types.Header, receipts types.Receipts, withdrawals []*types.Withdrawal, chain rules.ChainReader,
	syscall rules.SystemCall, skipReceiptsEval bool, logger log.Logger) (types.FlatRequests, error) {
	return nil, nil
}

func (p *Parlia) FinalizeAndAssemble(config *chain.Config, header *types.Header, state *state.IntraBlockState,
	txs types.Transactions, uncles []*types.Header, receipts types.Receipts, withdrawals []*types.Withdrawal,
	chain rules.ChainReader, syscall rules.SystemCall, call rules.Call, logger log.Logger,
) (*types.Block, types.FlatRequests, error) {
	return nil, nil, errNotSupported
}

func (p *Parlia) Seal(chain rules.ChainHeaderReader, block *types.BlockWithReceipts,
	results chan<- *types.BlockWithReceipts, stop <-chan struct{}) error {
	return errNotSupported
}

func (p *Parlia) SealHash(header *types.Header) common.Hash { return header.Hash() }

func (p *Parlia) CalcDifficulty(chain rules.ChainHeaderReader, time, parentTime uint64,
	parentDifficulty uint256.Int, parentNumber uint64, parentHash, parentUncleHash common.Hash,
	parentAuRaStep uint64) uint256.Int {
	return *uint256.NewInt(2)
}

func (p *Parlia) APIs(chain rules.ChainHeaderReader) []rpc.API { return nil }
