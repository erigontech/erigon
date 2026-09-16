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
	"fmt"
	"sort"
	"strconv"

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

// blockNumberTimeBoundary separates a BlockAlloc key that is a block number from
// one that is a unix timestamp. BSC's block-numbered forks are all far below it
// (Chapel's highest is ~3e7) and its timestamped forks all far above it (the
// earliest is ~1.7e9), so any value in that gap classifies every key correctly.
const blockNumberTimeBoundary = 1_000_000_000

type systemContractUpgrade struct {
	numOrTime uint64
	byTime    bool
	alloc     types.GenesisAlloc
}

// Parlia is the permissive stub engine. See package docs.
type Parlia struct {
	chainConfig *chain.Config
	signer      *types.Signer
	logger      log.Logger
	upgrades    []systemContractUpgrade
}

func New(chainConfig *chain.Config, logger log.Logger) *Parlia {
	p := &Parlia{chainConfig: chainConfig, signer: types.LatestSigner(chainConfig), logger: logger}
	if chainConfig.Parlia != nil {
		p.upgrades = parseSystemContractUpgrades(chainConfig.Parlia.BlockAlloc)
	}
	return p
}

func parseSystemContractUpgrades(blockAlloc map[string]any) []systemContractUpgrade {
	upgrades := make([]systemContractUpgrade, 0, len(blockAlloc))
	for key, raw := range blockAlloc {
		numOrTime, err := strconv.ParseUint(key, 10, 64)
		if err != nil {
			panic(fmt.Errorf("parlia: invalid blockAlloc key %q: %w", key, err))
		}
		alloc, err := types.DecodeGenesisAlloc(raw)
		if err != nil {
			panic(fmt.Errorf("parlia: invalid blockAlloc[%s]: %w", key, err))
		}
		upgrades = append(upgrades, systemContractUpgrade{
			numOrTime: numOrTime,
			byTime:    numOrTime >= blockNumberTimeBoundary,
			alloc:     alloc,
		})
	}
	sort.Slice(upgrades, func(i, j int) bool { return upgrades[i].numOrTime < upgrades[j].numOrTime })
	return upgrades
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

// ApplySystemTx performs the consensus state effect before a system transaction
// runs: distributeToSystem/distributeToValidator move the reward from
// SystemAddress to the validator, which forwards it on-chain as the tx value.
func (p *Parlia) ApplySystemTx(tx types.Transaction, ibs *state.IntraBlockState, header *types.Header) error {
	value := tx.GetValue()
	if value.IsZero() {
		return nil
	}
	if err := ibs.SubBalance(params.SystemAddress, *value, tracing.BalanceChangeUnspecified); err != nil {
		return err
	}
	return ibs.AddBalance(accounts.InternAddress(header.Coinbase), *value, tracing.BalanceChangeUnspecified)
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
	ibs *state.IntraBlockState, syscall rules.SysCallCustom, logger log.Logger, tracer *tracing.Hooks) error {
	return p.upgradeSystemContracts(chain, header, ibs)
}

func (p *Parlia) upgradeSystemContracts(chain rules.ChainHeaderReader, header *types.Header, ibs *state.IntraBlockState) error {
	number := header.Number.Uint64()

	var parentTime uint64
	var haveParentTime bool
	ensureParentTime := func() error {
		if haveParentTime {
			return nil
		}
		if number > 0 {
			parent := chain.GetHeader(header.ParentHash, number-1)
			if parent == nil {
				return fmt.Errorf("parlia: missing parent header for block %d, cannot evaluate timestamp system-contract upgrade", number)
			}
			parentTime = parent.Time
		}
		haveParentTime = true
		return nil
	}

	for i := range p.upgrades {
		u := &p.upgrades[i]
		if u.byTime {
			if header.Time < u.numOrTime {
				continue
			}
			// A silent parentTime == 0 would apply every past timestamp upgrade at
			// once, so a missing parent header is a hard error.
			if err := ensureParentTime(); err != nil {
				return err
			}
			if parentTime >= u.numOrTime {
				continue
			}
		} else if u.numOrTime != number {
			continue
		}
		for addr, account := range u.alloc {
			if err := ibs.SetCode(accounts.InternAddress(addr), account.Code, tracing.CodeChangeUnspecified); err != nil {
				return err
			}
		}
	}
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
