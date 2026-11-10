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

package parlia

import (
	"fmt"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/bsc/paymentlane"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
)

const paymentLaneGetterGas = 50_000_000

var _ rules.LaneEngine = (*Parlia)(nil)

// HasLanes reports whether the block may carry the payment lane (BEP-703).
func (p *Parlia) HasLanes(header *types.Header) bool {
	return p.chainConfig.IsJenner(header.Number.Uint64(), header.Time)
}

// LoadLanes reads the payment lane metadata from the parent post-state. The lane starts on
// the block after Jenner, so the Jenner block itself has none.
func (p *Parlia) LoadLanes(chain rules.ChainHeaderReader, header *types.Header, ibs *state.IntraBlockState) (any, error) {
	number := header.Number.Uint64()
	parent := chain.GetHeader(header.ParentHash, number-1)
	if parent == nil {
		return nil, fmt.Errorf("parlia: missing parent header for block %d, cannot load the payment lane", number)
	}
	if !p.chainConfig.IsJenner(parent.Number.Uint64(), parent.Time) {
		return nil, nil
	}
	evm := vm.NewEVM(paymentLaneBlockContext(header), evmtypes.TxContext{}, ibs, p.chainConfig, vm.Config{RestoreState: true})
	contract := accounts.InternAddress(paymentlane.ContractAddress)
	meta, err := paymentlane.LoadMeta(func(input []byte) ([]byte, error) {
		ret, _, _, err := evm.StaticCall(accounts.ZeroAddress, contract, input, mdgas.MdGas{Execution: paymentLaneGetterGas})
		return ret, err
	})
	if stateErr := ibs.StateReadError(); stateErr != nil {
		return nil, fmt.Errorf("parlia: block %d: payment lane state read: %w", number, stateErr)
	}
	if err != nil {
		return nil, fmt.Errorf("parlia: block %d: %w", number, err)
	}
	return meta, nil
}

func paymentLaneBlockContext(header *types.Header) evmtypes.BlockContext {
	return evmtypes.BlockContext{
		CanTransfer: func(evmtypes.IntraBlockState, accounts.Address, uint256.Int) (bool, error) { return true, nil },
		Transfer: func(evmtypes.IntraBlockState, accounts.Address, accounts.Address, uint256.Int, bool, *chain.Rules) error {
			return nil
		},
		GetHash:     func(uint64) (common.Hash, error) { return common.Hash{}, nil },
		Coinbase:    accounts.InternAddress(header.Coinbase),
		BlockNumber: header.Number.Uint64(),
		Time:        header.Time,
		Difficulty:  header.Difficulty,
		GasLimit:    header.GasLimit,
	}
}

// VerifyLanes checks the payment lane rule on an executed block. A block whose start was
// not executed in this run has no lanes to check against.
func (p *Parlia) VerifyLanes(header *types.Header, lanes any, txs types.Transactions, laneTxs []rules.LaneTx) error {
	meta, _ := lanes.(*paymentlane.Meta)
	if meta == nil {
		return nil
	}
	number := header.Number.Uint64()
	if len(laneTxs) != len(txs) {
		return fmt.Errorf("parlia: block %d: payment lane inputs for %d txs, got %d", number, len(txs), len(laneTxs))
	}
	var paymentGasUsed uint64
	for i, tx := range txs {
		isSystemTx, err := p.IsSystemTransaction(tx, header)
		if err != nil {
			return err
		}
		if paymentlane.IsPayment(tx, isSystemTx, meta.Listed(), laneTxs[i].RecipientHasCode) {
			paymentGasUsed += laneTxs[i].GasUsed
		}
	}
	return paymentlane.CheckInequality(header.GasLimit, header.GasUsed, paymentGasUsed, meta.Quota(header.GasLimit))
}
