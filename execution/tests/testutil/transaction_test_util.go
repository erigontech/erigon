// Copyright 2015 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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

package testutil

import (
	"errors"
	"fmt"
	"maps"
	"math/big"
	"slices"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/math"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/tests/testforks"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
)

// TransactionTest checks RLP decoding and sender derivation of transactions.
type TransactionTest struct {
	RLP   hexutil.Bytes `json:"txbytes"`
	Forks ttForks       `json:"result"`
}

type ttForks map[string]ttFork

type ttFork struct {
	Exception    string                `json:"exception"`
	Sender       common.Address        `json:"sender"`
	Hash         common.Hash           `json:"hash"`
	IntrinsicGas *math.HexOrDecimal256 `json:"intrinsicGas"`
}

func (tt *TransactionTest) Run(chainID *uint256.Int) error {
	validateTx := func(rlpData hexutil.Bytes, signer types.Signer, rules *chain.Rules) (*common.Address, *common.Hash, uint64, error) {
		tx, err := types.DecodeTransaction(rlpData)
		if err != nil {
			return nil, nil, 0, err
		}
		msg, err := tx.AsMessage(signer, nil, rules)
		if err != nil {
			return nil, nil, 0, err
		}
		sender := msg.From()

		// Intrinsic gas
		authorizationsLen := uint64(0)
		if stx, ok := tx.(*types.SetCodeTransaction); ok {
			authorizationsLen = uint64(len(stx.GetAuthorizations()))
		}
		intrinsicGasResult, overflow := mdgas.IntrinsicGas(mdgas.IntrinsicGasCalcArgs{
			Data:               msg.Data(),
			AuthorizationsLen:  authorizationsLen,
			AccessListLen:      uint64(len(msg.AccessList())),
			StorageKeysLen:     uint64(msg.AccessList().StorageKeys()),
			IsContractCreation: msg.To().IsNil(),
			IsSelfTransfer:     !msg.To().IsNil() && msg.To() == sender,
			HasValue:           !msg.Value().IsZero(),
			IsEIP2:             rules.IsHomestead,
			IsEIP2028:          rules.IsIstanbul,
			IsEIP3860:          rules.IsShanghai,
			IsEIP7623:          rules.IsPrague,
			IsEIP7976:          rules.IsAmsterdam,
			IsEIP7981:          rules.IsAmsterdam,
			IsEIP2780:          rules.IsAmsterdam,
		})
		requiredGas := intrinsicGasResult.ExecutionGas
		if overflow {
			return nil, nil, 0, protocol.ErrGasUintOverflow
		}
		minimumGas := max(requiredGas, intrinsicGasResult.FloorGasCost)
		if minimumGas > msg.Gas() {
			return nil, nil, requiredGas, fmt.Errorf("insufficient gas ( %d < %d )", msg.Gas(), minimumGas)
		}
		if msg.To().IsNil() {
			if err := vm.CheckMaxInitCodeSize(uint64(len(msg.Data())), rules.IsShanghai, rules.IsAmsterdam); err != nil {
				return nil, nil, requiredGas, err
			}
		}

		if rules.IsLondon {
			// EIP-1559 gas fee cap
			err = protocol.CheckEip1559TxGasFeeCap(sender, msg.FeeCap(), msg.TipCap(), nil, false /* isFree */)
			if err != nil {
				return nil, nil, 0, err
			}
		}
		_, overflow = new(uint256.Int).MulOverflow(uint256.NewInt(msg.Gas()), msg.FeeCap())
		if overflow {
			return nil, nil, 0, errors.New("GasLimitPriceProductOverflow")
		}

		// EIP-2681: Limit account nonce to 2^64-1
		if msg.Nonce()+1 < msg.Nonce() {
			return nil, nil, requiredGas, fmt.Errorf("%w: nonce: %d", protocol.ErrNonceMax, msg.Nonce())
		}
		h := tx.Hash()
		senderValue := sender.Value()
		return &senderValue, &h, requiredGas, nil
	}

	forkNames := slices.Sorted(maps.Keys(tt.Forks))
	validated := false
	for _, forkName := range forkNames {
		config, ok := testforks.Forks[forkName]
		if !ok || config == nil {
			continue
		}
		validated = true
		fork := tt.Forks[forkName]
		rules := (&evmtypes.BlockContext{}).Rules(config)
		signer := types.MakeSignerFromRules(chainID, rules)
		sender, txhash, intrinsicGas, err := validateTx(tt.RLP, *signer, rules)

		if fork.Exception != "" {
			if err == nil {
				return fmt.Errorf("expected error %v, got none [%v]", fork.Exception, forkName)
			}
			continue
		}
		// Should resolve the right address
		if err != nil {
			return fmt.Errorf("got error, expected none: %w", err)
		}
		if sender == nil {
			return fmt.Errorf("sender was nil, should be %x", fork.Sender)
		}
		if *sender != fork.Sender {
			return fmt.Errorf("sender mismatch: got %x, want %x", sender, fork.Sender)
		}
		if txhash == nil {
			return fmt.Errorf("txhash was nil, should be %x", fork.Hash)
		}
		if *txhash != fork.Hash {
			return fmt.Errorf("hash mismatch: got %x, want %x", *txhash, fork.Hash)
		}
		if new(big.Int).SetUint64(intrinsicGas).Cmp((*big.Int)(fork.IntrinsicGas)) != 0 {
			return fmt.Errorf("intrinsic gas mismatch: got %x, want %x", intrinsicGas, (*big.Int)(fork.IntrinsicGas))
		}
	}
	if !validated && len(forkNames) > 0 {
		return testforks.UnsupportedForkError{Name: forkNames[0]}
	}
	return nil
}
