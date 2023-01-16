// Copyright 2015 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package tests

import (
	"bytes"
	"errors"
	"fmt"
	"math/big"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/common/math"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/rlp"
)

// TransactionTest checks RLP decoding and sender derivation of transactions.
type TransactionTest struct {
	RLP   hexutil.Bytes `json:"txbytes"`
	Forks ttForks       `json:"result"`
}

type ttForks struct {
	Berlin            ttFork
	Byzantium         ttFork
	Constantinople    ttFork
	ConstantinopleFix ttFork
	EIP150            ttFork
	EIP158            ttFork
	Frontier          ttFork
	Homestead         ttFork
	Istanbul          ttFork
	London            ttFork
}

type ttFork struct {
	Exception    string                `json:"exception"`
	Sender       libcommon.Address     `json:"sender"`
	Hash         libcommon.Hash        `json:"hash"`
	IntrinsicGas *math.HexOrDecimal256 `json:"intrinsicGas"`
}

func (tt *TransactionTest) Run(chainID *big.Int) error {
	validateTx := func(rlpData hexutil.Bytes, signer types.Signer, rules *chain.Rules) (*libcommon.Address, *libcommon.Hash, uint64, error) {
		tx, err := types.DecodeTransaction(rlp.NewStream(bytes.NewReader(rlpData), 0))
		if err != nil {
			return nil, nil, 0, err
		}
		msg, err := tx.AsMessage(signer, nil, rules)
		if err != nil {
			return nil, nil, 0, err
		}
		sender := msg.From()

		// Intrinsic gas
		requiredGas, err := core.IntrinsicGas(msg.Data(), msg.AccessList(), msg.To() == nil, rules.IsHomestead, rules.IsIstanbul, rules.IsShanghai)
		if err != nil {
			return nil, nil, 0, err
		}
		if requiredGas > msg.Gas() {
			return nil, nil, requiredGas, fmt.Errorf("insufficient gas ( %d < %d )", msg.Gas(), requiredGas)
		}

		if rules.IsLondon {
			// EIP-1559 gas fee cap
			err = core.CheckEip1559TxGasFeeCap(sender, msg.FeeCap(), msg.Tip(), nil, false /* isFree */)
			if err != nil {
				return nil, nil, 0, err
			}
			// A corollary check of the following assert from EIP-1559:
			// signer.balance >= transaction.gas_limit * transaction.max_fee_per_gas
			_, overflow := new(uint256.Int).MulOverflow(uint256.NewInt(msg.Gas()), msg.FeeCap())
			if overflow {
				return nil, nil, 0, errors.New("GasLimitPriceProductOverflow")
			}
		}

		// EIP-2681: Limit account nonce to 2^64-1
		if msg.Nonce()+1 < msg.Nonce() {
			return nil, nil, requiredGas, fmt.Errorf("%w: nonce: %d", core.ErrNonceMax, msg.Nonce())
		}
		h := tx.Hash()
		return &sender, &h, requiredGas, nil
	}

	for _, testcase := range []struct {
		name   string
		signer *types.Signer
		fork   ttFork
		config *chain.Config
	}{
		{"Frontier", types.MakeFrontierSigner(), tt.Forks.Frontier, Forks["Frontier"]},
		{"Homestead", types.LatestSignerForChainID(nil), tt.Forks.Homestead, Forks["Homestead"]},
		{"EIP150", types.LatestSignerForChainID(nil), tt.Forks.EIP150, Forks["EIP150"]},
		{"EIP158", types.LatestSignerForChainID(chainID), tt.Forks.EIP158, Forks["EIP158"]},
		{"Byzantium", types.LatestSignerForChainID(chainID), tt.Forks.Byzantium, Forks["Byzantium"]},
		{"Constantinople", types.LatestSignerForChainID(chainID), tt.Forks.Constantinople, Forks["Constantinople"]},
		{"ConstantinopleFix", types.LatestSignerForChainID(chainID), tt.Forks.ConstantinopleFix, Forks["ConstantinopleFix"]},
		{"Istanbul", types.LatestSignerForChainID(chainID), tt.Forks.Istanbul, Forks["Istanbul"]},
		{"Berlin", types.LatestSignerForChainID(chainID), tt.Forks.Berlin, Forks["Berlin"]},
		{"London", types.LatestSignerForChainID(chainID), tt.Forks.London, Forks["London"]},
	} {
		sender, txhash, intrinsicGas, err := validateTx(tt.RLP, *testcase.signer, testcase.config.Rules(0, 0))

		if testcase.fork.Exception != "" {
			if err == nil {
				return fmt.Errorf("expected error %v, got none [%v]", testcase.fork.Exception, testcase.name)
			}
			continue
		}
		// Should resolve the right address
		if err != nil {
			return fmt.Errorf("got error, expected none: %w", err)
		}
		if sender == nil {
			return fmt.Errorf("sender was nil, should be %x", testcase.fork.Sender)
		}
		if *sender != testcase.fork.Sender {
			return fmt.Errorf("sender mismatch: got %x, want %x", sender, testcase.fork.Sender)
		}
		if txhash == nil {
			return fmt.Errorf("txhash was nil, should be %x", testcase.fork.Hash)
		}
		if *txhash != testcase.fork.Hash {
			return fmt.Errorf("hash mismatch: got %x, want %x", *txhash, testcase.fork.Hash)
		}
		if new(big.Int).SetUint64(intrinsicGas).Cmp((*big.Int)(testcase.fork.IntrinsicGas)) != 0 {
			return fmt.Errorf("intrinsic gas mismatch: got %x, want %x", intrinsicGas, (*big.Int)(testcase.fork.IntrinsicGas))
		}
	}
	return nil
}
