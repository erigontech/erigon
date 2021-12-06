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
	"fmt"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/params"
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
	Exception string         `json:"exception"`
	Sender    common.Address `json:"sender"`
	Hash      common.Hash    `json:"hash"`
}

func (tt *TransactionTest) Run(config *params.ChainConfig) error {
	validateTx := func(rlpData hexutil.Bytes, signer types.Signer, isHomestead bool, isIstanbul bool) (*common.Address, *common.Hash, error) {
		tx, err := types.DecodeTransaction(rlp.NewStream(bytes.NewReader(rlpData), 0))
		if err != nil {
			return nil, nil, err
		}
		sender, err := tx.Sender(signer)
		if err != nil {
			return nil, nil, err
		}
		// Intrinsic gas
		requiredGas, err := core.IntrinsicGas(tx.GetData(), tx.GetAccessList(), tx.GetTo() == nil, isHomestead, isIstanbul)
		if err != nil {
			return nil, nil, err
		}
		if requiredGas > tx.GetGas() {
			return nil, nil, fmt.Errorf("insufficient gas ( %d < %d )", tx.GetGas(), requiredGas)
		}
		h := tx.Hash()
		return &sender, &h, nil
	}

	for _, testcase := range []struct {
		name        string
		signer      *types.Signer
		fork        ttFork
		isHomestead bool
		isIstanbul  bool
	}{
		{"Frontier", types.MakeFrontierSigner(), tt.Forks.Frontier, false, false},
		{"Homestead", types.LatestSignerForChainID(nil), tt.Forks.Homestead, true, false},
		{"EIP150", types.LatestSignerForChainID(nil), tt.Forks.EIP150, true, false},
		{"EIP158", types.LatestSignerForChainID(config.ChainID), tt.Forks.EIP158, true, false},
		{"Byzantium", types.LatestSignerForChainID(config.ChainID), tt.Forks.Byzantium, true, false},
		{"Constantinople", types.LatestSignerForChainID(config.ChainID), tt.Forks.Constantinople, true, false},
		{"ConstantinopleFix", types.LatestSignerForChainID(config.ChainID), tt.Forks.ConstantinopleFix, true, false},
		{"Istanbul", types.LatestSignerForChainID(config.ChainID), tt.Forks.Istanbul, true, true},
		{"Berlin", types.LatestSignerForChainID(config.ChainID), tt.Forks.Berlin, true, true},
		{"London", types.LatestSignerForChainID(config.ChainID), tt.Forks.London, true, true},
	} {
		sender, txhash, err := validateTx(tt.RLP, *testcase.signer, testcase.isHomestead, testcase.isIstanbul)

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
			return fmt.Errorf("sender was nil, should be %x", common.Address(testcase.fork.Sender))
		}
		if *sender != common.Address(testcase.fork.Sender) {
			return fmt.Errorf("sender mismatch: got %x, want %x", sender, testcase.fork.Sender)
		}
		if txhash == nil {
			return fmt.Errorf("txhash was nil, should be %x", common.Hash(testcase.fork.Hash))
		}
		if *txhash != common.Hash(testcase.fork.Hash) {
			return fmt.Errorf("hash mismatch: got %x, want %x", *txhash, testcase.fork.Hash)
		}
	}
	return nil
}
