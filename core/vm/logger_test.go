// Copyright 2016 The go-ethereum Authors
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

package vm

import (
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon/core/vm/evmtypes"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/vm/stack"
	"github.com/ledgerwatch/erigon/params"
)

func TestStoreCapture(t *testing.T) {
	var (
		env      = NewEVM(evmtypes.BlockContext{}, evmtypes.TxContext{}, &dummyStatedb{}, params.TestChainConfig, Config{})
		logger   = NewStructLogger(nil)
		mem      = NewMemory()
		stack    = stack.New()
		contract = NewContract(&dummyContractRef{}, &dummyContractRef{}, new(uint256.Int), 0, false /* skipAnalysis */)
	)
	stack.Push(uint256.NewInt(1))
	stack.Push(uint256.NewInt(0))
	var index common.Hash
	logger.CaptureStart(env, common.Address{}, common.Address{}, false, false, nil, 0, nil, nil)
	logger.CaptureState(0, SSTORE, 0, 0, &ScopeContext{
		Memory:   mem,
		Stack:    stack,
		Contract: contract,
	}, nil, 0, nil)

	if len(logger.storage[contract.Address()]) == 0 {
		t.Fatalf("expected exactly 1 changed value on address %x, got %d", contract.Address(), len(logger.storage[contract.Address()]))
	}
	exp := common.BigToHash(big.NewInt(1))
	if logger.storage[contract.Address()][index] != exp {
		t.Errorf("expected %x, got %x", exp, logger.storage[contract.Address()][index])
	}
}
