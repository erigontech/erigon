// Copyright 2026 The Erigon Authors
// SPDX-License-Identifier: LGPL-3.0

package vm

import (
	"testing"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
	"github.com/holiman/uint256"
)

func TestContextAddressWordsReusedFrame(t *testing.T) {
	evm := &EVM{}
	addresses := []accounts.Address{accounts.NilAddress, accounts.ZeroAddress, accounts.InternAddress(common.HexToAddress("0x1234567890abcdef1234567890abcdef12345678")), accounts.InternAddress(common.HexToAddress("0xabcdef0123456789abcdef0123456789abcdef01"))}
	for _, address := range addresses {
		evm.TxContext = evmtypes.TxContext{Origin: address}
		evm.Context = evmtypes.BlockContext{Coinbase: address}
		scope := evm.getCallContext(*NewContract(address, address, address, uint256.Int{}), nil, mdgas.MdGas{})
		for _, op := range []executionFunc{opAddress, opOrigin, opCaller, opCoinbase} {
			for range 2 {
				_, _, err := op(0, evm, scope)
				value := address.Value()
				want := new(uint256.Int).SetBytes(value[:])
				if err != nil || *scope.Stack.peek() != *want {
					t.Fatalf("got %x, %v; want %x", scope.Stack.peek(), err, want)
				}
				scope.Stack.Reset()
			}
		}
		evm.putCallContext(scope)
	}
}
