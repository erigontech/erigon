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

func BenchmarkContextAddress(b *testing.B) {
	address := accounts.InternAddress(common.HexToAddress("0x1234567890abcdef1234567890abcdef12345678"))
	for _, tc := range []struct {
		name string
		op   executionFunc
	}{{"address", opAddress}, {"caller", opCaller}, {"origin", opOrigin}, {"coinbase", opCoinbase}} {
		b.Run(tc.name, func(b *testing.B) {
			evm := &EVM{TxContext: evmtypes.TxContext{Origin: address}, Context: evmtypes.BlockContext{Coinbase: address}}
			scope := evm.getCallContext(*NewContract(address, address, address, uint256.Int{}), nil, mdgas.MdGas{})
			defer evm.putCallContext(scope)
			b.ReportAllocs()
			for b.Loop() {
				if _, _, err := tc.op(0, evm, scope); err != nil {
					b.Fatal(err)
				}
				scope.Stack.Reset()
			}
		})
	}
}
