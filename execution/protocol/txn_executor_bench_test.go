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

package protocol

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	"github.com/erigontech/erigon/execution/protocol/rules/ethash"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing"
	_ "github.com/erigontech/erigon/execution/tracing/tracers/native"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
)

// BenchmarkSysCallContract measures what a system call pays around the contract
// itself: block start and block end run these on the newPayload path, and each
// call built its own EVM before SysCallContractWithEVM existed. The EIP-4788
// beacon-roots predeploy is the shortest of the system contracts.
func BenchmarkSysCallContract(b *testing.B) {
	code := hexutil.MustDecode("0x3373fffffffffffffffffffffffffffffffffffffffe14604d57602036146024575f5ffd5b5f35801560495762001fff810690815414603c575f5ffd5b62001fff01545f5260205ff35b5f5ffd5b62001fff42064281555f359062001fff015500")
	contract := accounts.InternAddress(common.HexToAddress("0x000F3df6D732807Ef1319fB7B8bB8522d0Beac02"))
	root := common.HexToHash("0x0badc0de")
	header := &types.Header{
		Number:                *uint256.NewInt(25604144),
		Time:                  1755000000,
		Difficulty:            uint256.Int{},
		BaseFee:               uint256.NewInt(7),
		ParentBeaconBlockRoot: &root,
	}
	chainConfig := chainspec.Mainnet.Config
	engine := ethash.NewFaker()

	newIBS := func(b *testing.B) *state.IntraBlockState {
		b.Helper()
		ibs := state.New(state.NewNoopReader())
		require.NoError(b, ibs.CreateAccount(contract, false))
		require.NoError(b, ibs.SetCode(contract, code, tracing.CodeChangeUnspecified))
		return ibs
	}

	b.Run("evm_per_call", func(b *testing.B) {
		ibs := newIBS(b)
		b.ReportAllocs()
		for b.Loop() {
			if _, err := SysCallContract(contract, root[:], chainConfig, ibs, header, engine, false, vm.Config{}); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("evm_reused", func(b *testing.B) {
		ibs := newIBS(b)
		evm := NewSysCallEVM(chainConfig, vm.Config{})
		b.ReportAllocs()
		for b.Loop() {
			if _, err := SysCallContractWithEVM(evm, contract, root[:], chainConfig, ibs, header, engine, false, vm.Config{}); err != nil {
				b.Fatal(err)
			}
		}
	})
}
