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

package vm_test

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
)

func TestGetVMContextHandsOutADeepRulesSnapshot(t *testing.T) {
	cfg := &chain.Config{
		ChainID:             uint256.NewInt(1337),
		HomesteadBlock:      common.NewUint64(0),
		SpuriousDragonBlock: common.NewUint64(0),
		DisabledEIPs:        []int{170},
	}
	evm := vm.NewEVM(evmtypes.BlockContext{}, evmtypes.TxContext{}, nil, cfg, vm.Config{})

	live := evm.ChainRules()
	require.True(t, live.IsSpuriousDragon, "the fixture must start with the flag set, or the mutation below proves nothing")
	require.NotNil(t, live.ChainID, "the fixture must carry a chain id, or the mutation below proves nothing")
	require.Equal(t, []int{170}, live.DisabledEIPs, "the fixture must carry a disabled eip, or the mutation below proves nothing")
	chainID := live.ChainID.Clone()

	env := evm.GetVMContext()
	env.Rules.IsSpuriousDragon = false
	env.Rules.ChainID.SetUint64(0xdead)
	env.Rules.DisabledEIPs[0] = 161

	require.True(t, evm.ChainRules().IsSpuriousDragon,
		"OnTxStart runs before execution, so a tracer holding the VMContext must not be able to clear a live fork flag")
	require.Equal(t, chainID, evm.ChainRules().ChainID,
		"CHAINID pushes evm.ChainRules().ChainID, so a tracer must not reach it through the snapshot")
	require.Equal(t, []int{170}, evm.ChainRules().DisabledEIPs,
		"Rules.IsEIPEnabled gates forks on DisabledEIPs, so the snapshot must not share its backing array")
}
