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

package cocoon_test

import (
	"context"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cocoon"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/node/rulesconfig"
)

type stubStatefulPrecompile struct{}

func (stubStatefulPrecompile) RequiredGas([]byte) uint64        { return 0 }
func (stubStatefulPrecompile) Run(input []byte) ([]byte, error) { return input, nil }
func (stubStatefulPrecompile) Name() string                     { return "STUB" }

func (stubStatefulPrecompile) RunStateful(input []byte, gas mdgas.MdGas, _ *vm.PrecompileContext) ([]byte, mdgas.MdGas, error) {
	return input, gas, nil
}

func TestCreateRulesEngineBareBonesReturnsCocoonEngine(t *testing.T) {
	engine := rulesconfig.CreateRulesEngineBareBones(context.Background(), cocoon.DevChainConfig(990101), log.New())

	require.IsType(t, &cocoon.Engine{}, engine)
}

func TestEnableRegistersPrecompiles(t *testing.T) {
	const chainID = 990201
	stubAddr := accounts.InternAddress(common.BytesToAddress([]byte{0xEE}))
	ecrecoverAddr := accounts.InternAddress(common.BytesToAddress([]byte{0x01}))

	cfg := &chain.Config{ChainID: uint256.NewInt(chainID)}
	cocoon.Enable(cfg, func(*chain.Rules) vm.PrecompiledContracts {
		return vm.PrecompiledContracts{stubAddr: stubStatefulPrecompile{}}
	})
	t.Cleanup(func() { vm.UnregisterPrecompiles(chainID) })

	contracts := vm.Precompiles(&chain.Rules{ChainID: uint256.NewInt(chainID)})

	require.IsType(t, stubStatefulPrecompile{}, contracts[stubAddr])
	require.Contains(t, contracts, ecrecoverAddr, "base built-ins must still be present")
}

func TestEnableNilChainIDPanics(t *testing.T) {
	require.Panics(t, func() {
		cocoon.Enable(&chain.Config{}, nil)
	})
}
