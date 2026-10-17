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
	"maps"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
)

type overlayPrecompile struct{ name string }

func (o overlayPrecompile) RequiredGas([]byte) uint64        { return 3000 }
func (o overlayPrecompile) Run(input []byte) ([]byte, error) { return input, nil }
func (o overlayPrecompile) Name() string                     { return o.name }

func chapelConfig() *chain.Config {
	return &chain.Config{
		ChainID:               uint256.NewInt(97),
		HomesteadBlock:        common.NewUint64(0),
		TangerineWhistleBlock: common.NewUint64(0),
		SpuriousDragonBlock:   common.NewUint64(0),
		ByzantiumBlock:        common.NewUint64(0),
		ConstantinopleBlock:   common.NewUint64(0),
		PetersburgBlock:       common.NewUint64(0),
		IstanbulBlock:         common.NewUint64(0),
		BerlinBlock:           common.NewUint64(31103030),
		LondonBlock:           common.NewUint64(31103030),
		ShanghaiTime:          common.NewUint64(1702972800),
		CancunTime:            common.NewUint64(1713330442),
		PragueTime:            common.NewUint64(1740452880),
	}
}

func TestRegisteredOverlayInheritsForkTier(t *testing.T) {
	const chapelChainID = 97
	tmHeader := accounts.InternAddress(common.BytesToAddress([]byte{0x64}))
	iavlProof := accounts.InternAddress(common.BytesToAddress([]byte{0x65}))
	modExp := accounts.InternAddress(common.BytesToAddress([]byte{0x05}))
	pointEval := accounts.InternAddress(common.BytesToAddress([]byte{0x0a}))

	overlay := vm.PrecompiledContracts{
		tmHeader:  overlayPrecompile{"TMHeaderValidate"},
		iavlProof: overlayPrecompile{"IAVLMerkleProofValidate"},
	}
	vm.RegisterPrecompiles(uint256.NewInt(chapelChainID), func(uint64) vm.PrecompiledContracts {
		return maps.Clone(overlay)
	})
	t.Cleanup(func() { vm.UnregisterPrecompiles(uint256.NewInt(chapelChainID)) })

	cfg := chapelConfig()
	for _, tc := range []struct {
		name     string
		blockNum uint64
		time     uint64
		base     vm.PrecompiledContracts
	}{
		{"genesis is the istanbul tier", 0, 0, vm.PrecompiledContractsIstanbul},
		{"berlin tier at berlinBlock", 31103030, 1690000000, vm.PrecompiledContractsBerlin},
		{"cancun tier at cancunTime", 40000000, 1720000000, vm.PrecompiledContractsCancun},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bc := evmtypes.BlockContext{BlockNumber: tc.blockNum, Time: tc.time}
			got := vm.Precompiles(bc.Rules(cfg))

			want := maps.Clone(tc.base)
			maps.Copy(want, overlay)
			require.Equal(t, want, got)

			require.Equal(t, tc.base[modExp], got[modExp],
				"the standard precompiles must come from the active fork tier, not a pinned one")
		})
	}

	cancunBC := evmtypes.BlockContext{BlockNumber: 40000000, Time: 1720000000}
	got := vm.Precompiles(cancunBC.Rules(cfg))
	require.Contains(t, got, pointEval, "crossing Cancun must bring 0x0a with it")
	require.Contains(t, got, tmHeader, "the chain overlay must survive the fork crossing")

	mainnet := cancunBC.Rules(&chain.Config{ChainID: uint256.NewInt(1), CancunTime: common.NewUint64(0)})
	require.NotContains(t, vm.Precompiles(mainnet), tmHeader, "the overlay must not leak to another chain")
}
