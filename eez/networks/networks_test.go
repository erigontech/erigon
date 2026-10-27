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
package networks

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/eez/config"
	"github.com/erigontech/erigon/eez/protocol"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	"github.com/erigontech/erigon/execution/state/genesiswrite"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// T-GEN-1: the state root and EEZL2 code hash are those of
// testing/kurtosis/l2-genesis-profile.json; the genesis hash was recomputed
// outside Erigon from the header fields reth derives for this genesis.
func TestDevnetGenesis(t *testing.T) {
	block, ibs, err := genesiswrite.GenesisToBlock(Devnet.Genesis, datadir.New(t.TempDir()), log.New())
	require.NoError(t, err)
	require.Equal(t, common.HexToHash("0xb015fd4359695a92c09e587870e1c04920dcf308d4e6649259de5596c77de75e"), block.Root())
	require.Equal(t, Devnet.GenesisStateRoot, block.Root())
	require.Equal(t, Devnet.GenesisHash, block.Hash())

	codeHash, err := ibs.GetCodeHash(accounts.InternAddress(protocol.EEZL2Address))
	require.NoError(t, err)
	require.Equal(t, common.HexToHash("0x958bcd64899023e3ab7cea07a73c4f8e65731f3df69d640a5d91737377d976ed"), codeHash.Value())
}

func TestDevnetRegistered(t *testing.T) {
	_, err := chainspec.ChainSpecByName("eez-devnet")
	require.ErrorIs(t, err, chainspec.ErrChainSpecUnknown)
	Register()
	Register()
	spec, err := chainspec.ChainSpecByName("eez-devnet")
	require.NoError(t, err)
	require.Same(t, Devnet.Genesis.Config, spec.Config)
	require.Same(t, spec.Config, spec.Genesis.Config)
	require.Equal(t, "eez-devnet", chainspec.NetworkNameByID[6290])

	cfg := spec.Config
	require.Equal(t, uint64(6290), cfg.ChainID.Uint64())
	require.True(t, chainspec.IsChainPoS(cfg, func() *uint256.Int { return nil }))
	require.Equal(t, accounts.InternAddress(common.HexToAddress("0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266")), cfg.GetFeeCollector())

	l2, ok := cfg.L2.(*config.L2Config)
	require.True(t, ok)
	require.NoError(t, l2.Validate())
	require.Equal(t, uint64(6), l2.BlocksPerL1Block())
	require.Equal(t, cfg.L2.Name(), config.Name)
	require.Equal(t, l2.BlockGasLimit, spec.Genesis.GasLimit)
}

// TestRollupXGenesis is T-GEN-1 for the eez-rollupX networks: the hashes and
// state roots are those the networks serve as block 0, recorded in the
// deployments' README.md and, for eez-chiado, fixture eezdev-699069.
func TestRollupXGenesis(t *testing.T) {
	for _, spec := range []chainspec.Spec{Chiado, ChiadoAsus, Mainnet} {
		t.Run(spec.Name, func(t *testing.T) {
			block, ibs, err := genesiswrite.GenesisToBlock(spec.Genesis, datadir.New(t.TempDir()), log.New())
			require.NoError(t, err)
			require.Equal(t, spec.GenesisHash, block.Hash())
			require.Equal(t, spec.GenesisStateRoot, block.Root())

			l2 := spec.Config.L2.(*config.L2Config)
			require.NoError(t, l2.RollupX.CheckGenesis(block.Header()))
			require.Equal(t, config.RollupXBlockGasLimit, block.GasLimit())
			require.Equal(t, uint64(1_000_000_000), block.BaseFee().Uint64())
			require.Equal(t, config.RollupXBaseFeeChangeDenominator, *spec.Config.BaseFeeChangeDenominator)
			require.Nil(t, spec.Config.FeeCollector)

			codeHash, err := ibs.GetCodeHash(accounts.InternAddress(protocol.RollupXEEZL2Address))
			require.NoError(t, err)
			require.False(t, codeHash.IsEmpty())
		})
	}
}

func TestRollupXRegistered(t *testing.T) {
	Register()
	for name, id := range map[string]uint64{"eez-chiado": 699069, "eez-chiado-asus": 906969, "eez-mainnet": 696990} {
		spec, err := chainspec.ChainSpecByName(name)
		require.NoError(t, err)
		require.Equal(t, id, spec.Config.ChainID.Uint64())
		require.Equal(t, name, chainspec.NetworkNameByID[id])
		require.Equal(t, config.RollupX, spec.Config.L2.(*config.L2Config).RuleSet())
	}
}
