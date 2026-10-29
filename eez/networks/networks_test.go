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
// deployments' README.md and, for 699069, fixture eezdev-699069; for
// 699169 and 916969 the hashes are their RollupCreated initial block hashes.
func TestRollupXGenesis(t *testing.T) {
	for _, spec := range []chainspec.Spec{Chiado699169, Chiado916969, Chiado699069, Chiado906969, Mainnet} {
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
			require.Equal(t, l2.RollupX.FeeCollector, spec.Config.FeeCollector)

			codeHash, err := ibs.GetCodeHash(accounts.InternAddress(protocol.RollupXEEZL2Address))
			require.NoError(t, err)
			require.False(t, codeHash.IsEmpty())
		})
	}
}

// TestRollupXV01Genesis checks what the v0.1 rule set adds to a genesis:
// the fee collector, the history schedule and the L1 history predeploy,
// empty, behind its proxy.
func TestRollupXV01Genesis(t *testing.T) {
	for _, spec := range []chainspec.Spec{Chiado699169, Chiado916969} {
		t.Run(spec.Name, func(t *testing.T) {
			l2 := spec.Config.L2.(*config.L2Config)
			require.Equal(t, common.HexToAddress("0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee0078"), *l2.RollupX.FeeCollector)
			require.Equal(t, uint64(2), *l2.RollupX.HistoryUpdateOffset)
			require.Equal(t, accounts.InternAddress(*l2.RollupX.FeeCollector), spec.Config.GetFeeCollector())

			history, ok := spec.Genesis.Alloc[protocol.HistoryAddress]
			require.True(t, ok)
			require.Equal(t, uint64(2), history.Nonce)
			require.Equal(t, map[common.Hash]common.Hash{
				common.HexToHash("0x360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc"): common.BytesToHash(common.FromHex("0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee007a")),
				common.HexToHash("0xb53127684a568b3173ae13b9f8a6016e243e63b6e8ee1178d6a717850b5d6103"): common.BytesToHash(common.FromHex("0x58ad165846a897f4aba3ec8f07a4a7db434324bf")),
			}, history.Storage)
			require.Zero(t, history.Balance.Sign())
		})
	}
}

func TestRollupXRegistered(t *testing.T) {
	Register()
	for name, want := range map[string]struct{ id, ruleSet uint64 }{
		"eez-chiado":        {699169, config.RollupXV01},
		"eez-chiado-asus":   {916969, config.RollupXV01},
		"eez-chiado-699069": {699069, config.RollupX},
		"eez-chiado-906969": {906969, config.RollupX},
		"eez-mainnet":       {696990, config.RollupX},
	} {
		spec, err := chainspec.ChainSpecByName(name)
		require.NoError(t, err)
		require.Equal(t, want.id, spec.Config.ChainID.Uint64())
		require.Equal(t, name, chainspec.NetworkNameByID[want.id])
		require.Equal(t, want.ruleSet, spec.Config.L2.(*config.L2Config).RuleSet())
	}
}
