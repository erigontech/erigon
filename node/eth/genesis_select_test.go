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

package eth

import (
	"context"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/state/genesiswrite"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/tracing/tracers"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/ethconfig"
)

func testGenesis(chainID uint64, gasLimit uint64) *types.Genesis {
	return &types.Genesis{
		Config:   &chain.Config{ChainID: uint256.NewInt(chainID)},
		GasLimit: gasLimit,
	}
}

// startGenesis runs this start's genesis selection and write on the DB, as New does.
func startGenesis(t *testing.T, db kv.RwDB, config *ethconfig.Config) (*chain.Config, *types.Block, error) {
	t.Helper()
	return setupGenesis(db, config, datadir.New(t.TempDir()), log.New(), nil)
}

// A restart with --override.genesis on a datadir initialised with another genesis
// must fail, not run the stored chain under the new file's network id.
func TestOverrideGenesisRestart(t *testing.T) {
	a := testGenesis(1337, 30_000_000)
	b := testGenesis(1338, 31_000_000)
	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))

	_, first, err := startGenesis(t, db, &ethconfig.Config{Genesis: a, GenesisFromFile: true})
	require.NoError(t, err)

	t.Run("same genesis", func(t *testing.T) {
		_, block, err := startGenesis(t, db, &ethconfig.Config{Genesis: a, GenesisFromFile: true})
		require.NoError(t, err)
		require.Equal(t, first.Hash(), block.Hash())
	})

	t.Run("different genesis", func(t *testing.T) {
		for _, keepStoredChainConfig := range []bool{false, true} {
			tracerInitialized := false
			tracer := &tracers.Tracer{Hooks: &tracing.Hooks{
				OnBlockchainInit: func(*chain.Config) { tracerInitialized = true },
			}}
			_, _, err := setupGenesis(db, &ethconfig.Config{
				Genesis:               b,
				GenesisFromFile:       true,
				KeepStoredChainConfig: keepStoredChainConfig,
			}, datadir.New(t.TempDir()), log.New(), tracer)
			var mismatch *genesiswrite.GenesisMismatchError
			require.ErrorAs(t, err, &mismatch)
			require.Equal(t, first.Hash(), mismatch.Stored)
			require.False(t, tracerInitialized, "rejected genesis must not initialize the tracer")
		}
	})

	t.Run("different chain ID with stored config", func(t *testing.T) {
		changed := testGenesis(1338, a.GasLimit)
		_, _, err := startGenesis(t, db, &ethconfig.Config{Genesis: changed, GenesisFromFile: true, KeepStoredChainConfig: true})
		require.ErrorContains(t, err, "chain ID")
		require.NoError(t, db.View(context.Background(), func(tx kv.Tx) error {
			stored, err := rawdb.ReadChainConfig(tx, first.Hash())
			require.NoError(t, err)
			require.Equal(t, a.Config.ChainID, stored.ChainID)
			return nil
		}))
	})

	// Without --override.genesis the stored genesis still wins, as before.
	t.Run("chain default", func(t *testing.T) {
		config := &ethconfig.Config{Genesis: b}
		cfg, block, err := startGenesis(t, db, config)
		require.NoError(t, err)
		require.Equal(t, first.Hash(), block.Hash())
		require.Equal(t, uint64(1337), cfg.ChainID.Uint64())
		require.Equal(t, a.GasLimit, config.Genesis.GasLimit)
	})
}

func TestOverrideGenesisKeepsStoredChainConfig(t *testing.T) {
	genesis := testGenesis(1337, 30_000_000)
	genesis.Config.OsakaTime = common.NewUint64(100)
	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	storedCfg, first, err := startGenesis(t, db, &ethconfig.Config{Genesis: genesis, GenesisFromFile: true})
	require.NoError(t, err)

	changed := *genesis
	changed.Config = genesis.Config.Copy()
	changed.Config.OsakaTime = common.NewUint64(200)
	var tracedConfig *chain.Config
	tracer := &tracers.Tracer{Hooks: &tracing.Hooks{
		OnBlockchainInit: func(cfg *chain.Config) { tracedConfig = cfg.Copy() },
	}}
	cfg, block, err := setupGenesis(db, &ethconfig.Config{
		Genesis:               &changed,
		GenesisFromFile:       true,
		KeepStoredChainConfig: true,
	}, datadir.New(t.TempDir()), log.New(), tracer)
	require.NoError(t, err)
	require.Equal(t, first.Hash(), block.Hash())
	require.Equal(t, storedCfg, cfg)
	require.Equal(t, cfg, tracedConfig, "tracer must receive the selected configuration")
	require.NoError(t, db.View(context.Background(), func(tx kv.Tx) error {
		persisted, err := rawdb.ReadChainConfig(tx, first.Hash())
		require.NoError(t, err)
		require.Equal(t, storedCfg, persisted)
		return nil
	}))
}

func TestOverrideGenesisCompatibilityErrorRollsBack(t *testing.T) {
	genesis := testGenesis(1337, 30_000_000)
	genesis.Config.HomesteadBlock = common.NewUint64(0)
	genesis.Config.TangerineWhistleBlock = common.NewUint64(0)
	genesis.Config.SpuriousDragonBlock = common.NewUint64(0)
	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	storedCfg, block, err := startGenesis(t, db, &ethconfig.Config{Genesis: genesis, GenesisFromFile: true})
	require.NoError(t, err)

	head := block.Header()
	head.ParentHash = block.Hash()
	head.Number.SetUint64(1)
	head.Time = 1
	require.NoError(t, db.Update(context.Background(), func(tx kv.RwTx) error {
		require.NoError(t, tx.Delete(kv.ConfigTable, kv.GenesisKey))
		require.NoError(t, rawdb.WriteHeader(tx, head))
		require.NoError(t, rawdb.WriteCanonicalHash(tx, head.Hash(), head.Number.Uint64()))
		return rawdb.WriteHeadHeaderHash(tx, head.Hash())
	}))

	changed := *genesis
	changed.Config = genesis.Config.Copy()
	changed.Config.ChainID = uint256.NewInt(1338)
	_, _, err = startGenesis(t, db, &ethconfig.Config{Genesis: &changed, GenesisFromFile: true})
	var compatErr *chain.ConfigCompatError
	require.ErrorAs(t, err, &compatErr)
	require.Equal(t, "EIP155 chain ID", compatErr.What)
	require.ErrorContains(t, err, "incompatible chain config")
	require.NoError(t, db.View(context.Background(), func(tx kv.Tx) error {
		persisted, err := rawdb.ReadChainConfig(tx, block.Hash())
		require.NoError(t, err)
		require.Equal(t, storedCfg, persisted)
		hasGenesis, err := tx.Has(kv.ConfigTable, kv.GenesisKey)
		require.NoError(t, err)
		require.False(t, hasGenesis, "failed startup must not save the rejected genesis specification")
		return nil
	}))
}

func TestOverrideGenesisRestartWithForkOverride(t *testing.T) {
	genesis := &types.Genesis{
		Config:     chain.TestChainOsakaConfig.Copy(),
		GasLimit:   30_000_000,
		Difficulty: uint256.NewInt(0),
	}
	config := &ethconfig.Config{
		Genesis:               genesis,
		GenesisFromFile:       true,
		OverrideAmsterdamTime: common.NewUint64(0),
		AllowAA:               true,
	}
	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	var tracedConfig *chain.Config
	tracer := &tracers.Tracer{Hooks: &tracing.Hooks{
		OnBlockchainInit: func(cfg *chain.Config) { tracedConfig = cfg.Copy() },
	}}

	cfg, first, err := setupGenesis(db, config, datadir.New(t.TempDir()), log.New(), tracer)
	require.NoError(t, err)
	require.NotNil(t, first.Header().SlotNumber)
	require.Nil(t, genesis.Config.AmsterdamTime)
	require.Equal(t, cfg, tracedConfig, "tracer must receive the overridden configuration")
	require.True(t, tracedConfig.AllowAA)

	tracedConfig = nil
	cfg, block, err := setupGenesis(db, config, datadir.New(t.TempDir()), log.New(), tracer)
	require.NoError(t, err)
	require.Equal(t, first.Hash(), block.Hash())
	require.Nil(t, genesis.Config.AmsterdamTime)
	require.Equal(t, cfg, tracedConfig, "tracer must receive the overridden configuration on restart")
	require.True(t, tracedConfig.AllowAA)
}
