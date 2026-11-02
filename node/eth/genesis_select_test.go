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
	var (
		cfg   *chain.Config
		block *types.Block
	)
	err := db.Update(context.Background(), func(tx kv.RwTx) error {
		spec, err := selectGenesis(tx, config)
		if err != nil {
			return err
		}
		cfg, block, err = genesiswrite.WriteGenesisBlock(tx, spec, "", config.OverrideOsakaTime, config.OverrideAmsterdamTime, config.KeepStoredChainConfig, datadir.New(t.TempDir()), log.New())
		return err
	})
	return cfg, block, err
}

// A restart with --override.genesis on a datadir initialised with another genesis
// must fail, not run the stored chain under the new file's network id.
func TestOverrideGenesisRestart(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}
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
			_, _, err := startGenesis(t, db, &ethconfig.Config{Genesis: b, GenesisFromFile: true, KeepStoredChainConfig: keepStoredChainConfig})
			var mismatch *genesiswrite.GenesisMismatchError
			require.ErrorAs(t, err, &mismatch)
			require.Equal(t, first.Hash(), mismatch.Stored)
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
	cfg, block, err := startGenesis(t, db, &ethconfig.Config{
		Genesis:               &changed,
		GenesisFromFile:       true,
		KeepStoredChainConfig: true,
	})
	require.NoError(t, err)
	require.Equal(t, first.Hash(), block.Hash())
	require.Equal(t, storedCfg, cfg)
	require.NoError(t, db.View(context.Background(), func(tx kv.Tx) error {
		persisted, err := rawdb.ReadChainConfig(tx, first.Hash())
		require.NoError(t, err)
		require.Equal(t, storedCfg, persisted)
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
	}
	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))

	_, first, err := startGenesis(t, db, config)
	require.NoError(t, err)
	require.NotNil(t, first.Header().SlotNumber)
	require.Nil(t, genesis.Config.AmsterdamTime)

	_, block, err := startGenesis(t, db, config)
	require.NoError(t, err)
	require.Equal(t, first.Hash(), block.Hash())
	require.Nil(t, genesis.Config.AmsterdamTime)
}
