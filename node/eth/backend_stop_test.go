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

	"github.com/c2h5oh/datasize"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/execution/chain/networkname"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	"github.com/erigontech/erigon/execution/state/genesiswrite"
	"github.com/erigontech/erigon/node"
	"github.com/erigontech/erigon/node/ethconfig"
	"github.com/erigontech/erigon/node/nodecfg"
	"github.com/erigontech/erigon/txnprovider/txpool/txpoolcfg"
)

// TestStopClosesTxPoolDBWithoutStart pins that Stop releases the txpool DB
// even when the pool's Run loop — which normally owns closing it — never ran
// because the backend was never started.
func TestStopClosesTxPoolDBWithoutStart(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	ctx := context.Background()
	logger := log.New()
	dirs := datadir.New(t.TempDir())

	stack, err := node.New(ctx, &nodecfg.Config{
		Dirs:            dirs,
		DisableSentry:   true,
		MdbxDBSizeLimit: 1 * datasize.GB,
	}, logger)
	require.NoError(t, err)
	defer stack.Close()

	chainDB, err := node.OpenDatabase(ctx, stack.Config(), dbcfg.ChainDB, "", false, logger)
	require.NoError(t, err)
	// Copy the spec genesis: it lacks Difficulty, which the stored-genesis
	// JSON round-trip in New requires.
	genesis := *chainspec.Test.Genesis
	genesis.Difficulty = uint256.NewInt(0)
	_, _, err = genesiswrite.CommitGenesisBlock(chainDB, &genesis, networkname.Test, dirs, logger)
	require.NoError(t, err)
	chainDB.Close()

	txPoolConfig := txpoolcfg.DefaultConfig
	txPoolConfig.DBDir = dirs.TxPool
	syncCfg := ethconfig.Defaults.Sync
	syncCfg.ParallelStateFlushing = false

	backend, err := New(ctx, stack, &ethconfig.Config{
		Sync:                  syncCfg,
		Dirs:                  dirs,
		Snapshot:              ethconfig.BlocksFreezing{NoDownloader: true},
		TxPool:                txPoolConfig,
		BatchSize:             512 * datasize.MB,
		KeepStoredChainConfig: true,
		StateCacheBudget:      1 * datasize.MB,
	}, logger, nil)
	require.NoError(t, err)
	require.NoError(t, backend.Stop())

	poolDB, err := mdbx.New(dbcfg.TxPoolDB, logger).Path(dirs.TxPool).Accede(true).Readonly(true).Open(ctx)
	require.NoError(t, err, "txpool DB still open after Stop")
	poolDB.Close()
}
