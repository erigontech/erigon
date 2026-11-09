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

package txpool_test

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/kvcache"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/execution/chain"
	txpoolcomp "github.com/erigontech/erigon/node/components/txpool"
	"github.com/erigontech/erigon/txnprovider/txpool"
	"github.com/erigontech/erigon/txnprovider/txpool/txpoolcfg"
)

func TestInitializeDisabled(t *testing.T) {
	cfg := txpoolcfg.DefaultConfig
	cfg.Disable = true

	var p txpoolcomp.Provider
	require.NoError(t, p.Initialize(txpoolcomp.Deps{Ctx: context.Background(), Config: cfg, Logger: log.New()}))

	require.Nil(t, p.Pool)
	require.IsType(t, &txpool.GrpcDisabled{}, p.GrpcServer)
}

func TestInitializeAssemblesPool(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	coreDB := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	seedChainConfig(t, ctx, coreDB, chain.AllProtocolChanges)

	var p txpoolcomp.Provider
	require.NoError(t, p.Initialize(txpoolcomp.Deps{
		Ctx:           ctx,
		Config:        txpoolcfg.DefaultConfig,
		ChainDB:       coreDB,
		Cache:         kvcache.New(kvcache.DefaultCoherentConfig),
		NotifyNewTxns: func() {},
		Logger:        log.New(),
		Options: []txpool.Option{
			txpool.WithFeeCalculator(nil),
			txpool.WithPoolDBInitializer(func(context.Context, txpoolcfg.Config, log.Logger) (kv.RwDB, error) {
				return mdbxtest.NewTestPoolDB(t), nil
			}),
		},
	}))

	require.NotNil(t, p.Pool)
	require.NotNil(t, p.GrpcServer)
}

func seedChainConfig(t *testing.T, ctx context.Context, db kv.TemporalRwDB, cfg *chain.Config) {
	t.Helper()
	data, err := json.Marshal(cfg)
	require.NoError(t, err)
	genesisHash := common.HexToHash("0x01")
	var key [8]byte
	binary.BigEndian.PutUint64(key[:], 0)
	require.NoError(t, db.Update(ctx, func(tx kv.RwTx) error {
		if err := tx.Put(kv.HeaderCanonical, key[:], genesisHash[:]); err != nil {
			return err
		}
		return tx.Put(kv.ConfigTable, genesisHash[:], data)
	}))
}
