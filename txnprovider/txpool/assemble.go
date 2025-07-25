// Copyright 2024 The Erigon Authors
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

package txpool

import (
	"context"
	"math/big"

	"github.com/c2h5oh/datasize"
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon-lib/gointerfaces/sentryproto"
	"github.com/erigontech/erigon-lib/gointerfaces/txpoolproto"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/kvcache"
	"github.com/erigontech/erigon-lib/kv/mdbx"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/txnprovider/txpool/txpoolcfg"
)

func Assemble(
	ctx context.Context,
	cfg txpoolcfg.Config,
	chainDB kv.RwDB,
	cache kvcache.Cache,
	sentryClients []sentryproto.SentryClient,
	stateChangesClient StateChangesClient,
	builderNotifyNewTxns func(),
	logger log.Logger,
	opts ...Option,
) (*TxPool, txpoolproto.TxpoolServer, error) {
	options := applyOpts(opts...)
	poolDB, err := options.poolDBInitializer(ctx, cfg, logger)
	if err != nil {
		return nil, nil, err
	}

	chainConfig, _, err := SaveChainConfigIfNeed(ctx, chainDB, poolDB, true, logger)
	if err != nil {
		return nil, nil, err
	}

	chainID, _ := uint256.FromBig(chainConfig.ChainID)

	shanghaiTime := chainConfig.ShanghaiTime
	var agraBlock *big.Int
	var bhilaiBlock *big.Int
	if chainConfig.Bor != nil {
		agraBlock = chainConfig.Bor.GetAgraBlock()
		bhilaiBlock = chainConfig.Bor.GetBhilaiBlock()
	}
	cancunTime := chainConfig.CancunTime
	pragueTime := chainConfig.PragueTime
	if cfg.OverridePragueTime != nil {
		pragueTime = cfg.OverridePragueTime
	}

	newTxns := make(chan Announcements, 1024)
	newSlotsStreams := &NewSlotsStreams{}
	pool, err := New(
		ctx,
		newTxns,
		poolDB,
		chainDB,
		cfg,
		cache,
		*chainID,
		shanghaiTime,
		agraBlock,
		bhilaiBlock,
		cancunTime,
		pragueTime,
		chainConfig.BlobSchedule,
		sentryClients,
		stateChangesClient,
		builderNotifyNewTxns,
		newSlotsStreams,
		logger,
		opts...,
	)
	if err != nil {
		return nil, nil, err
	}

	grpcServer := NewGrpcServer(ctx, pool, poolDB, newSlotsStreams, *chainID, logger)
	return pool, grpcServer, nil
}

type poolDBInitializer func(ctx context.Context, cfg txpoolcfg.Config, logger log.Logger) (kv.RwDB, error)

var defaultPoolDBInitializer = func(ctx context.Context, cfg txpoolcfg.Config, logger log.Logger) (kv.RwDB, error) {
	opts := mdbx.New(kv.TxPoolDB, logger).
		Path(cfg.DBDir).
		WithTableCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg { return kv.TxpoolTablesCfg }).
		WriteMergeThreshold(3 * 8192).
		PageSize(16 * datasize.KB).
		GrowthStep(16 * datasize.MB).
		DirtySpace(uint64(128 * datasize.MB)).
		MapSize(1 * datasize.TB).
		WriteMap(cfg.MdbxWriteMap)
	if cfg.MdbxPageSize > 0 {
		opts = opts.PageSize(cfg.MdbxPageSize)
	}
	if cfg.MdbxDBSizeLimit > 0 {
		opts = opts.MapSize(cfg.MdbxDBSizeLimit)
	}
	if cfg.MdbxGrowthStep > 0 {
		opts = opts.GrowthStep(cfg.MdbxGrowthStep)
	}
	return opts.Open(ctx)
}
