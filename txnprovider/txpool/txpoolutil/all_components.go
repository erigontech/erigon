// Copyright 2021 The Erigon Authors
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

package txpoolutil

import (
	"context"
	"errors"
	"math/big"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/gointerfaces/sentryproto"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/kvcache"
	"github.com/erigontech/erigon-lib/kv/mdbx"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/txnprovider/txpool"
	"github.com/erigontech/erigon/txnprovider/txpool/txpoolcfg"
)

func SaveChainConfigIfNeed(ctx context.Context, coreDB kv.RoDB, txPoolDB kv.RwDB, force bool, logger log.Logger) (cc *chain.Config, blockNum uint64, err error) {
	if err = txPoolDB.View(ctx, func(tx kv.Tx) error {
		cc, err = txpool.ChainConfig(tx)
		if err != nil {
			return err
		}
		blockNum, err = txpool.LastSeenBlock(tx)
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		return nil, 0, err
	}
	if cc != nil && !force {
		if cc.ChainID.Uint64() == 0 {
			return nil, 0, errors.New("wrong chain config")
		}
		return cc, blockNum, nil
	}

	for {
		if err = coreDB.View(ctx, func(tx kv.Tx) error {
			cc, err = chain.GetConfig(tx, nil)
			if err != nil {
				return err
			}
			n, err := chain.CurrentBlockNumber(tx)
			if err != nil {
				return err
			}
			if n != nil {
				blockNum = *n
			}
			return nil
		}); err != nil {
			logger.Error("cant read chain config from core db", "err", err)
			time.Sleep(5 * time.Second)
			continue
		} else if cc == nil {
			logger.Error("cant read chain config from core db")
			time.Sleep(5 * time.Second)
			continue
		}
		break
	}

	if err = txPoolDB.Update(ctx, func(tx kv.RwTx) error {
		if err = txpool.PutChainConfig(tx, cc, nil); err != nil {
			return err
		}
		if err = txpool.PutLastSeenBlock(tx, blockNum, nil); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return nil, 0, err
	}
	if cc.ChainID.Uint64() == 0 {
		return nil, 0, errors.New("wrong chain config")
	}
	return cc, blockNum, nil
}

func AllComponents(ctx context.Context, cfg txpoolcfg.Config, cache kvcache.Cache, newTxns chan txpool.Announcements, chainDB kv.RoDB,
	sentryClients []sentryproto.SentryClient, stateChangesClient txpool.StateChangesClient, feeCalculator txpool.FeeCalculator, logger log.Logger) (kv.RwDB, *txpool.TxPool, *txpool.Fetch, *txpool.Send, *txpool.GrpcServer, error) {
	opts := mdbx.New(kv.TxPoolDB, logger).Path(cfg.DBDir).
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

	txPoolDB, err := opts.Open(ctx)

	if err != nil {
		return nil, nil, nil, nil, nil, err
	}

	chainConfig, _, err := SaveChainConfigIfNeed(ctx, chainDB, txPoolDB, true, logger)
	if err != nil {
		return nil, nil, nil, nil, nil, err
	}

	chainID, _ := uint256.FromBig(chainConfig.ChainID)
	maxBlobsPerBlock := chainConfig.GetMaxBlobsPerBlock()

	shanghaiTime := chainConfig.ShanghaiTime
	var agraBlock *big.Int
	if chainConfig.Bor != nil {
		agraBlock = chainConfig.Bor.GetAgraBlock()
	}
	cancunTime := chainConfig.CancunTime
	pragueTime := chainConfig.PragueTime
	if cfg.OverridePragueTime != nil {
		pragueTime = cfg.OverridePragueTime
	}

	txPool, err := txpool.New(
		newTxns,
		txPoolDB,
		chainDB,
		cfg,
		cache,
		*chainID,
		shanghaiTime,
		agraBlock,
		cancunTime,
		pragueTime,
		maxBlobsPerBlock,
		feeCalculator,
		logger,
	)
	if err != nil {
		return nil, nil, nil, nil, nil, err
	}

	fetch := txpool.NewFetch(ctx, sentryClients, txPool, stateChangesClient, chainDB, txPoolDB, *chainID, logger)
	//fetch.ConnectCore()
	//fetch.ConnectSentries()

	send := txpool.NewSend(ctx, sentryClients, txPool, logger)
	txpoolGrpcServer := txpool.NewGrpcServer(ctx, txPool, txPoolDB, *chainID, logger)
	return txPoolDB, txPool, fetch, send, txpoolGrpcServer, nil
}
