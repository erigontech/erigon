/*
   Copyright 2021 The Erigon contributors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package txpooluitl

import (
	"context"
	"fmt"
	"time"

	"github.com/c2h5oh/datasize"
	mdbx2 "github.com/erigontech/mdbx-go/mdbx"
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/direct"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/kvcache"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon-lib/txpool"
	"github.com/ledgerwatch/erigon-lib/txpool/txpoolcfg"
	"github.com/ledgerwatch/erigon-lib/types"
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
			return nil, 0, fmt.Errorf("wrong chain config")
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
		return nil, 0, fmt.Errorf("wrong chain config")
	}
	return cc, blockNum, nil
}

func AllComponents(ctx context.Context, cfg txpoolcfg.Config, cache kvcache.Cache, newTxs chan types.Announcements, chainDB kv.RoDB,
	sentryClients []direct.SentryClient, stateChangesClient txpool.StateChangesClient, logger log.Logger) (kv.RwDB, *txpool.TxPool, *txpool.Fetch, *txpool.Send, *txpool.GrpcServer, error) {
	opts := mdbx.NewMDBX(log.New()).Label(kv.TxPoolDB).Path(cfg.DBDir).
		WithTableCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg { return kv.TxpoolTablesCfg }).
		Flags(func(f uint) uint { return f ^ mdbx2.Durable }).
		WriteMergeThreshold(3 * 8192).
		PageSize(uint64(16 * datasize.KB)).
		GrowthStep(16 * datasize.MB).
		MapSize(1 * datasize.TB)

	if cfg.MdbxPageSize.Bytes() > 0 {
		opts = opts.PageSize(cfg.MdbxPageSize.Bytes())
	}
	if cfg.MdbxDBSizeLimit > 0 {
		opts = opts.MapSize(cfg.MdbxDBSizeLimit)
	}
	if cfg.MdbxGrowthStep > 0 {
		opts = opts.GrowthStep(cfg.MdbxGrowthStep)
	}

	txPoolDB, err := opts.Open()

	if err != nil {
		return nil, nil, nil, nil, nil, err
	}

	chainConfig, _, err := SaveChainConfigIfNeed(ctx, chainDB, txPoolDB, true, logger)
	if err != nil {
		return nil, nil, nil, nil, nil, err
	}

	chainID, _ := uint256.FromBig(chainConfig.ChainID)

	shanghaiTime := chainConfig.ShanghaiTime
	cancunTime := chainConfig.CancunTime
	if cfg.OverrideCancunTime != nil {
		cancunTime = cfg.OverrideCancunTime
	}

	txPool, err := txpool.New(newTxs, chainDB, cfg, cache, *chainID, shanghaiTime, cancunTime, logger)
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
