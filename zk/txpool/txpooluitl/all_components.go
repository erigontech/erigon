/*
   Copyright 2021 Erigon contributors

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
	"math/big"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/txpool/txpoolcfg"
	mdbx2 "github.com/erigontech/mdbx-go/mdbx"
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/direct"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/kvcache"
	"github.com/erigontech/erigon-lib/kv/mdbx"
	"github.com/erigontech/erigon-lib/types"
	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/erigontech/erigon/zk/txpool"
)

func SaveChainConfigIfNeed(ctx context.Context, coreDB kv.RoDB, txPoolDB kv.RwDB, force bool) (cc *chain.Config, blockNum uint64, err error) {
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
			log.Error("cant read chain config from core db", "err", err)
			time.Sleep(5 * time.Second)
			continue
		} else if cc == nil {
			log.Error("cant read chain config from core db")
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

func AllComponents(ctx context.Context, cfg txpoolcfg.Config, ethCfg *ethconfig.Config, cache kvcache.Cache, newTxs chan types.Announcements, chainDB kv.RoDB, sentryClients []direct.SentryClient, stateChangesClient txpool.StateChangesClient, logger log.Logger) (kv.RwDB, *txpool.TxPool, *txpool.Fetch, *txpool.Send, *txpool.GrpcServer, error) {
	txPoolDB, err := mdbx.NewMDBX(log.New()).Label(kv.TxPoolDB).Path(cfg.DBDir).
		WithTableCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg { return kv.TxpoolTablesCfg }).
		Flags(func(f uint) uint { return f ^ mdbx2.Durable | mdbx2.SafeNoSync }).
		GrowthStep(16 * datasize.MB).
		SyncPeriod(30 * time.Second).
		Open(ctx)
	if err != nil {
		return nil, nil, nil, nil, nil, err
	}

	aclDB, err := txpool.OpenACLDB(ctx, cfg.DBDir)
	if err != nil {
		return nil, nil, nil, nil, nil, err
	}

	if ethCfg.Zk.ACLPrintHistory > 0 && len(ethCfg.Zk.ACLJsonLocation) <= 0 {
		pts, _ := txpool.LastPolicyTransactions(context.Background(), aclDB, ethCfg.Zk.ACLPrintHistory)
		if len(pts) == 0 {
			log.Info("[ACL] No policy transactions found")
		}
		for i, pt := range pts {
			log.Info("[ACL] Policy transaction - ", "index:", i, "pt:", pt.ToString())
		}
	}

	chainConfig, _, err := SaveChainConfigIfNeed(ctx, chainDB, txPoolDB, true)
	if err != nil {
		return nil, nil, nil, nil, nil, err
	}

	chainID, _ := uint256.FromBig(chainConfig.ChainID)

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
	if cfg.OverrideShanghaiTime != nil {
		shanghaiTime = cfg.OverrideShanghaiTime
	}

	var priorityList *txpool.PriorityList
	if len(ethCfg.Zk.PrioritySendersJsonLocation) > 0 {
		priorityList, err = txpool.UnmarshalDynamicPriorityList(ethCfg.Zk.PrioritySendersJsonLocation)
		if err != nil {
			return nil, nil, nil, nil, nil, fmt.Errorf("failed to unmarshal dynamic priority list: %w", err)
		}
	}

	txPool, err := txpool.New(newTxs, chainDB, cfg, cache, *chainID, shanghaiTime, agraBlock, cancunTime, pragueTime, chainConfig.BlobSchedule, chainConfig.LondonBlock, ethCfg, aclDB, priorityList)
	if err != nil {
		return nil, nil, nil, nil, nil, err
	}

	if err = txPoolDB.Update(ctx, func(tx kv.RwTx) error {
		return txpool.CreateTxPoolBuckets(tx)
	}); err != nil {
		return nil, nil, nil, nil, nil, err
	}

	fetch := txpool.NewFetch(ctx, sentryClients, txPool, stateChangesClient, chainDB, txPoolDB, *chainID)
	//fetch.ConnectCore()
	//fetch.ConnectSentries()

	send := txpool.NewSend(ctx, sentryClients, txPool)
	txpoolGrpcServer := txpool.NewGrpcServer(ctx, txPool, txPoolDB, *chainID)
	return txPoolDB, txPool, fetch, send, txpoolGrpcServer, nil
}
