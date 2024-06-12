package stagedsync

import (
	"context"
	"fmt"
	"time"

	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/rawdbv3"
	"github.com/ledgerwatch/erigon-lib/state"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/log/v3"
)

var testKey = []byte("test")

func CustomTestExecutor(ctx context.Context, db kv.RoDB, agg *state.Aggregator, tx kv.RwTx, dom *state.SharedDomains, isInternalTx bool, tmpDir string, chainConfig *chain.Config, blockReader services.FullBlockReader, engine consensus.Engine, startBlock, endBlock uint64, isShortInterval bool, logEvery *time.Ticker, s *StageState, logger log.Logger) (uint64, error) {
	if !isShortInterval {
		log.Info(fmt.Sprintf("[%s] Using incremental executor", s.LogPrefix()))
	}

	startTxNum, err := rawdbv3.TxNums.Min(tx, startBlock)
	if err != nil {
		return startBlock, err
	}
	endTxNum, err := rawdbv3.TxNums.Max(tx, endBlock)
	if err != nil {
		return startBlock, err
	}

	currentTxNum := startTxNum
	count := 0
	for ; currentTxNum <= endTxNum; currentTxNum++ {
		if currentTxNum%10_000_000 == 0 {
			dom.SetTxNum(currentTxNum)
			if err := dom.IndexAdd(kv.TblTestIIIdx, testKey); err != nil {
				return startBlock, err
			}
			count++
		}

		select {
		default:
		case <-ctx.Done():
			return startBlock, libcommon.ErrStopped
		case <-logEvery.C:
			log.Info(fmt.Sprintf("[%s]", s.LogPrefix()), "currentTxNum", currentTxNum, "c", count)
		}
	}
	if count > 0 {
		log.Info(fmt.Sprintf("[%s] new test indexes generated", s.LogPrefix()), "txNum", currentTxNum, "c", count)
	}

	return endBlock, nil
}
