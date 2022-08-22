package exec22

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/erigon-lib/common/cmp"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/cmd/hack/tool"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
)

type TxNums []uint64

func (s TxNums) MaxOf(blockNum uint64) (txnNum uint64) { return s[blockNum] }
func (s TxNums) MinOf(blockNum uint64) (txnNum uint64) {
	if blockNum == 0 {
		return 0
	}
	return s[blockNum-1] + 1
}

func TxNumsFromDB(s *snapshotsync.RoSnapshots, db kv.RoDB) TxNums {
	historyV2 := tool.HistoryV2FromDB(db)
	if !historyV2 {
		return nil
	}

	toBlock := s.BlocksAvailable()
	if err := db.View(context.Background(), func(tx kv.Tx) error {
		p, err := stages.GetStageProgress(tx, stages.Bodies)
		if err != nil {
			return err
		}
		toBlock = cmp.Max(toBlock, p)
		return nil
	}); err != nil {
		panic(err)
	}

	txNums := make(TxNums, toBlock+1)
	if err := db.View(context.Background(), func(tx kv.Tx) error {
		if err := (snapshotsync.BodiesIterator{}).ForEach(tx, s, 0, func(blockNum, baseTxNum, txAmount uint64) error {
			if blockNum > toBlock {
				return nil
			}
			txNums[blockNum] = baseTxNum + txAmount
			return nil
		}); err != nil {
			return fmt.Errorf("build txNum => blockNum mapping: %w", err)
		}
		return nil
	}); err != nil {
		panic(err)
	}
	return txNums
}
