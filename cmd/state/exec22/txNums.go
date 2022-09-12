package exec22

import (
	"context"
	"fmt"
	"sort"

	"github.com/ledgerwatch/erigon-lib/common/cmp"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
)

type TxNums struct {
	nums []uint64
}

func (s *TxNums) LastBlockNum() uint64                  { return uint64(len(s.nums) - 1) }
func (s *TxNums) MaxOf(blockNum uint64) (txnNum uint64) { return s.nums[blockNum] }
func (s *TxNums) MinOf(blockNum uint64) (txnNum uint64) {
	if blockNum == 0 {
		return 0
	}
	return s.nums[blockNum-1] + 1
}
func (s *TxNums) Append(blockNum, maxTxnNum uint64) {
	if len(s.nums) > int(blockNum) {
		err := fmt.Errorf("trying append blockNum=%d, but already have=%d", blockNum, len(s.nums))
		panic(err)
	}
	if len(s.nums) < int(blockNum) {
		err := fmt.Errorf("append with gap blockNum=%d, but current heigh=%d", blockNum, len(s.nums))
		panic(err)
	}
	s.nums = append(s.nums, maxTxnNum)
	//fmt.Printf("after append: %d, %d, %d\n", s.nums, blockNum, maxTxnNum)
}
func (s *TxNums) Unwind(unwindTo uint64) {
	//fmt.Printf("before unwind: %d, %d\n", unwindTo, s.nums)
	s.nums = s.nums[:unwindTo]
	//fmt.Printf("after unwind: %d, %d\n", unwindTo, s.nums)
}
func (s *TxNums) Find(endTxNumMinimax uint64) (ok bool, blockNum uint64) {
	blockNum = uint64(sort.Search(len(s.nums), func(i int) bool {
		return s.nums[i] > endTxNumMinimax
	}))
	return int(blockNum) != len(s.nums), blockNum
}

func (t *TxNums) Restore(s *snapshotsync.RoSnapshots, tx kv.Tx) error {
	historyV2, err := rawdb.HistoryV2.Enabled(tx)
	if err != nil {
		return err
	}
	if !historyV2 {
		return nil
	}

	var toBlock uint64
	if s != nil {
		toBlock = s.BlocksAvailable()
	}
	p, err := stages.GetStageProgress(tx, stages.Bodies)
	if err != nil {
		return err
	}
	toBlock = cmp.Max(toBlock, p)

	t.nums = make([]uint64, toBlock+1)
	if err := (snapshotsync.BodiesIterator{}).ForEach(tx, s, func(blockNum, baseTxNum, txAmount uint64) error {
		if blockNum > toBlock {
			return nil
		}
		t.nums[blockNum] = baseTxNum + txAmount - 1
		return nil
	}); err != nil {
		return fmt.Errorf("build txNum => blockNum mapping: %w", err)
	}
	if t.nums[0] == 0 {
		t.nums[0] = 1
	}
	return nil
}

func TxNumsFromDB(s *snapshotsync.RoSnapshots, db kv.RoDB) *TxNums {
	t := &TxNums{}
	if err := db.View(context.Background(), func(tx kv.Tx) error {
		return t.Restore(s, tx)
	}); err != nil {
		panic(err)
	}
	return t
}
