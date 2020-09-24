package stagedsync

import (
	"context"
	"encoding/binary"
	"fmt"
	"github.com/RoaringBitmap/roaring"
	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/ethdb/bitmapdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/rlp"
	"sort"
	"time"

	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

const (
	logIndicesMemLimit       = 1 * datasize.GB
	logIndicesSingleMemLimit = 64 * datasize.KB
	logIndicesCheckSizeEvery = 1 * time.Minute
)

var LogIndexBlackList = map[string]bool{
	//string(common.FromHex("0000000000000000000000000000000000000000000000000000000000000000")): true,
	//string(common.FromHex("0000000000000000000000000000000000000000000000000000000000000001")): true,
	//string(common.FromHex("0000000000000000000000000000000000000000000000000000000000000002")): true,
	//string(common.FromHex("0000000000000000000000000000000000000000000000000000000000000003")): true,
	//string(common.FromHex("0000000000000000000000000000000000000000000000000000000000000004")): true,
	//string(common.FromHex("0000000000000000000000000000000000000000000000000000000000000005")): true,
	//common.HexToHash("ea0f544916910bb1ff33390cbe54a3f5d36d298328578399311cde3c9a750686"):  true,
	//common.HexToHash("009f837f1feddc3de305fab200310a83d2871686078dab617c02b44360c9e236"):  true,
	//common.HexToHash("00000000000000000000000048175da4c20313bcb6b62d74937d3ff985885701"):  true,
	//common.HexToHash("ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"):  true,
	//common.HexToHash("0000000000000000000000000000000000000000000000000000000000000058"):  true,
	//common.HexToHash("90890809c654f11d6e72a28fa60149770a0d11ec6c92319d6ceb2bb0a4ea1a15"):  true,
	//common.HexToHash("cc494284735b76f0235b8a507abc67ce930b369dac12b8a45e49510ccee0abe5"):  true,
	//common.HexToHash("f10cb5dcb691bb26c2685b3fd72f4ca4008c33eafd1ee88c27210ef1db722459"):  true,
	//common.HexToHash("e1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c"):  true,
	//common.HexToHash("5b59a4139d8c317549b49f57962d4733012f0e76915ab0828c22548892d71782"):  true,
	//common.HexToHash("0000000000000000000000007fb3b877f2d85d92d4172764ea5cd68982cbe53e"):  true,
	//common.HexToHash("000000000000000000000000490c0dd13bfea5865ca985297cf2bed3f77beb5d"):  true,
	//common.HexToHash("000000000000000000000000b3089884fa970922e6c099e818a8164bd0d402d2"):  true,
	//common.HexToHash("0000000000000000000000002a65aca4d5fc5b5c859090a6c34d164135398226"):  true,
	//common.HexToHash("000000000000000000000000b5606469f317018d21f504b6e1518e54b23fa761"):  true,
	//common.HexToHash("000000000000000000000000939292f2b41b74ccb7261a452de556ba2c45db86"):  true,
	//common.HexToHash("0000000000000000000000009c4ea8d25d6150a8ed2848fc745158aad926bf8d"):  true,
	//common.HexToHash("dbccb92686efceafb9bb7e0394df7f58f71b954061b81afb57109bf247d3d75a"):  true,
	//common.HexToHash("95c567a11896e793a41e067198ab5c4a4bdc7b3cf1182571fe911ec7e1426853"):  true,
	//common.HexToHash("23919512b2162ddc59b67a65e3b03c419d4105366f7d4a632f5d3c3bee9b1cff"):  true,
}

func SpawnLogIndex(s *StageState, db ethdb.Database, datadir string, quit <-chan struct{}) error {
	var tx ethdb.DbWithPendingMutations
	var useExternalTx bool
	if hasTx, ok := db.(ethdb.HasTx); ok && hasTx.Tx() != nil {
		tx = db.(ethdb.DbWithPendingMutations)
		useExternalTx = true
	} else {
		var err error
		tx, err = db.Begin(context.Background())
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	endBlock, err := s.ExecutionAt(tx)
	if err != nil {
		return fmt.Errorf("logs index: getting last executed block: %w", err)
	}
	if endBlock == s.BlockNumber {
		s.Done()
		return nil
	}

	start := s.BlockNumber
	if start > 0 {
		start++
	}

	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()

	indices := map[string]*roaring.Bitmap{}
	logIndexCursor := tx.(ethdb.HasTx).Tx().Cursor(dbutils.LogIndex2)
	receipts := tx.(ethdb.HasTx).Tx().Cursor(dbutils.BlockReceiptsPrefix)
	checkFlushEvery := time.NewTicker(logIndicesCheckSizeEvery)
	defer checkFlushEvery.Stop()

	for k, v, err := receipts.Seek(dbutils.EncodeBlockNumber(start)); k != nil; k, v, err = receipts.Next() {
		if err != nil {
			return err
		}

		if err := common.Stopped(quit); err != nil {
			return err
		}
		blockNum64Bytes := k[:len(k)-32]
		blockNum := binary.BigEndian.Uint64(blockNum64Bytes)

		select {
		default:
		case <-logEvery.C:
			sz, err := tx.(ethdb.HasTx).Tx().BucketSize(dbutils.LogIndex2)
			if err != nil {
				return err
			}
			log.Info("Progress", "blockNum", blockNum, "bucketSize", common.StorageSize(sz))
		case <-checkFlushEvery.C:
			if needFlush(indices, logIndicesMemLimit, bitmapdb.HotShardLimit/2) {
				if err := flushBitmaps(logIndexCursor, indices); err != nil {
					return err
				}

				indices = map[string]*roaring.Bitmap{}
			}
		}

		// Convert the receipts from their storage form to their internal representation
		storageReceipts := []*types.ReceiptForStorage{}
		if err := rlp.DecodeBytes(v, &storageReceipts); err != nil {
			return fmt.Errorf("invalid receipt array RLP: %w, blocl=%d", err, blockNum)
		}

		for _, receipt := range storageReceipts {
			for _, log := range receipt.Logs {
				for _, topic := range log.Topics {
					topicStr := string(topic.Bytes())
					m, ok := indices[topicStr]
					if !ok {
						m = roaring.New()
						indices[topicStr] = m
					}
					m.Add(uint32(blockNum))
				}

				accStr := string(log.Address.Bytes())
				m, ok := indices[accStr]
				if !ok {
					m = roaring.New()
					indices[accStr] = m
				}
				m.Add(uint32(blockNum))
			}
		}
	}

	if err := flushBitmaps(logIndexCursor, indices); err != nil {
		return err
	}

	if err := s.DoneAndUpdate(tx, endBlock); err != nil {
		return err
	}
	if !useExternalTx {
		if _, err := tx.Commit(); err != nil {
			return err
		}
	}

	return nil
}

func UnwindLogIndex(u *UnwindState, s *StageState, db ethdb.Database, quitCh <-chan struct{}) error {
	var tx ethdb.DbWithPendingMutations
	var useExternalTx bool
	if hasTx, ok := db.(ethdb.HasTx); ok && hasTx.Tx() != nil {
		tx = db.(ethdb.DbWithPendingMutations)
		useExternalTx = true
	} else {
		var err error
		tx, err = db.Begin(context.Background())
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	logsIndexKeys := map[string]bool{}
	receipts := tx.(ethdb.HasTx).Tx().Cursor(dbutils.BlockReceiptsPrefix)
	logIndexCursor := tx.(ethdb.HasTx).Tx().Cursor(dbutils.LogIndex2)

	for k, v, err := receipts.Seek(dbutils.EncodeBlockNumber(u.UnwindPoint + 1)); k != nil; k, v, err = receipts.Next() {
		if err != nil {
			return err
		}
		if err := common.Stopped(quitCh); err != nil {
			return err
		}
		// Convert the receipts from their storage form to their internal representation
		storageReceipts := []*types.ReceiptForStorage{}
		if err := rlp.DecodeBytes(v, &storageReceipts); err != nil {
			return fmt.Errorf("invalid receipt array RLP: %w, k=%x", err, k)
		}

		for _, storageReceipt := range storageReceipts {
			for _, log := range storageReceipt.Logs {
				for _, topic := range log.Topics {
					logsIndexKeys[string(topic.Bytes())] = true
				}
				logsIndexKeys[string(log.Address.Bytes())] = true
			}
		}
	}

	if err := truncateBitmaps(logIndexCursor, logsIndexKeys, u.UnwindPoint, s.BlockNumber+1); err != nil {
		return err
	}
	if err := u.Done(tx); err != nil {
		return fmt.Errorf("unwind AccountHistorytIndex: %w", err)
	}

	if !useExternalTx {
		if _, err := tx.Commit(); err != nil {
			return err
		}
	}

	return nil
}

func needFlush(bitmaps map[string]*roaring.Bitmap, memLimit, singleLimit datasize.ByteSize) bool {
	sz := uint64(0)
	for _, m := range bitmaps {
		sz1 := m.GetSizeInBytes()
		if sz1 > uint64(singleLimit) {
			return true
		}
		sz += sz1
	}
	const memoryNeedsForKey = 32 * 2 // each key stored in RAM: as string ang slice of bytes
	return uint64(len(bitmaps)*memoryNeedsForKey)+sz > uint64(memLimit)
}

func flushBitmaps(c ethdb.Cursor, inMem map[string]*roaring.Bitmap) error {
	t := time.Now()
	keys := make([]string, 0, len(inMem))
	for k := range inMem {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var total uint64
	for _, b := range inMem {
		total += b.GetSerializedSizeInBytes()
	}

	for _, k := range keys {
		if _, ok := LogIndexBlackList[k]; ok {
			continue
		}
		b := inMem[k]
		if err := bitmapdb.AppendMergeByOr3(c, []byte(k), b); err != nil {
			return err
		}
	}

	fmt.Printf("flush: %s, %d %s\n", time.Since(t), len(keys), common.StorageSize(total))

	return nil
}

func truncateBitmaps(c ethdb.Cursor, inMem map[string]bool, from, to uint64) error {
	t := time.Now()

	defer func(t time.Time) { fmt.Printf("stage_log_index.go:230: %s\n", time.Since(t)) }(time.Now())
	keys := make([]string, 0, len(inMem))
	for k := range inMem {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if _, ok := LogIndexBlackList[k]; ok {
			continue
		}

		if err := bitmapdb.TrimShardedRange(c, []byte(k), from, to); err != nil {
			return nil
		}
	}
	fmt.Printf("truncateBitmaps: %s, %d\n", time.Since(t), len(keys))

	return nil
}
