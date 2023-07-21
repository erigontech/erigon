package freezeblocks

import (
	"context"
	"encoding/binary"
	"fmt"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/ledgerwatch/erigon-lib/chain"
	common2 "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/background"
	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/common/dbg"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon-lib/compress"
	"github.com/ledgerwatch/erigon-lib/downloader/snaptype"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/recsplit"
	"github.com/ledgerwatch/erigon/cmd/hack/tool/fromdb"
	"github.com/ledgerwatch/erigon/core/rawdb/blockio"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/log/v3"
)

type BorRetire struct {
	working               atomic.Bool
	needSaveFilesListInDB atomic.Bool

	workers int
	tmpDir  string
	db      kv.RoDB

	notifier    services.DBEventNotifier
	logger      log.Logger
	blockReader services.FullBlockReader
	blockWriter *blockio.BlockWriter
	dirs        datadir.Dirs
}

func NewBorRetire(workers int, dirs datadir.Dirs, blockReader services.FullBlockReader, blockWriter *blockio.BlockWriter, db kv.RoDB, notifier services.DBEventNotifier, logger log.Logger) *BorRetire {
	return &BorRetire{workers: workers, tmpDir: dirs.Tmp, dirs: dirs, blockReader: blockReader, blockWriter: blockWriter, db: db, notifier: notifier, logger: logger}
}

func (br *BorRetire) snapshots() *RoSnapshots { return br.blockReader.Snapshots().(*RoSnapshots) }

func (br *BorRetire) PruneAncientBlocks(tx kv.RwTx, limit int) error {
	if br.blockReader.FreezingCfg().KeepBlocks {
		return nil
	}
	currentProgress, err := stages.GetStageProgress(tx, stages.Senders)
	if err != nil {
		return err
	}
	canDeleteTo := CanDeleteTo(currentProgress, br.blockReader.FrozenBorBlocks())
	if err := br.blockWriter.PruneBorBlocks(context.Background(), tx, canDeleteTo, limit); err != nil {
		return nil
	}
	return nil
}

func (br *BorRetire) RetireBlocks(ctx context.Context, blockFrom, blockTo uint64, lvl log.Lvl, seedNewSnapshots func(downloadRequest []services.DownloadRequest) error) error {
	chainConfig := fromdb.ChainConfig(br.db)
	logger, blockReader, tmpDir, db, workers := br.logger, br.blockReader, br.tmpDir, br.db, br.workers
	logger.Log(lvl, "[snapshots] Retire Bor Blocks", "range", fmt.Sprintf("%dk-%dk", blockFrom/1000, blockTo/1000))
	snapshots := br.snapshots()
	firstTxNum := blockReader.(*BlockReader).FirstTxNumNotInSnapshots()

	if err := DumpBorBlocks(ctx, chainConfig, blockFrom, blockTo, snaptype.Erigon2SegmentSize, tmpDir, snapshots.Dir(), firstTxNum, db, workers, lvl, logger, blockReader); err != nil {
		return fmt.Errorf("DumpBorBlocks: %w", err)
	}
	return nil
}

func DumpBorBlocks(ctx context.Context, chainConfig *chain.Config, blockFrom, blockTo, blocksPerFile uint64, tmpDir, snapDir string, firstTxNum uint64, chainDB kv.RoDB, workers int, lvl log.Lvl, logger log.Logger, blockReader services.FullBlockReader) error {
	if blocksPerFile == 0 {
		return nil
	}

	for i := blockFrom; i < blockTo; i = chooseSegmentEnd(i, blockTo, blocksPerFile) {
		if err := dumpBorBlocksRange(ctx, i, chooseSegmentEnd(i, blockTo, blocksPerFile), tmpDir, snapDir, firstTxNum, chainDB, *chainConfig, workers, lvl, logger, blockReader); err != nil {
			return err
		}
	}
	return nil
}

func dumpBorBlocksRange(ctx context.Context, blockFrom, blockTo uint64, tmpDir, snapDir string, firstTxNum uint64, chainDB kv.RoDB, chainConfig chain.Config, workers int, lvl log.Lvl, logger log.Logger, blockReader services.FullBlockReader) error {
	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()

	{
		segName := snaptype.SegmentFileName(blockFrom, blockTo, snaptype.BorEvents)
		f, _ := snaptype.ParseFileName(snapDir, segName)

		sn, err := compress.NewCompressor(ctx, "Snapshot BorEvents", f.Path, tmpDir, compress.MinPatternScore, workers, log.LvlTrace, logger)
		if err != nil {
			return err
		}
		defer sn.Close()
		if err := DumpBorEvents(ctx, chainDB, blockFrom, blockTo, workers, lvl, logger, func(v []byte) error {
			return sn.AddWord(v)
		}); err != nil {
			return fmt.Errorf("DumpBorEvents: %w", err)
		}
		if err := sn.Compress(); err != nil {
			return fmt.Errorf("compress: %w", err)
		}

		p := &background.Progress{}
		if err := buildIdx(ctx, f, &chainConfig, tmpDir, p, lvl, logger); err != nil {
			return err
		}
	}
	return nil
}

// DumpBorEvents - [from, to)
func DumpBorEvents(ctx context.Context, db kv.RoDB, blockFrom, blockTo uint64, workers int, lvl log.Lvl, logger log.Logger, collect func([]byte) error) error {
	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()

	from := hexutility.EncodeTs(blockFrom)
	if err := kv.BigChunks(db, kv.BorEventNums, from, func(tx kv.Tx, blockNumBytes, eventIdBytes []byte) (bool, error) {
		blockNum := binary.BigEndian.Uint64(blockNumBytes)
		if blockNum >= blockTo {
			return false, nil
		}
		event, e := tx.GetOne(kv.BorEvents, eventIdBytes)
		if e != nil {
			return false, e
		}
		if err := collect(event); err != nil {
			return false, err
		}
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-logEvery.C:
			var m runtime.MemStats
			if lvl >= log.LvlInfo {
				dbg.ReadMemStats(&m)
			}
			logger.Log(lvl, "[snapshots] Dumping bor events", "block num", blockNum,
				"alloc", common2.ByteCount(m.Alloc), "sys", common2.ByteCount(m.Sys),
			)
		default:
		}
		return true, nil
	}); err != nil {
		return err
	}
	return nil
}

func BorEventsIdx(ctx context.Context, segmentFilePath string, firstBlockNumInSegment uint64, tmpDir string, p *background.Progress, lvl log.Lvl, logger log.Logger) (err error) {
	defer func() {
		if rec := recover(); rec != nil {
			_, fName := filepath.Split(segmentFilePath)
			err = fmt.Errorf("BorEventsIdx: at=%s, %v, %s", fName, rec, dbg.Stack())
		}
	}()

	num := make([]byte, 8)

	d, err := compress.NewDecompressor(segmentFilePath)
	if err != nil {
		return err
	}
	defer d.Close()

	_, fname := filepath.Split(segmentFilePath)
	p.Name.Store(&fname)
	p.Total.Store(uint64(d.Count()))

	if err := Idx(ctx, d, firstBlockNumInSegment, tmpDir, log.LvlDebug, func(idx *recsplit.RecSplit, i, offset uint64, word []byte) error {
		p.Processed.Add(1)
		n := binary.PutUvarint(num, i)
		if err := idx.AddKey(num[:n], offset); err != nil {
			return err
		}
		return nil
	}, logger); err != nil {
		return fmt.Errorf("EventNumberIdx: %w", err)
	}
	return nil
}

func (br *BorRetire) RetireBlocksInBackground(ctx context.Context, forwardProgress uint64, lvl log.Lvl, seedNewSnapshots func(downloadRequest []services.DownloadRequest) error) {
	ok := br.working.CompareAndSwap(false, true)
	if !ok {
		// go-routine is still working
		return
	}
	go func() {
		defer br.working.Store(false)

		blockFrom, blockTo, ok := CanRetire(forwardProgress, br.blockReader.FrozenBorBlocks())
		if !ok {
			return
		}

		err := br.RetireBlocks(ctx, blockFrom, blockTo, lvl, seedNewSnapshots)
		if err != nil {
			br.logger.Warn("[snapshots] retire blocks", "err", err, "fromBlock", blockFrom, "toBlock", blockTo)
		}
	}()
}
func (br *BorRetire) BuildMissedIndicesIfNeed(ctx context.Context, logPrefix string, notifier services.DBEventNotifier, cc *chain.Config) error {
	return nil
}
