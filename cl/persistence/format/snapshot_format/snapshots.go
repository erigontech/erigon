package snapshot_format

import (
	"bytes"
	"context"
	"fmt"

	"github.com/ledgerwatch/erigon-lib/common/background"
	"github.com/ledgerwatch/erigon-lib/common/cmp"
	"github.com/ledgerwatch/erigon-lib/compress"
	"github.com/ledgerwatch/erigon-lib/downloader/snaptype"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/cl/persistence"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync/freezeblocks"
	"github.com/ledgerwatch/log/v3"
)

func dumpBeaconBlocksRange(ctx context.Context, db kv.RoDB, b persistence.BlockSource, fromSlot uint64, toSlot uint64, tmpDir, snapDir string, workers int, lvl log.Lvl, logger log.Logger) error {
	segName := snaptype.SegmentFileName(fromSlot, toSlot, snaptype.BeaconBlocks)
	f, _ := snaptype.ParseFileName(snapDir, segName)

	sn, err := compress.NewCompressor(ctx, "Snapshot BeaconBlocks", f.Path, tmpDir, compress.MinPatternScore, workers, lvl, logger)
	if err != nil {
		return err
	}
	defer sn.Close()

	tx, err := db.BeginRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	// Generate .seg file, which is just the list of beacon blocks.
	var buf bytes.Buffer
	for i := fromSlot; i <= toSlot; i++ {
		obj, err := b.GetBlock(tx, ctx, i)
		if err != nil {
			return err
		}
		if obj == nil {
			if err := sn.AddWord(nil); err != nil {
				return err
			}
			continue
		}
		if err := WriteBlockForSnapshot(obj.Data, &buf); err != nil {
			return err
		}
		if err := sn.AddWord(buf.Bytes()); err != nil {
			return err
		}
		buf.Reset()
	}
	if err := sn.Compress(); err != nil {
		return fmt.Errorf("compress: %w", err)
	}
	// Generate .idx file, which is the slot => offset mapping.
	p := &background.Progress{}

	return freezeblocks.BeaconBlocksIdx(ctx, segName, fromSlot, toSlot, snapDir, tmpDir, p, lvl, logger)
}

func chooseSegmentEnd(from, to, blocksPerFile uint64) uint64 {
	next := (from/blocksPerFile + 1) * blocksPerFile
	to = cmp.Min(next, to)

	if to < snaptype.Erigon2MinSegmentSize {
		return to
	}

	return to - (to % snaptype.Erigon2MinSegmentSize) // round down to the nearest 1k
}

func DumpBeaconBlocks(ctx context.Context, db kv.RoDB, b persistence.BlockSource, fromSlot, toSlot, blocksPerFile uint64, tmpDir, snapDir string, workers int, lvl log.Lvl, logger log.Logger) error {
	if blocksPerFile == 0 {
		return nil
	}

	for i := fromSlot; i < toSlot; i = chooseSegmentEnd(i, toSlot, blocksPerFile) {
		to := chooseSegmentEnd(i, toSlot, blocksPerFile)
		logger.Log(lvl, "Dumping beacon blocks", "from", i, "to", to)
		if err := dumpBeaconBlocksRange(ctx, db, b, i, to, tmpDir, snapDir, workers, lvl, logger); err != nil {
			return err
		}
	}
	return nil
}
