package migrations

import (
	"context"
	"encoding/binary"

	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/cmd/hack/tool"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/rawdb/rawdbreset"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync/snap"
	"github.com/ledgerwatch/log/v3"
)

var resetBlocks4 = Migration{
	Name: "reset_blocks_4",
	Up: func(db kv.RwDB, dirs datadir.Dirs, progress []byte, BeforeCommit Callback) (err error) {
		tx, err := db.BeginRw(context.Background())
		if err != nil {
			return err
		}
		defer tx.Rollback()

		enabled, err := snap.Enabled(tx)
		if err != nil {
			return err
		}

		if !enabled {
			if err := BeforeCommit(tx, nil, true); err != nil {
				return err
			}
			return tx.Commit()
		}
		// Detect whether the correction is required
		snaps := snapshotsync.NewRoSnapshots(ethconfig.Snapshot{
			Enabled:    true,
			KeepBlocks: true,
			Produce:    false,
		}, dirs.Snap)
		snaps.ReopenFolder()
		var lastFound bool
		var lastBlockNum, lastBaseTxNum, lastAmount uint64
		if err := snaps.Bodies.View(func(sns []*snapshotsync.BodySegment) error {
			// Take the last snapshot
			if len(sns) == 0 {
				return nil
			}
			sn := sns[len(sns)-1]
			sn.Iterate(func(blockNum uint64, baseTxNum uint64, txAmount uint64) error {
				lastBlockNum = blockNum
				lastBaseTxNum = baseTxNum
				lastAmount = txAmount
				lastFound = true
				return nil
			})
			return nil
		}); err != nil {
			return err
		}
		if !lastFound {
			if err := BeforeCommit(tx, nil, true); err != nil {
				return err
			}
			return tx.Commit()
		}
		c, err := tx.Cursor(kv.BlockBody)
		if err != nil {
			return err
		}
		defer c.Close()
		var fixNeeded bool
		for k, _, err := c.First(); k != nil; k, _, err = c.Next() {
			if err != nil {
				return err
			}
			blockNumber := binary.BigEndian.Uint64(k[:8])
			if blockNumber != lastBlockNum+1 {
				continue
			}
			blockHash := common.BytesToHash(k[8:])
			var hash common.Hash
			if hash, err = rawdb.ReadCanonicalHash(tx, blockNumber); err != nil {
				return err
			}
			// ReadBody is not returning baseTxId which is written into the DB record, but that value + 1
			_, baseTxId, _ := rawdb.ReadBody(tx, blockHash, blockNumber)
			if hash != blockHash {
				continue
			}
			if lastBaseTxNum+lastAmount+1 != baseTxId {
				log.Info("Fix required, last block in seg files", "height", lastBlockNum, "baseTxNum", lastBaseTxNum, "txAmount", lastAmount, "first txId in DB", baseTxId, "expected", lastBaseTxNum+lastAmount+1)
				fixNeeded = true
			}
		}
		if !fixNeeded {
			log.Info("Fix is not required")
			if err := BeforeCommit(tx, nil, true); err != nil {
				return err
			}
			return tx.Commit()
		}

		headersProgress, _ := stages.GetStageProgress(tx, stages.Headers)
		if headersProgress > 0 {
			log.Warn("NOTE: this migration will remove recent blocks (and senders) to fix several recent bugs. Your node will re-download last ~400K blocks, should not take very long")
		}

		cc := tool.ChainConfig(tx)
		if err := rawdbreset.ResetBlocks(tx, db, nil, nil, dirs, *cc, nil); err != nil {
			return err
		}

		if err := rawdbreset.ResetSenders(context.Background(), db, tx); err != nil {
			return err
		}

		if err := rawdbreset.ResetTxLookup(tx); err != nil {
			return err
		}

		// This migration is no-op, but it forces the migration mechanism to apply it and thus write the DB schema version info
		if err := BeforeCommit(tx, nil, true); err != nil {
			return err
		}
		return tx.Commit()
	},
}
