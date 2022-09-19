package migrations

import (
	"context"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/rawdb/rawdbreset"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/node/nodecfg/datadir"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync/snap"
	"github.com/ledgerwatch/log/v3"
)

var resetBlocks = Migration{
	Name: "reset_blocks_3",
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
		genesisBlock := rawdb.ReadHeaderByNumber(tx, 0)
		if genesisBlock == nil {
			if err := BeforeCommit(tx, nil, true); err != nil {
				return err
			}
			return tx.Commit()
		}
		chainConfig, err := rawdb.ReadChainConfig(tx, genesisBlock.Hash())
		if err != nil {
			return err
		}
		headersProgress, _ := stages.GetStageProgress(tx, stages.Headers)
		if headersProgress > 0 {
			log.Warn("NOTE: this migration will remove recent blocks (and senders) to fix several recent bugs. Your node will re-download last ~400K blocks, should not take very long")
		}

		if err := snap.RemoveNonPreverifiedFiles(chainConfig.ChainName, dirs.Snap); err != nil {
			return err
		}

		if err := rawdbreset.ResetBlocks(tx, nil, nil); err != nil {
			return err
		}

		if err := rawdbreset.ResetSenders(tx); err != nil {
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
