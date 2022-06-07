package migrations

import (
	"context"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/rawdb/rawdbreset"
	"github.com/ledgerwatch/erigon/node/nodecfg/datadir"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync/snap"
)

var resetBlocks = Migration{
	Name: "reset_blocks",
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
			return
		}
		genesisBlock := rawdb.ReadHeaderByNumber(tx, 0)
		if genesisBlock == nil {
			if err := BeforeCommit(tx, nil, true); err != nil {
				return err
			}
			return nil
		}
		chainConfig, err := rawdb.ReadChainConfig(tx, genesisBlock.Hash())
		if err != nil {
			return err
		}

		if err := snap.RemoveNonPreverifiedFiles(chainConfig.ChainName, dirs.Snap); err != nil {
			return err
		}

		if err := rawdbreset.ResetBlocks(tx); err != nil {
			return err
		}

		if err := rawdbreset.ResetSenders(tx); err != nil {
			return err
		}

		// This migration is no-op, but it forces the migration mechanism to apply it and thus write the DB schema version info
		if err := BeforeCommit(tx, nil, true); err != nil {
			return err
		}
		return tx.Commit()
	},
}
