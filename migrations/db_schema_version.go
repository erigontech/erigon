package migrations

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
)

var dbSchemaVersion5 = Migration{
	Name: "db_schema_version5",
	Up: func(db kv.RwDB, tmpdir string, progress []byte, BeforeCommit Callback) (err error) {
		tx, err := db.BeginRw(context.Background())
		if err != nil {
			return err
		}
		defer tx.Rollback()

		// This migration is no-op, but it forces the migration mechanism to apply it and thus write the DB schema version info
		if err := BeforeCommit(tx, nil, true); err != nil {
			return err
		}
		return tx.Commit()
	},
}

var txsBeginEnd = Migration{
	Name: "txs_begin_end",
	Up: func(db kv.RwDB, tmpdir string, progress []byte, BeforeCommit Callback) (err error) {
		var latestBlock uint64
		if err := db.View(context.Background(), func(tx kv.Tx) error {
			latestBlock, err = stages.GetStageProgress(tx, stages.Finish)
			if err != nil {
				return err
			}
			return nil
		}); err != nil {
			return err
		}

		numBuf := make([]byte, 8)
		numHashBuf := make([]byte, 8+32)
		for i := latestBlock; i >= 0; i-- {
			if err := db.Update(context.Background(), func(tx kv.RwTx) error {
				h, err := rawdb.ReadCanonicalHash(tx, i)
				if err != nil {
					return err
				}
				binary.BigEndian.PutUint64(numHashBuf[:8], i)
				copy(numHashBuf[8:], h[:])
				b, err := rawdb.ReadBodyForStorageByKey(tx, numHashBuf)
				if err != nil {
					return err
				}

				txs, err := rawdb.CanonicalTransactions(tx, b.BaseTxId, b.TxAmount)
				if err != nil {
					return err
				}

				b.BaseTxId += (latestBlock - i) * 2
				b.TxAmount += 2
				if err := rawdb.WriteBodyForStorage(tx, h, i, b); err != nil {
					return fmt.Errorf("failed to write body: %w", err)
				}
				if err := rawdb.WriteTransactions(tx, txs, uint64(len(txs))+2); err != nil {
					return fmt.Errorf("failed to write body txs: %w", err)
				}

				//TODO: drop nonCanonical bodies, headers, txs

				binary.BigEndian.PutUint64(numBuf, i)
				return BeforeCommit(tx, numBuf, false)
			}); err != nil {
				return err
			}
		}

		return db.Update(context.Background(), func(tx kv.RwTx) error {
			// This migration is no-op, but it forces the migration mechanism to apply it and thus write the DB schema version info
			return BeforeCommit(tx, nil, true)
		})
	},
}
