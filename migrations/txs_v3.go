package migrations

import (
	"bytes"
	"context"
	"encoding/binary"
	"runtime"
	"time"

	common2 "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/common/dbg"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/core/rawdb"
)

var TxsV3 = Migration{
	Name: "txs_v3",
	Up: func(db kv.RwDB, dirs datadir.Dirs, progress []byte, BeforeCommit Callback, logger log.Logger) (err error) {
		logEvery := time.NewTicker(10 * time.Second)
		defer logEvery.Stop()

		// in TxsV3 format canonical and non-canonical blocks are stored in the same table: kv.EthTx

		txIDBytes := make([]byte, 8)

		// just delete all non-canonical blocks.
		if err := db.Update(context.Background(), func(tx kv.RwTx) error {
			from := hexutility.EncodeTs(1) // protect genesis
			if err := tx.ForEach(kv.BlockBody, from, func(k, _ []byte) error {
				blockNum := binary.BigEndian.Uint64(k)
				select {
				case <-logEvery.C:
					var m runtime.MemStats
					dbg.ReadMemStats(&m)
					logger.Info("[txs_v3] Migration progress", "block_num", blockNum,
						"alloc", common2.ByteCount(m.Alloc), "sys", common2.ByteCount(m.Sys))
				default:
				}

				canonicalHash, err := rawdb.ReadCanonicalHash(tx, blockNum)
				if err != nil {
					return err
				}
				isCanonical := bytes.Equal(k[8:], canonicalHash[:])
				if isCanonical {
					return nil // skip
				}
				b, err := rawdb.ReadBodyForStorageByKey(tx, k)
				if err != nil {
					return err
				}
				if b == nil {
					log.Debug("PruneBlocks: block body not found", "height", blockNum)
				} else {
					for txID := b.BaseTxId; txID < b.BaseTxId+uint64(b.TxAmount); txID++ {
						binary.BigEndian.PutUint64(txIDBytes, txID)
						if err = tx.Delete(kv.NonCanonicalTxs, txIDBytes); err != nil {
							return err
						}
					}
				}
				// Copying k because otherwise the same memory will be reused
				// for the next key and Delete below will end up deleting 1 more record than required
				kCopy := common2.CopyBytes(k)
				if err = tx.Delete(kv.Senders, kCopy); err != nil {
					return err
				}
				if err = tx.Delete(kv.BlockBody, kCopy); err != nil {
					return err
				}
				if err = tx.Delete(kv.Headers, kCopy); err != nil {
					return err
				}
				return nil
			}); err != nil {
				return err
			}

			// This migration is no-op, but it forces the migration mechanism to apply it and thus write the DB schema version info
			return BeforeCommit(tx, nil, true)
		}); err != nil {
			return err
		}
		return nil
	},
}
