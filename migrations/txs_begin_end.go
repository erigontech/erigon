package migrations

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"runtime"
	"time"

	common2 "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/log/v3"
)

const ASSERT = true

var ErrTxsBeginEndNoMigration = fmt.Errorf("in this Erigon version DB format was changed: added additional first/last system-txs to blocks. There is no DB migration for this change. Please re-sync or switch to earlier version")

var txsBeginEnd = Migration{
	Name: "txs_begin_end",
	Up: func(db kv.RwDB, tmpdir string, progress []byte, BeforeCommit Callback) (err error) {
		var latestBlock uint64
		if err := db.View(context.Background(), func(tx kv.Tx) error {
			latestBlock, err = stages.GetStageProgress(tx, stages.Bodies)
			if err != nil {
				return err
			}
			return nil
		}); err != nil {
			return err
		}
		if latestBlock > 0 {
			return ErrTxsBeginEndNoMigration
		}
		return db.Update(context.Background(), func(tx kv.RwTx) error {
			return BeforeCommit(tx, nil, true)
		})
	},
}

var txsBeginEnd2 = Migration{
	Name: "txs_begin_end",
	Up: func(db kv.RwDB, tmpdir string, progress []byte, BeforeCommit Callback) (err error) {
		logEvery := time.NewTicker(10 * time.Second)
		defer logEvery.Stop()

		var latestBlock uint64
		if err := db.View(context.Background(), func(tx kv.Tx) error {
			bodiesProgress, err := stages.GetStageProgress(tx, stages.Bodies)
			if err != nil {
				return err
			}
			if progress != nil {
				latestBlock = binary.BigEndian.Uint64(progress)
				log.Info("[migration] Continue migration", "from_block", latestBlock)
			} else {
				latestBlock = bodiesProgress + 1 // include block 0
			}
			return nil
		}); err != nil {
			return err
		}

		if progress == nil {
			if err := db.Update(context.Background(), func(tx kv.RwTx) error {
				if _, err := tx.IncrementSequence(kv.EthTx, latestBlock*2); err != nil {
					return err
				}
				return nil
			}); err != nil {
				return err
			}
		}

		tx, err := db.BeginRw(context.Background())
		if err != nil {
			return err
		}
		defer tx.Rollback()

		numBuf := make([]byte, 8)
		numHashBuf := make([]byte, 8+32)
		for i := int(latestBlock); i >= 0; i-- {
			blockNum := uint64(i)

			select {
			case <-logEvery.C:
				var m runtime.MemStats
				runtime.ReadMemStats(&m)
				log.Info("[migration] Adding system-txs",
					"progress", fmt.Sprintf("%.2f%%", 100-100*float64(blockNum)/float64(latestBlock)), "block_num", blockNum,
					"alloc", common2.ByteCount(m.Alloc), "sys", common2.ByteCount(m.Sys))
			default:
			}

			canonicalHash, err := rawdb.ReadCanonicalHash(tx, blockNum)
			if err != nil {
				return err
			}

			var oldBlock *types.Body
			if ASSERT {
				oldBlock = readCanonicalBodyWithTransactionsDeprecated(tx, canonicalHash, blockNum)
			}

			binary.BigEndian.PutUint64(numHashBuf[:8], blockNum)
			copy(numHashBuf[8:], canonicalHash[:])
			b, err := rawdb.ReadBodyForStorageByKey(tx, numHashBuf)
			if err != nil {
				return err
			}
			if b == nil {
				continue
			}

			txs, err := rawdb.CanonicalTransactions(tx, b.BaseTxId, b.TxAmount)
			if err != nil {
				return err
			}

			b.BaseTxId += (blockNum) * 2
			b.TxAmount += 2
			if err := rawdb.WriteBodyForStorage(tx, canonicalHash, blockNum, b); err != nil {
				return fmt.Errorf("failed to write body: %w", err)
			}
			binary.BigEndian.PutUint64(numBuf, b.BaseTxId) // del first tx in block
			if err = tx.Delete(kv.EthTx, numHashBuf, nil); err != nil {
				return err
			}
			if err := writeTransactionsNewDeprecated(tx, txs, b.BaseTxId+1); err != nil {
				return fmt.Errorf("failed to write body txs: %w", err)
			}
			binary.BigEndian.PutUint64(numBuf, b.BaseTxId+uint64(b.TxAmount)-1) // del last tx in block
			if err = tx.Delete(kv.EthTx, numHashBuf, nil); err != nil {
				return err
			}

			if ASSERT {
				newBlock, baseTxId, txAmount := rawdb.ReadBody(tx, canonicalHash, blockNum)
				newBlock.Transactions, err = rawdb.CanonicalTransactions(tx, baseTxId, txAmount)
				for i, oldTx := range oldBlock.Transactions {
					newTx := newBlock.Transactions[i]
					if oldTx.GetNonce() != newTx.GetNonce() {
						panic(blockNum)
					}
				}
			}

			if err = tx.ForPrefix(kv.BlockBody, numHashBuf[:8], func(k, v []byte) error {
				if bytes.Equal(k, numHashBuf) { // don't delete canonical blocks
					return nil
				}
				bodyForStorage := new(types.BodyForStorage)
				if err := rlp.DecodeBytes(v, bodyForStorage); err != nil {
					return err
				}

				for i := bodyForStorage.BaseTxId; i < bodyForStorage.BaseTxId+uint64(bodyForStorage.TxAmount); i++ {
					binary.BigEndian.PutUint64(numBuf, i)
					if err = tx.Delete(kv.NonCanonicalTxs, numBuf, nil); err != nil {
						return err
					}
				}

				if err = tx.Delete(kv.BlockBody, k, nil); err != nil {
					return err
				}
				if err = tx.Delete(kv.Headers, k, nil); err != nil {
					return err
				}
				if err = tx.Delete(kv.HeaderTD, k, nil); err != nil {
					return err
				}
				if err = tx.Delete(kv.HeaderNumber, k[8:], nil); err != nil {
					return err
				}
				if err = tx.Delete(kv.HeaderNumber, k[8:], nil); err != nil {
					return err
				}

				return nil
			}); err != nil {
				return err
			}

			binary.BigEndian.PutUint64(numBuf, blockNum)
			if err := BeforeCommit(tx, numBuf, false); err != nil {
				return err
			}
			if blockNum%10_000 == 0 {
				if err := tx.Commit(); err != nil {
					return err
				}
				tx, err = db.BeginRw(context.Background())
				if err != nil {
					return err
				}
			}
		}

		if err := tx.Commit(); err != nil {
			return err
		}

		return db.Update(context.Background(), func(tx kv.RwTx) error {
			// reset non-canonical sequence to 0
			v, err := tx.ReadSequence(kv.NonCanonicalTxs)
			if err != nil {
				return err
			}
			if _, err := tx.IncrementSequence(kv.NonCanonicalTxs, -v); err != nil {
				return err
			}

			// This migration is no-op, but it forces the migration mechanism to apply it and thus write the DB schema version info
			return BeforeCommit(tx, nil, true)
		})
	},
}

func WriteRawBodyDeprecated(db kv.StatelessRwTx, hash common.Hash, number uint64, body *types.RawBody) error {
	baseTxId, err := db.IncrementSequence(kv.EthTx, uint64(len(body.Transactions)))
	if err != nil {
		return err
	}
	data := types.BodyForStorage{
		BaseTxId: baseTxId,
		TxAmount: uint32(len(body.Transactions)),
		Uncles:   body.Uncles,
	}
	if err = rawdb.WriteBodyForStorage(db, hash, number, &data); err != nil {
		return fmt.Errorf("failed to write body: %w", err)
	}
	if err = rawdb.WriteRawTransactions(db, body.Transactions, baseTxId); err != nil {
		return fmt.Errorf("failed to WriteRawTransactions: %w", err)
	}
	return nil
}
func writeTransactionsNewDeprecated(db kv.RwTx, txs []types.Transaction, baseTxId uint64) error {
	txId := baseTxId
	buf := bytes.NewBuffer(nil)
	for _, tx := range txs {
		txIdKey := make([]byte, 8)
		binary.BigEndian.PutUint64(txIdKey, txId)

		buf.Reset()
		if err := rlp.Encode(buf, tx); err != nil {
			return fmt.Errorf("broken tx rlp: %w", err)
		}
		// If next Append returns KeyExists error - it means you need to open transaction in App code before calling this func. Batch is also fine.
		if err := db.Put(kv.EthTx, txIdKey, common.CopyBytes(buf.Bytes())); err != nil {
			return err
		}
		txId++
	}
	return nil
}

func readCanonicalBodyWithTransactionsDeprecated(db kv.Getter, hash common.Hash, number uint64) *types.Body {
	data := rawdb.ReadStorageBodyRLP(db, hash, number)
	if len(data) == 0 {
		return nil
	}
	bodyForStorage := new(types.BodyForStorage)
	err := rlp.DecodeBytes(data, bodyForStorage)
	if err != nil {
		log.Error("Invalid block body RLP", "hash", hash, "err", err)
		return nil
	}
	body := new(types.Body)
	body.Uncles = bodyForStorage.Uncles
	body.Transactions, err = rawdb.CanonicalTransactions(db, bodyForStorage.BaseTxId, bodyForStorage.TxAmount)
	if err != nil {
		log.Error("failed ReadTransactionByHash", "hash", hash, "block", number, "err", err)
		return nil
	}
	return body
}
