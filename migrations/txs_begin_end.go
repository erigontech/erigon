package migrations

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"runtime"
	"time"

	common2 "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/assert"
	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/common/dbg"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/rlp"
)

var ErrTxsBeginEndNoMigration = fmt.Errorf("in this Erigon version DB format was changed: added additional first/last system-txs to blocks. There is no DB migration for this change. Please re-sync or switch to earlier version")

var TxsBeginEnd = Migration{
	Name: "txs_begin_end",
	Up: func(db kv.RwDB, dirs datadir.Dirs, progress []byte, BeforeCommit Callback, logger log.Logger) (err error) {
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
				logger.Info("[database version migration] Continue migration", "from_block", latestBlock)
			} else {
				latestBlock = bodiesProgress + 1 // include block 0
			}
			return nil
		}); err != nil {
			return err
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
				dbg.ReadMemStats(&m)
				logger.Info("[database version migration] Adding system-txs",
					"progress", fmt.Sprintf("%.2f%%", 100-100*float64(blockNum)/float64(latestBlock)), "block_num", blockNum,
					"alloc", common2.ByteCount(m.Alloc), "sys", common2.ByteCount(m.Sys))
			default:
			}

			canonicalHash, err := rawdb.ReadCanonicalHash(tx, blockNum)
			if err != nil {
				return err
			}

			var oldBlock *types.Body
			if assert.Enable {
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

			txs, err := canonicalTransactions(tx, b.BaseTxId, b.TxAmount)
			if err != nil {
				return err
			}

			b.BaseTxId += (blockNum) * 2
			b.TxAmount += 2
			if err := rawdb.WriteBodyForStorage(tx, canonicalHash, blockNum, b); err != nil {
				return fmt.Errorf("failed to write body: %w", err)
			}

			// del first tx in block
			if err = tx.Delete(kv.EthTx, hexutility.EncodeTs(b.BaseTxId)); err != nil {
				return err
			}
			if err := writeTransactionsNewDeprecated(tx, txs, b.BaseTxId+1); err != nil {
				return fmt.Errorf("failed to write body txs: %w", err)
			}
			// del last tx in block
			if err = tx.Delete(kv.EthTx, hexutility.EncodeTs(b.BaseTxId+uint64(b.TxAmount)-1)); err != nil {
				return err
			}

			if assert.Enable {
				newBlock, baseTxId, txAmount := rawdb.ReadBody(tx, canonicalHash, blockNum)
				newBlock.Transactions, err = canonicalTransactions(tx, baseTxId, txAmount)
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
					if err = tx.Delete(kv.NonCanonicalTxs, numBuf); err != nil {
						return err
					}
				}

				if err = tx.Delete(kv.BlockBody, k); err != nil {
					return err
				}
				if err = tx.Delete(kv.Headers, k); err != nil {
					return err
				}
				if err = tx.Delete(kv.HeaderTD, k); err != nil {
					return err
				}
				if err = tx.Delete(kv.HeaderNumber, k[8:]); err != nil {
					return err
				}
				if err = tx.Delete(kv.HeaderNumber, k[8:]); err != nil {
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

			{
				c, err := tx.Cursor(kv.HeaderCanonical)
				if err != nil {
					return err
				}
				k, v, err := c.Last()
				if err != nil {
					return err
				}
				data, err := tx.GetOne(kv.BlockBody, append(k, v...))
				if err != nil {
					return err
				}
				var newSeqValue uint64
				if len(data) > 0 {
					bodyForStorage := new(types.BodyForStorage)
					if err := rlp.DecodeBytes(data, bodyForStorage); err != nil {
						return fmt.Errorf("rlp.DecodeBytes(bodyForStorage): %w", err)
					}
					currentSeq, err := tx.ReadSequence(kv.EthTx)
					if err != nil {
						return err
					}
					newSeqValue = bodyForStorage.BaseTxId + uint64(bodyForStorage.TxAmount) - currentSeq
				}

				if _, err := tx.IncrementSequence(kv.EthTx, newSeqValue); err != nil {
					return err
				}
			}

			// This migration is no-op, but it forces the migration mechanism to apply it and thus write the DB schema version info
			return BeforeCommit(tx, nil, true)
		})
	},
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

func readCanonicalBodyWithTransactionsDeprecated(db kv.Getter, hash common2.Hash, number uint64) *types.Body {
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
	body.Transactions, err = canonicalTransactions(db, bodyForStorage.BaseTxId, bodyForStorage.TxAmount)
	if err != nil {
		log.Error("failed ReadTransactionByHash", "hash", hash, "block", number, "err", err)
		return nil
	}
	return body
}

func MakeBodiesNonCanonicalDeprecated(tx kv.RwTx, from uint64, ctx context.Context, logPrefix string, logEvery *time.Ticker) error {
	for blockNum := from; ; blockNum++ {
		h, err := rawdb.ReadCanonicalHash(tx, blockNum)
		if err != nil {
			return err
		}
		if h == (common2.Hash{}) {
			break
		}
		data := rawdb.ReadStorageBodyRLP(tx, h, blockNum)
		if len(data) == 0 {
			break
		}

		bodyForStorage := new(types.BodyForStorage)
		if err := rlp.DecodeBytes(data, bodyForStorage); err != nil {
			return err
		}

		// move txs to NonCanonical bucket, it has own sequence
		newBaseId, err := tx.IncrementSequence(kv.NonCanonicalTxs, uint64(bodyForStorage.TxAmount))
		if err != nil {
			return err
		}
		id := newBaseId
		if err := tx.ForAmount(kv.EthTx, hexutility.EncodeTs(bodyForStorage.BaseTxId), bodyForStorage.TxAmount, func(k, v []byte) error {
			if err := tx.Put(kv.NonCanonicalTxs, hexutility.EncodeTs(id), v); err != nil {
				return err
			}
			id++
			return tx.Delete(kv.EthTx, k)
		}); err != nil {
			return err
		}

		bodyForStorage.BaseTxId = newBaseId
		if err := rawdb.WriteBodyForStorage(tx, h, blockNum, bodyForStorage); err != nil {
			return err
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-logEvery.C:
			log.Info(fmt.Sprintf("[%s] Unwinding transactions...", logPrefix), "current block", blockNum)
		default:
		}
	}

	// EthTx must have canonical id's - means need decrement it's sequence on unwind
	c, err := tx.Cursor(kv.EthTx)
	if err != nil {
		return err
	}
	defer c.Close()
	k, _, err := c.Last()
	if err != nil {
		return err
	}
	var nextTxID uint64
	if k != nil {
		nextTxID = binary.BigEndian.Uint64(k) + 1
	}
	if err := rawdb.ResetSequence(tx, kv.EthTx, nextTxID); err != nil {
		return err
	}

	return nil
}

func canonicalTransactions(db kv.Getter, baseTxId uint64, amount uint32) ([]types.Transaction, error) {
	if amount == 0 {
		return []types.Transaction{}, nil
	}
	txIdKey := make([]byte, 8)
	txs := make([]types.Transaction, amount)
	binary.BigEndian.PutUint64(txIdKey, baseTxId)
	i := uint32(0)

	if err := db.ForAmount(kv.EthTx, txIdKey, amount, func(k, v []byte) error {
		var decodeErr error
		if txs[i], decodeErr = types.UnmarshalTransactionFromBinary(v); decodeErr != nil {
			return decodeErr
		}
		i++
		return nil
	}); err != nil {
		return nil, err
	}
	txs = txs[:i] // user may request big "amount", but db can return small "amount". Return as much as we found.
	return txs, nil
}
