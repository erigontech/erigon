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

		numBuf := make([]byte, 8)
		numHashBuf := make([]byte, 8+32)
		for i := int(latestBlock); i >= 0; i-- {
			blockNum := uint64(i)

			select {
			default:
			case <-logEvery.C:
				var m runtime.MemStats
				runtime.ReadMemStats(&m)
				log.Debug("[migration] Replacement preprocessing",
					"processed", fmt.Sprintf("%.2f%%", 100-100*float64(blockNum)/float64(latestBlock)),
					//"input", common.ByteCount(inputSize.Load()), "output", common.ByteCount(outputSize.Load()),
					"alloc", common2.ByteCount(m.Alloc), "sys", common2.ByteCount(m.Sys))
			}

			if err := db.Update(context.Background(), func(tx kv.RwTx) error {
				canonicalHash, err := rawdb.ReadCanonicalHash(tx, blockNum)
				if err != nil {
					return err
				}
				binary.BigEndian.PutUint64(numHashBuf[:8], blockNum)
				copy(numHashBuf[8:], canonicalHash[:])
				b, err := rawdb.ReadBodyForStorageByKey(tx, numHashBuf)
				if err != nil {
					panic(err)
					return err
				}
				if b == nil {
					return nil
				}

				txs, err := rawdb.CanonicalTransactions(tx, b.BaseTxId, b.TxAmount)
				if err != nil {
					return err
				}

				b.BaseTxId += (latestBlock - blockNum) * 2
				b.TxAmount += 2
				if err := rawdb.WriteBodyForStorage(tx, canonicalHash, blockNum, b); err != nil {
					return fmt.Errorf("failed to write body: %w", err)
				}
				if err := writeTransactionsNewDeprecated(tx, txs, uint64(len(txs))+2); err != nil {
					return fmt.Errorf("failed to write body txs: %w", err)
				}

				//TODO: drop nonCanonical bodies, headers, txs
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

					fmt.Printf("del: %x\n", k)
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
				return BeforeCommit(tx, numBuf, false)
			}); err != nil {
				return err
			}
		}

		return db.Update(context.Background(), func(tx kv.RwTx) error {
			// reset non-canonical sequence to 0
			//v, err := tx.ReadSequence(kv.NonCanonicalTxs)
			//if err != nil {
			//	return err
			//}
			//if _, err := tx.IncrementSequence(kv.NonCanonicalTxs, -v); err != nil {
			//	return err
			//}

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
		txId++

		buf.Reset()
		if err := rlp.Encode(buf, tx); err != nil {
			return fmt.Errorf("broken tx rlp: %w", err)
		}

		// If next Append returns KeyExists error - it means you need to open transaction in App code before calling this func. Batch is also fine.
		if err := db.Put(kv.EthTx, txIdKey, common.CopyBytes(buf.Bytes())); err != nil {
			return err
		}
	}
	return nil
}
