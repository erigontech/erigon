package migrations

import (
	"context"
	"encoding/binary"
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/ethdb/kv"
	"github.com/ledgerwatch/erigon/rlp"
	"math/big"
	"testing"
)

func TestBlockTransactions(t *testing.T) {
	db := kv.NewTestDB(t)
	tx, err := db.Begin(context.Background(), ethdb.RW)
	if err != nil {
		t.Fatal(err)
	}

	defer tx.Rollback()
	for blockNum := uint64(1); blockNum <= 3; blockNum++ {
		for blockID := 1; blockID < 4; blockID++ {
			bodyForStorage := new(BodyForStorageDeprecated)
			baseTxId, err := tx.IncrementSequence(dbutils.EthTx, 3)
			if err != nil {
				t.Fatal(err)
			}
			bodyForStorage.BaseTxId = baseTxId
			bodyForStorage.TxAmount = 3
			body, err := rlp.EncodeToBytes(bodyForStorage)
			if err != nil {
				t.Fatal(err)
			}
			err = tx.Put(dbutils.BlockBodyPrefix, dbutils.BlockBodyKey(blockNum, common.Hash{uint8(blockNum), uint8(blockID)}), body)
			if err != nil {
				t.Fatal(err)
			}
			header := &types.Header{
				Number: big.NewInt(int64(blockNum)),
			}
			headersBytes, err := rlp.EncodeToBytes(header)
			if err != nil {
				t.Fatal(err)
			}

			err = tx.Put(dbutils.HeadersBucket, dbutils.HeaderKey(blockNum, common.Hash{uint8(blockNum), uint8(blockID)}), headersBytes)
			if err != nil {
				t.Fatal(err)
			}

			genTx := func(a common.Address) ([]byte, error) {
				return rlp.EncodeToBytes(types.NewTransaction(1, a, uint256.NewInt(1), 1, uint256.NewInt(1), nil))
			}
			txBytes, err := genTx(common.Address{uint8(blockNum), uint8(blockID), 1})
			if err != nil {
				t.Fatal(err)
			}

			err = tx.Put(dbutils.EthTx, dbutils.EncodeBlockNumber(baseTxId), txBytes)
			if err != nil {
				t.Fatal(err)
			}
			txBytes, err = genTx(common.Address{uint8(blockNum), uint8(blockID), 2})
			if err != nil {
				t.Fatal(err)
			}

			err = tx.Put(dbutils.EthTx, dbutils.EncodeBlockNumber(baseTxId+1), txBytes)
			if err != nil {
				t.Fatal(err)
			}

			txBytes, err = genTx(common.Address{uint8(blockNum), uint8(blockID), 3})
			if err != nil {
				t.Fatal(err)
			}

			err = tx.Put(dbutils.EthTx, dbutils.EncodeBlockNumber(baseTxId+2), txBytes)
			if err != nil {
				t.Fatal(err)
			}
		}

		err := tx.Put(dbutils.HeaderCanonicalBucket, dbutils.EncodeBlockNumber(blockNum), common.Hash{uint8(blockNum), uint8(blockNum%3) + 1}.Bytes())
		if err != nil {
			t.Fatal(err)
		}
	}
	err = tx.Commit()
	if err != nil {
		t.Fatal(err)
	}

	tx, err = db.Begin(context.Background(), ethdb.RW)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	err = splitCanonicalAndNonCanonicalTransactionsBuckets.Up(tx, t.TempDir(), []byte{}, func(_ ethdb.Putter, key []byte, isDone bool) error {
		if isDone {
			return tx.Commit()
		}
		return tx.CommitAndBegin(context.Background())
	})
	if err != nil {
		t.Fatal(err)
	}
	tx.Rollback()
	seq, err := db.ReadSequence(dbutils.EthTx)
	if err != nil {
		t.Fatal(err)
	}
	//3 blocks 3 transactions + tx before block + tx after block
	if seq != 3*(2+3)-1 {
		t.Fatal(seq)
	}

	seq, err = db.ReadSequence(dbutils.NonCanonicalTXBucket)
	//6 blocks 3 transactions + tx before block + tx after block
	if seq != 6*(2+3)-1 {
		t.Fatal(seq)
	}

	err = db.ForEach(dbutils.BlockBodyPrefix, []byte{}, func(k, v []byte) error {
		blockNum := binary.BigEndian.Uint64(k[:8])
		blockHash := common.BytesToHash(k[8:])
		bfs := types.BodyForStorage{}
		err = rlp.DecodeBytes(v, &bfs)
		if err != nil {
			t.Fatal(err, v)
		}

		canonical, err := rawdb.ReadCanonicalHash(db, blockNum)
		if err != nil {
			t.Fatal(err)
		}
		transactions, err := rawdb.ReadTransactions(db, bfs.BaseTxId, bfs.TxAmount, canonical == blockHash)
		if err != nil {
			t.Fatal(err)
		}

		var txNum uint8 = 1
		for _, tr := range transactions {
			expected := common.Address{uint8(blockNum), uint8(blockHash[1]), txNum}
			if *tr.GetTo() != expected {
				t.Fatal(*tr.GetTo(), expected)
			}
			txNum++
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
