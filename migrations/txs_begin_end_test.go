package migrations_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"testing"
	"time"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon-lib/common/u256"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon/migrations"
	"github.com/ledgerwatch/log/v3"
	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
)

func TestTxsBeginEnd(t *testing.T) {
	require, tmpDir, db := require.New(t), t.TempDir(), memdb.NewTestDB(t)
	txn := &types.DynamicFeeTransaction{Tip: u256.N1, FeeCap: u256.N1, ChainID: u256.N1, CommonTx: types.CommonTx{Value: u256.N1, Gas: 1, Nonce: 1}}
	buf := bytes.NewBuffer(nil)
	err := txn.MarshalBinary(buf)
	require.NoError(err)
	rlpTxn := buf.Bytes()
	logEvery := time.NewTicker(10 * time.Second)
	defer logEvery.Stop()

	b := &types.RawBody{Transactions: [][]byte{rlpTxn, rlpTxn, rlpTxn}}
	err = db.Update(context.Background(), func(tx kv.RwTx) error {
		for i := uint64(0); i < 10; i++ {
			hash := libcommon.Hash{byte(i)}
			err = writeRawBodyDeprecated(tx, hash, i, b)
			require.NoError(err)
			err = rawdb.WriteCanonicalHash(tx, hash, i)
			require.NoError(err)
		}
		if err := migrations.MakeBodiesNonCanonicalDeprecated(tx, 7, context.Background(), "", logEvery); err != nil {
			return err
		}

		err = rawdb.TruncateCanonicalHash(tx, 7, false)
		for i := uint64(7); i < 10; i++ {
			require.NoError(err)
			hash := libcommon.Hash{0xa, byte(i)}
			err = writeRawBodyDeprecated(tx, hash, i, b)
			require.NoError(err)
			err = rawdb.WriteCanonicalHash(tx, hash, i)
			require.NoError(err)
		}
		if err := stages.SaveStageProgress(tx, stages.Bodies, 9); err != nil {
			return err
		}
		return nil
	})
	require.NoError(err)

	migrator := migrations.NewMigrator(kv.ChainDB)
	migrator.Migrations = []migrations.Migration{migrations.TxsBeginEnd}
	logger := log.New()
	err = migrator.Apply(db, tmpDir, logger)
	require.NoError(err)

	err = db.View(context.Background(), func(tx kv.Tx) error {
		v, err := tx.ReadSequence(kv.EthTx)
		require.NoError(err)
		require.Equal(uint64(3*10+2*10), v)
		return nil
	})
	require.NoError(err)

	err = db.View(context.Background(), func(tx kv.Tx) error {
		for i := uint64(7); i < 10; i++ {
			hash := libcommon.Hash{byte(i)}
			k := make([]byte, 8+32)
			binary.BigEndian.PutUint64(k, 7)
			copy(k[8:], hash[:])

			has, err := tx.Has(kv.BlockBody, k)
			require.NoError(err)
			require.False(has)
		}

		c, err := tx.Cursor(kv.NonCanonicalTxs)
		require.NoError(err)
		cnt, err := c.Count()
		require.NoError(err)
		require.Zero(cnt)

		v, err := tx.ReadSequence(kv.NonCanonicalTxs)
		require.NoError(err)
		require.Zero(v)

		has, err := tx.Has(kv.EthTx, hexutility.EncodeTs(0))
		require.NoError(err)
		require.False(has)

		return nil
	})
	require.NoError(err)

}

func writeRawBodyDeprecated(db kv.RwTx, hash libcommon.Hash, number uint64, body *types.RawBody) error {
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
	if err = writeRawTransactionsDeprecated(db, body.Transactions, baseTxId); err != nil {
		return fmt.Errorf("failed to WriteRawTransactions: %w, blockNum=%d", err, number)
	}
	return nil
}

func writeRawTransactionsDeprecated(tx kv.RwTx, txs [][]byte, baseTxId uint64) error {
	txId := baseTxId
	for _, txn := range txs {
		txIdKey := make([]byte, 8)
		binary.BigEndian.PutUint64(txIdKey, txId)
		// If next Append returns KeyExists error - it means you need to open transaction in App code before calling this func. Batch is also fine.
		if err := tx.Append(kv.EthTx, txIdKey, txn); err != nil {
			return fmt.Errorf("txId=%d, baseTxId=%d, %w", txId, baseTxId, err)
		}
		txId++
	}
	return nil
}
