package migrations_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"math/big"
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
)

func TestTxsV3(t *testing.T) {
	require, tmpDir, db := require.New(t), t.TempDir(), memdb.NewTestDB(t)
	txn := &types.DynamicFeeTransaction{Tip: u256.N1, FeeCap: u256.N1, CommonTx: types.CommonTx{ChainID: u256.N1, Value: u256.N1, Gas: 1, Nonce: 1}}
	buf := bytes.NewBuffer(nil)
	err := txn.MarshalBinary(buf)
	require.NoError(err)
	rlpTxn := buf.Bytes()
	logEvery := time.NewTicker(10 * time.Second)
	defer logEvery.Stop()

	b := &types.RawBody{Transactions: [][]byte{rlpTxn, rlpTxn, rlpTxn}}
	err = db.Update(context.Background(), func(tx kv.RwTx) error {
		for i := uint64(1); i < 10; i++ {
			h := &types.Header{Number: big.NewInt(int64(i))}
			hash := h.Hash()
			err = writeRawBodyDeprecated(tx, hash, i, b)
			require.NoError(err)
			err = rawdb.WriteCanonicalHash(tx, hash, i)
			require.NoError(err)
		}
		err = rawdb.MakeBodiesNonCanonical(tx, 1, false, context.Background(), "", logEvery)
		require.NoError(err)

		for i := uint64(7); i < 10; i++ {
			require.NoError(err)
			h := &types.Header{Number: big.NewInt(int64(i))}
			hash := h.Hash()
			err = writeRawBodyDeprecated(tx, hash, i, b)
			require.NoError(err)
			err = rawdb.WriteCanonicalHash(tx, hash, i)
			require.NoError(err)
		}
		return nil
	})
	require.NoError(err)

	migrator := migrations.NewMigrator(kv.ChainDB)
	migrator.Migrations = []migrations.Migration{migrations.TxsV3}
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
