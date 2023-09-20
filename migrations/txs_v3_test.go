package migrations_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math/big"
	"testing"
	"time"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon-lib/common/u256"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/migrations"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/log/v3"
	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
)

func TestTxsV3(t *testing.T) {
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
		for i := uint64(1); i < 10; i++ {
			h := &types.Header{Number: big.NewInt(int64(i)), Extra: []byte("fork1")}
			hash := h.Hash()
			err = rawdb.WriteCanonicalHash(tx, hash, i)
			require.NoError(err)
			err = rawdb.WriteHeader(tx, h)
			require.NoError(err)
			_, err = rawdb.WriteRawBody(tx, hash, i, b)
			require.NoError(err)
		}

		err = makeBodiesNonCanonicalDeprecated(tx, 7, context.Background(), "", logEvery)
		require.NoError(err)

		for i := uint64(7); i < 10; i++ {
			require.NoError(err)
			h := &types.Header{Number: big.NewInt(int64(i)), Extra: []byte("fork2")}
			hash := h.Hash()
			err = rawdb.WriteCanonicalHash(tx, hash, i)
			require.NoError(err)
			err = rawdb.WriteHeader(tx, h)
			require.NoError(err)
			_, err = rawdb.WriteRawBody(tx, hash, i, b)
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

	//err = db.View(context.Background(), func(tx kv.Tx) error {
	//	v, err := tx.ReadSequence(kv.EthTx)
	//	require.NoError(err)
	//	require.Equal(uint64(3*10+2*10), v)
	//	return nil
	//})
	//require.NoError(err)

	err = db.View(context.Background(), func(tx kv.Tx) error {
		for i := uint64(7); i < 10; i++ {
			h := &types.Header{Number: big.NewInt(int64(i)), Extra: []byte("fork1")}
			hash := h.Hash()

			has, err := tx.Has(kv.BlockBody, dbutils.BlockBodyKey(i, hash))
			require.NoError(err)
			require.False(has)
		}

		c, err := tx.Cursor(kv.NonCanonicalTxs)
		require.NoError(err)
		cnt, err := c.Count()
		require.NoError(err)
		require.Zero(cnt)

		has, err := tx.Has(kv.EthTx, hexutility.EncodeTs(0))
		require.NoError(err)
		require.False(has)

		return nil
	})
	require.NoError(err)
}

func makeBodiesNonCanonicalDeprecated(tx kv.RwTx, from uint64, ctx context.Context, logPrefix string, logEvery *time.Ticker) error {
	var firstMovedTxnID uint64
	var firstMovedTxnIDIsSet bool
	for blockNum := from; ; blockNum++ {
		h, err := rawdb.ReadCanonicalHash(tx, blockNum)
		if err != nil {
			return err
		}
		if h == (libcommon.Hash{}) {
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
		if !firstMovedTxnIDIsSet {
			firstMovedTxnIDIsSet = true
			firstMovedTxnID = bodyForStorage.BaseTxId
		}

		newBaseId := uint64(0)

		// move txs to NonCanonical bucket, it has own sequence
		newBaseId, err = tx.IncrementSequence(kv.NonCanonicalTxs, uint64(bodyForStorage.TxAmount))
		if err != nil {
			return err
		}

		// next loop does move only non-system txs. need move system-txs manually (because they may not exist)
		i := uint64(0)
		if err := tx.ForAmount(kv.EthTx, hexutility.EncodeTs(bodyForStorage.BaseTxId+1), bodyForStorage.TxAmount-2, func(k, v []byte) error {
			id := newBaseId + 1 + i
			if err := tx.Put(kv.NonCanonicalTxs, hexutility.EncodeTs(id), v); err != nil {
				return err
			}
			if err := tx.Delete(kv.EthTx, k); err != nil {
				return err
			}
			i++
			return nil
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
	if firstMovedTxnIDIsSet {
		c, err := tx.Cursor(kv.EthTx)
		if err != nil {
			return err
		}
		k, _, err := c.Last()
		if err != nil {
			return err
		}
		if k != nil && binary.BigEndian.Uint64(k) >= firstMovedTxnID {
			panic(fmt.Sprintf("must not happen, ResetSequence: %d, lastInDB: %d", firstMovedTxnID, binary.BigEndian.Uint64(k)))
		}

		if err := rawdb.ResetSequence(tx, kv.EthTx, firstMovedTxnID); err != nil {
			return err
		}
	}

	return nil
}
