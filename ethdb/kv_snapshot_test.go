package ethdb

import (
	"bytes"
	"context"
	"fmt"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"testing"
	"time"
	"github.com/stretchr/testify/require"
)

func TestSnapshot2Get(t *testing.T) {
	sn1 := NewLMDB().WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
		return dbutils.BucketsCfg{
			dbutils.HeadersBucket: dbutils.BucketConfigItem{},
		}
	}).InMem().MustOpen()
	err := sn1.Update(context.Background(), func(tx RwTx) error {
		bucket, err := tx.RwCursor(dbutils.HeadersBucket)
		if err != nil {
			return err
		}
		innerErr := bucket.Put(dbutils.HeaderKey(1, common.Hash{1}), []byte{1})
		if innerErr != nil {
			return innerErr
		}
		innerErr = bucket.Put(dbutils.HeaderKey(2, common.Hash{2}), []byte{2})
		if innerErr != nil {
			return innerErr
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	sn2 := NewLMDB().WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
		return dbutils.BucketsCfg{
			dbutils.BlockBodyPrefix: dbutils.BucketConfigItem{},
		}
	}).InMem().MustOpen()
	err = sn2.Update(context.Background(), func(tx RwTx) error {
		bucket, err := tx.RwCursor(dbutils.BlockBodyPrefix)
		require.NoError(t, err)
		innerErr := bucket.Put(dbutils.BlockBodyKey(1, common.Hash{1}), []byte{1})
		if innerErr != nil {
			return innerErr
		}
		innerErr = bucket.Put(dbutils.BlockBodyKey(2, common.Hash{2}), []byte{2})
		if innerErr != nil {
			return innerErr
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	mainDB := NewLMDB().InMem().MustOpen()
	err = mainDB.Update(context.Background(), func(tx RwTx) error {
		bucket, err := tx.RwCursor(dbutils.HeadersBucket)
		if err != nil {
			return err
		}
		innerErr := bucket.Put(dbutils.HeaderKey(2, common.Hash{2}), []byte{22})
		if innerErr != nil {
			return innerErr
		}
		innerErr = bucket.Put(dbutils.HeaderKey(3, common.Hash{3}), []byte{33})
		if innerErr != nil {
			return innerErr
		}

		bucket, err = tx.RwCursor(dbutils.BlockBodyPrefix)
		if err != nil {
			return err
		}

		innerErr = bucket.Put(dbutils.BlockBodyKey(2, common.Hash{2}), []byte{22})
		if innerErr != nil {
			return innerErr
		}
		innerErr = bucket.Put(dbutils.BlockBodyKey(3, common.Hash{3}), []byte{33})
		if innerErr != nil {
			return innerErr
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	kv := NewSnapshotKV().DB(mainDB).SnapshotDB([]string{dbutils.HeadersBucket}, sn1).
		SnapshotDB([]string{dbutils.BlockBodyPrefix}, sn2).Open()

	tx, err := kv.BeginRo(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	v, err := tx.GetOne(dbutils.HeadersBucket, dbutils.HeaderKey(1, common.Hash{1}))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(v, []byte{1}) {
		t.Fatal(v)
	}

	v, err = tx.GetOne(dbutils.HeadersBucket, dbutils.HeaderKey(2, common.Hash{2}))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(v, []byte{22}) {
		t.Fatal(v)
	}

	v, err = tx.GetOne(dbutils.HeadersBucket, dbutils.HeaderKey(3, common.Hash{3}))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(v, []byte{33}) {
		t.Fatal(v)
	}

	v, err = tx.GetOne(dbutils.BlockBodyPrefix, dbutils.BlockBodyKey(1, common.Hash{1}))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(v, []byte{1}) {
		t.Fatal(v)
	}

	v, err = tx.GetOne(dbutils.BlockBodyPrefix, dbutils.BlockBodyKey(2, common.Hash{2}))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(v, []byte{22}) {
		t.Fatal(v)
	}

	v, err = tx.GetOne(dbutils.BlockBodyPrefix, dbutils.BlockBodyKey(3, common.Hash{3}))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(v, []byte{33}) {
		t.Fatal(v)
	}

	headerCursor, err := tx.Cursor(dbutils.HeadersBucket)
	require.NoError(t, err)
	k, v, err := headerCursor.Last()
	require.NoError(t, err)
	if !(bytes.Equal(dbutils.HeaderKey(3, common.Hash{3}), k) && bytes.Equal(v, []byte{33})) {
		t.Fatal(k, v)
	}
	k, v, err = headerCursor.First()
	require.NoError(t, err)
	if !(bytes.Equal(dbutils.HeaderKey(1, common.Hash{1}), k) && bytes.Equal(v, []byte{1})) {
		t.Fatal(k, v)
	}

	k, v, err = headerCursor.Next()
	require.NoError(t, err)

	if !(bytes.Equal(dbutils.HeaderKey(2, common.Hash{2}), k) && bytes.Equal(v, []byte{22})) {
		t.Fatal(k, v)
	}

	k, v, err = headerCursor.Next()
	require.NoError(t, err)

	if !(bytes.Equal(dbutils.HeaderKey(3, common.Hash{3}), k) && bytes.Equal(v, []byte{33})) {
		t.Fatal(k, v)
	}

	k, v, err = headerCursor.Next()
	require.NoError(t, err)

	if !(bytes.Equal([]byte{}, k) && bytes.Equal(v, []byte{})) {
		t.Fatal(k, v)
	}
}

func TestSnapshot2WritableTxAndGet(t *testing.T) {
	sn1 := NewLMDB().WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
		return dbutils.BucketsCfg{
			dbutils.HeadersBucket: dbutils.BucketConfigItem{},
		}
	}).InMem().MustOpen()
	{
		err := sn1.Update(context.Background(), func(tx RwTx) error {
			bucket, err := tx.RwCursor(dbutils.HeadersBucket)
			require.NoError(t, err)
			innerErr := bucket.Put(dbutils.HeaderKey(1, common.Hash{1}), []byte{1})
			if innerErr != nil {
				return innerErr
			}
			innerErr = bucket.Put(dbutils.HeaderKey(2, common.Hash{2}), []byte{2})
			if innerErr != nil {
				return innerErr
			}

			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	sn2 := NewLMDB().WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
		return dbutils.BucketsCfg{
			dbutils.BlockBodyPrefix: dbutils.BucketConfigItem{},
		}
	}).InMem().MustOpen()
	{
		err := sn2.Update(context.Background(), func(tx RwTx) error {
			bucket, err := tx.RwCursor(dbutils.BlockBodyPrefix)
			require.NoError(t, err)
			innerErr := bucket.Put(dbutils.BlockBodyKey(1, common.Hash{1}), []byte{1})
			if innerErr != nil {
				return innerErr
			}
			innerErr = bucket.Put(dbutils.BlockBodyKey(2, common.Hash{2}), []byte{2})
			if innerErr != nil {
				return innerErr
			}

			return nil
		})
		require.NoError(t, err)
	}

	mainDB := NewLMDB().InMem().MustOpen()

	kv := NewSnapshotKV().DB(mainDB).SnapshotDB([]string{dbutils.HeadersBucket}, sn1).
		SnapshotDB([]string{dbutils.BlockBodyPrefix}, sn2).Open()
	{
		tx, err := kv.BeginRw(context.Background())
		require.NoError(t, err)

		v, err := tx.GetOne(dbutils.HeadersBucket, dbutils.HeaderKey(1, common.Hash{1}))
		require.NoError(t, err)
		if !bytes.Equal(v, []byte{1}) {
			t.Fatal(v)
		}

		v, err = tx.GetOne(dbutils.BlockBodyPrefix, dbutils.BlockBodyKey(1, common.Hash{1}))
		require.NoError(t, err)
		if !bytes.Equal(v, []byte{1}) {
			t.Fatal(v)
		}

		err = tx.Put(dbutils.BlockBodyPrefix, dbutils.BlockBodyKey(4, common.Hash{4}), []byte{4})
		require.NoError(t, err)
		err = tx.Put(dbutils.HeadersBucket, dbutils.HeaderKey(4, common.Hash{4}), []byte{4})
		require.NoError(t, err)
		err = tx.Commit()
		require.NoError(t, err)
	}
	tx, err := kv.BeginRo(context.Background())
	require.NoError(t, err)
	c, err := tx.Cursor(dbutils.HeadersBucket)
	require.NoError(t, err)
	k, v, err := c.First()
	require.NoError(t, err)
	if !bytes.Equal(k, dbutils.HeaderKey(1, common.Hash{1})) {
		t.Fatal(k, v)
	}

	k, v, err = c.Next()
	require.NoError(t, err)
	if !bytes.Equal(k, dbutils.HeaderKey(2, common.Hash{2})) {
		t.Fatal(common.Bytes2Hex(k))
	}
	if !bytes.Equal(v, []byte{2}) {
		t.Fatal(common.Bytes2Hex(k))
	}

	k, v, err = c.Next()
	require.NoError(t, err)
	if !bytes.Equal(k, dbutils.HeaderKey(4, common.Hash{4})) {
		t.Fatal("invalid key", common.Bytes2Hex(k))
	}
	if !bytes.Equal(v, []byte{4}) {
		t.Fatal(common.Bytes2Hex(k), common.Bytes2Hex(v))
	}

	k, v, err = c.Next()
	if k != nil || v != nil || err != nil {
		t.Fatal(k, v, err)
	}

	c, err = tx.Cursor(dbutils.BlockBodyPrefix)
	require.NoError(t, err)
	k, v, err = c.First()
	require.NoError(t, err)
	if !bytes.Equal(k, dbutils.BlockBodyKey(1, common.Hash{1})) {
		t.Fatal(k, v)
	}

	k, v, err = c.Next()
	require.NoError(t, err)
	if !bytes.Equal(k, dbutils.BlockBodyKey(2, common.Hash{2})) {
		t.Fatal()
	}
	if !bytes.Equal(v, []byte{2}) {
		t.Fatal(common.Bytes2Hex(k), common.Bytes2Hex(v))
	}

	k, v, err = c.Next()
	require.NoError(t, err)
	if !bytes.Equal(k, dbutils.BlockBodyKey(4, common.Hash{4})) {
		t.Fatal()
	}
	if !bytes.Equal(v, []byte{4}) {
		t.Fatal(common.Bytes2Hex(k), common.Bytes2Hex(v))
	}

	k, v, err = c.Next()
	if k != nil || v != nil || err != nil {
		t.Fatal(k, v, err)
	}
}

func TestSnapshot2WritableTxWalkReplaceAndCreateNewKey(t *testing.T) {
	data := []KvData{}
	for i := 1; i < 3; i++ {
		for j := 1; j < 3; j++ {
			data = append(data, KvData{
				K: dbutils.PlainGenerateCompositeStorageKey([]byte{uint8(i) * 2}, 1, []byte{uint8(j) * 2}),
				V: []byte{uint8(i) * 2, uint8(j) * 2},
			})
		}
	}
	snapshotDB, err := GenStateData(data)
	if err != nil {
		t.Fatal(err)
	}
	mainDB := NewLMDB().InMem().MustOpen()

	kv := NewSnapshotKV().DB(mainDB).SnapshotDB([]string{dbutils.PlainStateBucket}, snapshotDB).
		Open()

	tx, err := kv.BeginRw(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	c, err := tx.RwCursor(dbutils.PlainStateBucket)
	require.NoError(t, err)
	replaceKey := dbutils.PlainGenerateCompositeStorageKey([]byte{2}, 1, []byte{4})
	replaceValue := []byte{2, 4, 4}
	newKey := dbutils.PlainGenerateCompositeStorageKey([]byte{2}, 1, []byte{5})
	newValue := []byte{2, 5}

	//get first correct k&v
	k, v, err := c.First()
	if err != nil {
		t.Fatal(err)
	}
	checkKV(t, k, v, data[0].K, data[0].V)
	if !(bytes.Equal(k, data[0].K) || bytes.Equal(v, data[0].V)) {
		t.Fatal(k, data[0].K, v, data[0].V)
	}
	err = c.Put(replaceKey, replaceValue)
	if err != nil {
		t.Fatal(err)
	}

	// check the key that we've replaced value
	k, v, err = c.Next()
	if err != nil {
		t.Fatal(err)
	}
	checkKV(t, k, v, replaceKey, replaceValue)

	err = c.Put(newKey, newValue)
	if err != nil {
		t.Fatal(err)
	}
	// check the key that we've inserted
	k, v, err = c.Next()
	if err != nil {
		t.Fatal(err)
	}
	checkKV(t, k, v, newKey, newValue)

	//check the rest keys
	k, v, err = c.Next()
	if err != nil {
		t.Fatal(err)
	}
	checkKV(t, k, v, data[2].K, data[2].V)
}

func TestSnapshot2WritableTxWalkAndDeleteKey(t *testing.T) {
	data := []KvData{
		{K: []byte{1}, V: []byte{1}},
		{K: []byte{2}, V: []byte{2}},
		{K: []byte{3}, V: []byte{3}},
		{K: []byte{4}, V: []byte{4}},
		{K: []byte{5}, V: []byte{5}},
	}
	snapshotDB, err := GenStateData(data)
	if err != nil {
		t.Fatal(err)
	}

	mainDB := NewLMDB().InMem().MustOpen()
	kv := NewSnapshotKV().DB(mainDB).SnapshotDB([]string{dbutils.PlainStateBucket}, snapshotDB).
		Open()

	tx, err := kv.BeginRw(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	c, err := tx.Cursor(dbutils.PlainStateBucket)
	require.NoError(t, err)
	deleteCursor, err := tx.RwCursor(dbutils.PlainStateBucket)
	require.NoError(t, err)

	//get first correct k&v
	k, v, err := c.First()
	if err != nil {
		t.Fatal(err)
	}
	checkKV(t, k, v, data[0].K, data[0].V)

	//remove value
	err = deleteCursor.Delete(data[1].K, nil)
	if err != nil {
		t.Fatal(err)
	}
	err = deleteCursor.Delete(data[2].K, nil)
	if err != nil {
		t.Fatal(err)
	}
	err = deleteCursor.Delete(data[4].K, nil)
	if err != nil {
		t.Fatal(err)
	}

	// check the key that we've replaced value
	k, v, err = c.Next()
	if err != nil {
		t.Fatal(err)
	}
	checkKV(t, k, v, data[3].K, data[3].V)

	k, v, err = c.Next()
	if err != nil {
		t.Fatal(err)
	}
	checkKV(t, k, v, nil, nil)

	//2,3,5 removed. Current 4. Prev -
	k, v, err = c.Prev()
	if err != nil {
		t.Fatal(err)
	}
	checkKV(t, k, v, data[0].K, data[0].V)

	k, v, err = c.Prev()
	if err != nil {
		t.Fatal(err)
	}
	checkKV(t, k, v, nil, nil)
}

func TestSnapshot2WritableTxNextAndPrevAndDeleteKey(t *testing.T) {
	data := []KvData{
		{K: []byte{1}, V: []byte{1}}, //to remove
		{K: []byte{2}, V: []byte{2}},
		{K: []byte{3}, V: []byte{3}},
		{K: []byte{4}, V: []byte{4}}, //to remove
		{K: []byte{5}, V: []byte{5}},
	}
	snapshotDB, err := GenStateData(data)
	if err != nil {
		t.Fatal(err)
	}

	mainDB := NewLMDB().InMem().MustOpen()
	kv := NewSnapshotKV().DB(mainDB).SnapshotDB([]string{dbutils.PlainStateBucket}, snapshotDB).
		Open()

	tx, err := kv.BeginRw(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	c, err := tx.Cursor(dbutils.PlainStateBucket)
	require.NoError(t, err)
	deleteCursor, err := tx.RwCursor(dbutils.PlainStateBucket)
	require.NoError(t, err)

	//get first correct k&v
	k, v, err := c.Last()
	if err != nil {
		t.Fatal(err)
	}
	checkKV(t, k, v, data[len(data)-1].K, data[len(data)-1].V)

	for i := len(data) - 2; i >= 0; i-- {
		k, v, err = c.Prev()
		if err != nil {
			t.Fatal(i, err)
		}
		checkKV(t, k, v, data[i].K, data[i].V)

		k, v, err = c.Current()
		if err != nil {
			t.Fatal(i, err)
		}
		checkKV(t, k, v, data[i].K, data[i].V)
	}

	k, v, err = c.Last()
	if err != nil {
		t.Fatal(err)
	}
	checkKV(t, k, v, data[4].K, data[4].V)

	//remove 4. Current on 5
	err = deleteCursor.Delete(data[3].K, nil)
	if err != nil {
		t.Fatal(err)
	}

	//cursor on 3 after it
	k, v, err = c.Prev()
	if err != nil {
		t.Fatal(err)
	}
	checkKV(t, k, v, data[2].K, data[2].V)

	err = deleteCursor.Delete(data[0].K, nil)
	if err != nil {
		t.Fatal(err)
	}

	k, v, err = c.Prev()
	if err != nil {
		t.Fatal(err)
	}
	checkKV(t, k, v, data[1].K, data[1].V)

	k, v, err = c.Prev()
	if err != nil {
		t.Fatal(err)
	}
	checkKV(t, k, v, nil, nil)

}
func TestSnapshot2WritableTxWalkLastElementIsSnapshot(t *testing.T) {
	snapshotData := []KvData{
		{
			K: []byte{0, 1},
			V: []byte{1},
		},
		{
			K: []byte{0, 4},
			V: []byte{4},
		},
	}
	replacedValue := []byte{1, 1}
	mainData := []KvData{
		{
			K: []byte{0, 1},
			V: replacedValue,
		},
		{
			K: []byte{0, 2},
			V: []byte{2},
		},
		{
			K: []byte{0, 3},
			V: []byte{3},
		},
	}
	snapshotDB, err := GenStateData(snapshotData)
	if err != nil {
		t.Fatal(err)
	}
	mainDB, err := GenStateData(mainData)
	if err != nil {
		t.Fatal(err)
	}

	kv := NewSnapshotKV().DB(mainDB).SnapshotDB([]string{dbutils.PlainStateBucket}, snapshotDB).
		Open()

	tx, err := kv.BeginRw(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	c, err := tx.Cursor(dbutils.PlainStateBucket)
	require.NoError(t, err)
	//get first correct k&v
	k, v, err := c.First()
	if err != nil {
		t.Fatal(err)
	}

	checkKV(t, k, v, mainData[0].K, mainData[0].V)

	k, v, err = c.Next()
	if err != nil {
		t.Fatal(err)
	}
	checkKV(t, k, v, mainData[1].K, mainData[1].V)

	k, v, err = c.Next()
	if err != nil {
		t.Fatal(err)
	}
	checkKV(t, k, v, mainData[2].K, mainData[2].V)

	k, v, err = c.Next()
	if err != nil {
		t.Fatal(err)
	}
	checkKV(t, k, v, snapshotData[1].K, snapshotData[1].V)

	k, v, err = c.Next()
	if err != nil {
		t.Fatal(err)
	}
	checkKV(t, k, v, nil, nil)
}

func TestSnapshot2WritableTxWalkForwardAndBackward(t *testing.T) {
	snapshotData := []KvData{
		{
			K: []byte{0, 1},
			V: []byte{1},
		},
		{
			K: []byte{0, 4},
			V: []byte{4},
		},
	}
	replacedValue := []byte{1, 1}
	mainData := []KvData{
		{
			K: []byte{0, 1},
			V: replacedValue,
		},
		{
			K: []byte{0, 2},
			V: []byte{2},
		},
		{
			K: []byte{0, 3},
			V: []byte{3},
		},
	}
	data := []KvData{
		mainData[0],
		mainData[1],
		mainData[2],
		snapshotData[1],
	}
	snapshotDB, err := GenStateData(snapshotData)
	if err != nil {
		t.Fatal(err)
	}
	mainDB, err := GenStateData(mainData)
	if err != nil {
		t.Fatal(err)
	}

	kv := NewSnapshotKV().DB(mainDB).SnapshotDB([]string{dbutils.PlainStateBucket}, snapshotDB).
		Open()

	tx, err := kv.BeginRw(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	c, err := tx.Cursor(dbutils.PlainStateBucket)
	require.NoError(t, err)
	//get first correct k&v
	k, v, err := c.First()
	if err != nil {
		t.Fatal(err)
	}
	checkKV(t, k, v, data[0].K, data[0].V)

	for i := 1; i < len(data); i++ {
		k, v, err = c.Next()
		if err != nil {
			t.Fatal(err)
		}
		checkKV(t, k, v, data[i].K, data[i].V)

		k, v, err = c.Current()
		if err != nil {
			t.Fatal(err)
		}
		checkKV(t, k, v, data[i].K, data[i].V)
	}

	for i := len(data) - 2; i > 0; i-- {
		k, v, err = c.Prev()
		if err != nil {
			t.Fatal(err)
		}
		checkKV(t, k, v, data[i].K, data[i].V)

		k, v, err = c.Current()
		if err != nil {
			t.Fatal(err)
		}
		checkKV(t, k, v, data[i].K, data[i].V)
	}

	k, v, err = c.Last()
	if err != nil {
		t.Fatal(err)
	}
	checkKV(t, k, v, data[len(data)-1].K, data[len(data)-1].V)
	k, v, err = c.Current()
	if err != nil {
		t.Fatal(err)
	}

	checkKV(t, k, v, data[len(data)-1].K, data[len(data)-1].V)

	for i := len(data) - 2; i > 0; i-- {
		k, v, err = c.Prev()
		if err != nil {
			t.Fatal(err)
		}
		checkKV(t, k, v, data[i].K, data[i].V)

		k, v, err = c.Current()
		if err != nil {
			t.Fatal(err)
		}
		checkKV(t, k, v, data[i].K, data[i].V)
	}

	i := 0
	err = Walk(c, []byte{}, 0, func(k, v []byte) (bool, error) {
		checkKV(t, k, v, data[i].K, data[i].V)
		i++
		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestSnapshot2WalkByEmptyDB(t *testing.T) {
	data := []KvData{
		{K: []byte{1}, V: []byte{1}},
		{K: []byte{2}, V: []byte{2}},
		{K: []byte{3}, V: []byte{3}},
		{K: []byte{4}, V: []byte{4}},
		{K: []byte{5}, V: []byte{5}},
	}
	snapshotDB, err := GenStateData(data)
	if err != nil {
		t.Fatal(err)
	}

	mainDB := NewLMDB().InMem().MustOpen()
	kv := NewSnapshotKV().DB(mainDB).SnapshotDB([]string{dbutils.PlainStateBucket}, snapshotDB).
		Open()

	tx, err := kv.BeginRw(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	c, err := tx.Cursor(dbutils.PlainStateBucket)
	require.NoError(t, err)

	i := 0
	err = Walk(c, []byte{}, 0, func(k, v []byte) (bool, error) {
		checkKV(t, k, v, data[i].K, data[i].V)
		i++
		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}

}

func TestSnapshot2WritablePrevAndDeleteKey(t *testing.T) {
	data := []KvData{
		{K: []byte{1}, V: []byte{1}},
		{K: []byte{2}, V: []byte{2}},
		{K: []byte{3}, V: []byte{3}},
		{K: []byte{4}, V: []byte{4}},
		{K: []byte{5}, V: []byte{5}},
	}
	snapshotDB, err := GenStateData(data)
	if err != nil {
		t.Fatal(err)
	}

	mainDB := NewLMDB().InMem().MustOpen()
	kv := NewSnapshotKV().DB(mainDB).SnapshotDB([]string{dbutils.PlainStateBucket}, snapshotDB).
		Open()

	tx, err := kv.BeginRw(context.Background())
	require.NoError(t, err)
	c, err := tx.Cursor(dbutils.PlainStateBucket)
	require.NoError(t, err)

	//get first correct k&v
	k, v, err := c.First()
	if err != nil {
		printBucket(kv, dbutils.PlainStateBucket)
		t.Fatal(err)
	}
	checkKV(t, k, v, data[0].K, data[0].V)

	for i := 1; i < len(data); i++ {
		k, v, err = c.Next()
		require.NoError(t, err)
		checkKV(t, k, v, data[i].K, data[i].V)

		k, v, err = c.Current()
		require.NoError(t, err)
		checkKV(t, k, v, data[i].K, data[i].V)
	}

	// check the key that we've replaced value
	k, v, err = c.Next()
	if err != nil {
		t.Fatal(err)
	}
	checkKV(t, k, v, nil, nil)

	for i := len(data) - 2; i >= 0; i-- {
		k, v, err = c.Prev()
		if err != nil {
			t.Fatal(err)
		}
		checkKV(t, k, v, data[i].K, data[i].V)

		k, v, err = c.Current()
		if err != nil {
			t.Fatal(err)
		}
		checkKV(t, k, v, data[i].K, data[i].V)
	}
}

func TestSnapshot2WritableTxNextAndPrevWithDeleteAndPutKeys(t *testing.T) {
	data := []KvData{
		{K: []byte{1}, V: []byte{1}},
		{K: []byte{2}, V: []byte{2}},
		{K: []byte{3}, V: []byte{3}},
		{K: []byte{4}, V: []byte{4}},
		{K: []byte{5}, V: []byte{5}},
	}
	snapshotDB, err := GenStateData(data)
	if err != nil {
		t.Fatal(err)
	}

	mainDB := NewLMDB().InMem().MustOpen()
	kv := NewSnapshotKV().DB(mainDB).SnapshotDB([]string{dbutils.PlainStateBucket}, snapshotDB).
		Open()

	tx, err := kv.BeginRw(context.Background())
	require.NoError(t, err)
	c, err := tx.Cursor(dbutils.PlainStateBucket)
	require.NoError(t, err)
	deleteCursor, err := tx.RwCursor(dbutils.PlainStateBucket)
	require.NoError(t, err)

	//get first correct k&v
	k, v, err := c.First()
	if err != nil {
		t.Fatal(err)
	}
	checkKV(t, k, v, data[0].K, data[0].V)

	k, v, err = c.Next()
	if err != nil {
		t.Fatal(err)
	}
	checkKV(t, k, v, data[1].K, data[1].V)

	err = deleteCursor.Delete(data[2].K, nil)
	if err != nil {
		t.Fatal(err)
	}

	k, v, err = c.Next()
	if err != nil {
		t.Fatal(err)
	}
	checkKV(t, k, v, data[3].K, data[3].V)

	k, v, err = c.Prev()
	if err != nil {
		t.Fatal(err)
	}
	checkKV(t, k, v, data[1].K, data[1].V)

	err = deleteCursor.Put(data[2].K, data[2].V)
	if err != nil {
		t.Fatal(err)
	}

	k, v, err = c.Next()
	if err != nil {
		t.Fatal(err)
	}
	checkKV(t, k, v, data[2].K, data[2].V)

	k, v, err = c.Next()
	if err != nil {
		t.Fatal(err)
	}
	checkKV(t, k, v, data[3].K, data[3].V)

	err = deleteCursor.Delete(data[2].K, nil)
	if err != nil {
		t.Fatal(err)
	}

	k, v, err = c.Prev()
	if err != nil {
		t.Fatal(err)
	}
	checkKV(t, k, v, data[1].K, data[1].V)

	k, v, err = c.Prev()
	if err != nil {
		t.Fatal(err)
	}
	checkKV(t, k, v, data[0].K, data[0].V)

}

func TestSnapshotUpdateSnapshot(t *testing.T) {
	data := []KvData{
		{K: []byte{1}, V: []byte{1}},
		{K: []byte{2}, V: []byte{2}},
		{K: []byte{3}, V: []byte{3}},
		{K: []byte{4}, V: []byte{4}},
		{K: []byte{5}, V: []byte{5}},
	}
	snapshotDB, err := GenStateData(data)
	if err != nil {
		t.Fatal(err)
	}

	data2 := append(data, []KvData{
		{K: []byte{6}, V: []byte{6}},
		{K: []byte{7}, V: []byte{7}},
	}...)
	snapshotDB2, err := GenStateData(data2)
	if err != nil {
		t.Fatal(err)
	}

	mainDB := NewLMDB().InMem().MustOpen()
	kv := NewSnapshotKV().DB(mainDB).SnapshotDB([]string{dbutils.PlainStateBucket}, snapshotDB).
		Open()

	tx,err:=kv.BeginRo(context.Background())
	if err!=nil {
		t.Fatal(err)
	}
	c, err:=tx.Cursor(dbutils.PlainStateBucket)
	if err!=nil {
		t.Fatal(err)
	}

	k,v, err:=c.First()
	if err!=nil {
		t.Fatal(err)
	}
	checkKVErr(t, k, v, err, []byte{1}, []byte{1})

	done:=make(chan struct{})
	kv.(*SnapshotKV).UpdateSnapshots([]string{dbutils.PlainStateBucket}, snapshotDB2, done)

	tx2,err:=kv.BeginRo(context.Background())
	if err!=nil {
		t.Fatal(err)
	}

	c2,err:=tx2.Cursor(dbutils.PlainStateBucket)
	if err!=nil {
		t.Fatal(err)
	}

	k2,v2, err2:=c2.First()
	if err2!=nil {
		t.Fatal(err2)
	}
	checkKVErr(t, k2, v2, err2, []byte{1}, []byte{1})

	i:=2
	for  {
		k,v, err=c.Next()
		if err!=nil {
			t.Fatal(err)
		}
		if k==nil {
			break
		}
		checkKVErr(t, k, v, err, []byte{uint8(i)}, []byte{uint8(i)})
		i++
	}
	//data[maxK]+1
	if i!=6 {
		t.Fatal("incorrect last key", i)
	}
	tx.Rollback()


	i=2
	for  {
		k2,v2, err2=c2.Next()
		if err2!=nil {
			t.Fatal(err2)
		}
		if k2==nil {
			break
		}
		checkKVErr(t, k2, v2, err2, []byte{uint8(i)}, []byte{uint8(i)})
		i++
	}
	//data2[maxK]+1
	if i!=8 {
		t.Fatal("incorrect last key", i)
	}

	//a short delay to close
	time.Sleep(time.Second)
	select {
	case <-done:
	default:
		t.Fatal("Hasn't closed database")

	}
}

func printBucket(kv RoKV, bucket string) {
	fmt.Println("+Print bucket", bucket)
	defer func() {
		fmt.Println("-Print bucket", bucket)
	}()
	err := kv.View(context.Background(), func(tx Tx) error {
		c, err := tx.Cursor(bucket)
		if err != nil {
			return err
		}
		k, v, err := c.First()
		if err != nil {
			panic(fmt.Errorf("first err: %w", err))
		}
		for k != nil && v != nil {
			fmt.Println("k:=", common.Bytes2Hex(k), "v:=", common.Bytes2Hex(v))
			k, v, err = c.Next()
			if err != nil {
				panic(fmt.Errorf("next err: %w", err))
			}
		}
		return nil
	})
	fmt.Println("Print err", err)
}

func checkKVErr(t *testing.T,k, v []byte, err error, expectedK, expectedV []byte)  {
	t.Helper()
	if err!=nil {
		t.Fatal(err)
	}
	if !bytes.Equal(k, expectedK) {
		t.Error("k!= expected", k, expectedK)
	}
	if !bytes.Equal(v, expectedV) {
		t.Error("v!= expected", v, expectedV)
	}
}

func checkKV(t *testing.T, key, val, expectedKey, expectedVal []byte) {
	t.Helper()
	if !bytes.Equal(key, expectedKey) {
		t.Log("+", common.Bytes2Hex(expectedKey))
		t.Log("-", common.Bytes2Hex(key))
		t.Fatal("wrong key")
	}
	if !bytes.Equal(val, expectedVal) {
		t.Log("+", common.Bytes2Hex(expectedVal))
		t.Log("-", common.Bytes2Hex(val))
		t.Fatal("wrong value for key", common.Bytes2Hex(key))
	}
}
