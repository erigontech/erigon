package ethdb

import (
	"bytes"
	"context"
	"fmt"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"testing"
)

func TestSnapshotGet(t *testing.T) {
	sn1 := NewLMDB().WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
		return dbutils.BucketsCfg{
			dbutils.HeaderPrefix: dbutils.BucketConfigItem{},
		}
	}).InMem().MustOpen()
	err := sn1.Update(context.Background(), func(tx Tx) error {
		bucket := tx.Cursor(dbutils.HeaderPrefix)
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
	err = sn2.Update(context.Background(), func(tx Tx) error {
		bucket := tx.Cursor(dbutils.BlockBodyPrefix)
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
	err = mainDB.Update(context.Background(), func(tx Tx) error {
		bucket := tx.Cursor(dbutils.HeaderPrefix)
		innerErr := bucket.Put(dbutils.HeaderKey(2, common.Hash{2}), []byte{22})
		if innerErr != nil {
			return innerErr
		}
		innerErr = bucket.Put(dbutils.HeaderKey(3, common.Hash{3}), []byte{33})
		if innerErr != nil {
			return innerErr
		}

		bucket = tx.Cursor(dbutils.BlockBodyPrefix)
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

	kv := NewSnapshotKV().For(dbutils.HeaderPrefix).SnapshotDB(sn1).DB(mainDB).MustOpen()
	kv = NewSnapshotKV().For(dbutils.BlockBodyPrefix).SnapshotDB(sn2).DB(kv).MustOpen()

	tx, err := kv.Begin(context.Background(), nil, RO)
	if err != nil {
		t.Fatal(err)
	}

	v, err := tx.GetOne(dbutils.HeaderPrefix, dbutils.HeaderKey(1, common.Hash{1}))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(v, []byte{1}) {
		t.Fatal(v)
	}

	v, err = tx.GetOne(dbutils.HeaderPrefix, dbutils.HeaderKey(2, common.Hash{2}))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(v, []byte{22}) {
		t.Fatal(v)
	}

	v, err = tx.GetOne(dbutils.HeaderPrefix, dbutils.HeaderKey(3, common.Hash{3}))
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

	headerCursor := tx.Cursor(dbutils.HeaderPrefix)
	k, v, err := headerCursor.Last()
	if err != nil {
		t.Fatal(err)
	}
	if !(bytes.Equal(dbutils.HeaderKey(3, common.Hash{3}), k) && bytes.Equal(v, []byte{33})) {
		t.Fatal(k, v)
	}
	k, v, err = headerCursor.First()
	if err != nil {
		t.Fatal(err)
	}
	if !(bytes.Equal(dbutils.HeaderKey(1, common.Hash{1}), k) && bytes.Equal(v, []byte{1})) {
		t.Fatal(k, v)
	}

	k, v, err = headerCursor.Next()
	if err != nil {
		t.Fatal(err)
	}

	if !(bytes.Equal(dbutils.HeaderKey(2, common.Hash{2}), k) && bytes.Equal(v, []byte{22})) {
		t.Fatal(k, v)
	}

	k, v, err = headerCursor.Next()
	if err != nil {
		t.Fatal(err)
	}

	if !(bytes.Equal(dbutils.HeaderKey(3, common.Hash{3}), k) && bytes.Equal(v, []byte{33})) {
		t.Fatal(k, v)
	}

	k, v, err = headerCursor.Next()
	if err != nil {
		t.Fatal(err)
	}

	if !(bytes.Equal([]byte{}, k) && bytes.Equal(v, []byte{})) {
		t.Fatal(k, v)
	}
}

func TestSnapshotWritableTxAndGet(t *testing.T) {
	sn1 := NewLMDB().WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
		return dbutils.BucketsCfg{
			dbutils.HeaderPrefix: dbutils.BucketConfigItem{},
		}
	}).InMem().MustOpen()
	err := sn1.Update(context.Background(), func(tx Tx) error {
		bucket := tx.Cursor(dbutils.HeaderPrefix)
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
	err = sn2.Update(context.Background(), func(tx Tx) error {
		bucket := tx.Cursor(dbutils.BlockBodyPrefix)
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

	kv := NewSnapshotKV().For(dbutils.HeaderPrefix).SnapshotDB(sn1).DB(mainDB).MustOpen()
	kv = NewSnapshotKV().For(dbutils.BlockBodyPrefix).SnapshotDB(sn2).DB(kv).MustOpen()

	tx, err := kv.Begin(context.Background(), nil, RW)
	if err != nil {
		t.Fatal(err)
	}

	v, err := tx.GetOne(dbutils.HeaderPrefix, dbutils.HeaderKey(1, common.Hash{1}))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(v, []byte{1}) {
		t.Fatal(v)
	}

	v, err = tx.GetOne(dbutils.BlockBodyPrefix, dbutils.BlockBodyKey(1, common.Hash{1}))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(v, []byte{1}) {
		t.Fatal(v)
	}

	err = tx.Cursor(dbutils.BlockBodyPrefix).Put(dbutils.BlockBodyKey(4, common.Hash{4}), []byte{4})
	if err != nil {
		t.Fatal(err)
	}
	err = tx.Cursor(dbutils.HeaderPrefix).Put(dbutils.HeaderKey(4, common.Hash{4}), []byte{4})
	if err != nil {
		t.Fatal(err)
	}
	err = tx.Commit(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	tx, err = kv.Begin(context.Background(), nil, RO)
	if err != nil {
		t.Fatal(err)
	}
	c := tx.Cursor(dbutils.HeaderPrefix)
	k, v, err := c.First()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(k, dbutils.HeaderKey(1, common.Hash{1})) {
		t.Fatal(k, v)
	}

	k, v, err = c.Next()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(k, dbutils.HeaderKey(2, common.Hash{2})) {
		t.Fatal()
	}

	k, v, err = c.Next()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(k, dbutils.HeaderKey(4, common.Hash{4})) {
		t.Fatal()
	}
	k, v, err = c.Next()
	if k != nil || v != nil || err != nil {
		t.Fatal(k, v, err)
	}

	c = tx.Cursor(dbutils.BlockBodyPrefix)
	k, v, err = c.First()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(k, dbutils.BlockBodyKey(1, common.Hash{1})) {
		t.Fatal(k, v)
	}

	k, v, err = c.Next()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(k, dbutils.BlockBodyKey(2, common.Hash{2})) {
		t.Fatal()
	}

	k, v, err = c.Next()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(k, dbutils.BlockBodyKey(4, common.Hash{4})) {
		t.Fatal()
	}
	k, v, err = c.Next()
	if k != nil || v != nil || err != nil {
		t.Fatal(k, v, err)
	}
}

func TestSnapshot2Get(t *testing.T) {
	sn1 := NewLMDB().WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
		return dbutils.BucketsCfg{
			dbutils.HeaderPrefix: dbutils.BucketConfigItem{},
		}
	}).InMem().MustOpen()
	err := sn1.Update(context.Background(), func(tx Tx) error {
		bucket := tx.Cursor(dbutils.HeaderPrefix)
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
	err = sn2.Update(context.Background(), func(tx Tx) error {
		bucket := tx.Cursor(dbutils.BlockBodyPrefix)
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
	err = mainDB.Update(context.Background(), func(tx Tx) error {
		bucket := tx.Cursor(dbutils.HeaderPrefix)
		innerErr := bucket.Put(dbutils.HeaderKey(2, common.Hash{2}), []byte{22})
		if innerErr != nil {
			return innerErr
		}
		innerErr = bucket.Put(dbutils.HeaderKey(3, common.Hash{3}), []byte{33})
		if innerErr != nil {
			return innerErr
		}

		bucket = tx.Cursor(dbutils.BlockBodyPrefix)
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

	kv := NewSnapshot2KV().DB(mainDB).SnapshotDB([]string{dbutils.HeaderPrefix}, sn1).
		SnapshotDB([]string{dbutils.BlockBodyPrefix}, sn2).MustOpen()

	tx, err := kv.Begin(context.Background(), nil, RO)
	if err != nil {
		t.Fatal(err)
	}

	v, err := tx.GetOne(dbutils.HeaderPrefix, dbutils.HeaderKey(1, common.Hash{1}))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(v, []byte{1}) {
		t.Fatal(v)
	}

	v, err = tx.GetOne(dbutils.HeaderPrefix, dbutils.HeaderKey(2, common.Hash{2}))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(v, []byte{22}) {
		t.Fatal(v)
	}

	v, err = tx.GetOne(dbutils.HeaderPrefix, dbutils.HeaderKey(3, common.Hash{3}))
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

	headerCursor := tx.Cursor(dbutils.HeaderPrefix)
	k, v, err := headerCursor.Last()
	if err != nil {
		t.Fatal(err)
	}
	if !(bytes.Equal(dbutils.HeaderKey(3, common.Hash{3}), k) && bytes.Equal(v, []byte{33})) {
		t.Fatal(k, v)
	}
	k, v, err = headerCursor.First()
	if err != nil {
		t.Fatal(err)
	}
	if !(bytes.Equal(dbutils.HeaderKey(1, common.Hash{1}), k) && bytes.Equal(v, []byte{1})) {
		t.Fatal(k, v)
	}

	k, v, err = headerCursor.Next()
	if err != nil {
		t.Fatal(err)
	}

	if !(bytes.Equal(dbutils.HeaderKey(2, common.Hash{2}), k) && bytes.Equal(v, []byte{22})) {
		t.Fatal(k, v)
	}

	k, v, err = headerCursor.Next()
	if err != nil {
		t.Fatal(err)
	}

	if !(bytes.Equal(dbutils.HeaderKey(3, common.Hash{3}), k) && bytes.Equal(v, []byte{33})) {
		t.Fatal(k, v)
	}

	k, v, err = headerCursor.Next()
	if err != nil {
		t.Fatal(err)
	}

	if !(bytes.Equal([]byte{}, k) && bytes.Equal(v, []byte{})) {
		t.Fatal(k, v)
	}
}

func TestSnapshot2WritableTxAndGet(t *testing.T) {
	sn1 := NewLMDB().WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
		return dbutils.BucketsCfg{
			dbutils.HeaderPrefix: dbutils.BucketConfigItem{},
		}
	}).InMem().MustOpen()
	err := sn1.Update(context.Background(), func(tx Tx) error {
		bucket := tx.Cursor(dbutils.HeaderPrefix)
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
	err = sn2.Update(context.Background(), func(tx Tx) error {
		bucket := tx.Cursor(dbutils.BlockBodyPrefix)
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

	kv := NewSnapshot2KV().DB(mainDB).SnapshotDB([]string{dbutils.HeaderPrefix}, sn1).
		SnapshotDB([]string{dbutils.BlockBodyPrefix}, sn2).MustOpen()
	tx, err := kv.Begin(context.Background(), nil, RW)
	if err != nil {
		t.Fatal(err)
	}

	v, err := tx.GetOne(dbutils.HeaderPrefix, dbutils.HeaderKey(1, common.Hash{1}))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(v, []byte{1}) {
		t.Fatal(v)
	}

	v, err = tx.GetOne(dbutils.BlockBodyPrefix, dbutils.BlockBodyKey(1, common.Hash{1}))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(v, []byte{1}) {
		t.Fatal(v)
	}

	err = tx.Cursor(dbutils.BlockBodyPrefix).Put(dbutils.BlockBodyKey(4, common.Hash{4}), []byte{4})
	if err != nil {
		t.Fatal(err)
	}
	err = tx.Cursor(dbutils.HeaderPrefix).Put(dbutils.HeaderKey(4, common.Hash{4}), []byte{4})
	if err != nil {
		t.Fatal(err)
	}
	err = tx.Commit(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	tx, err = kv.Begin(context.Background(), nil, RO)
	if err != nil {
		t.Fatal(err)
	}
	c := tx.Cursor(dbutils.HeaderPrefix)
	k, v, err := c.First()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(k, dbutils.HeaderKey(1, common.Hash{1})) {
		t.Fatal(k, v)
	}

	k, v, err = c.Next()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(k, dbutils.HeaderKey(2, common.Hash{2})) {
		t.Fatal(common.Bytes2Hex(k))
	}

	k, v, err = c.Next()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(k, dbutils.HeaderKey(4, common.Hash{4})) {
		t.Fatal("invalid key", common.Bytes2Hex(k))
	}
	k, v, err = c.Next()
	if k != nil || v != nil || err != nil {
		t.Fatal(k, v, err)
	}

	c = tx.Cursor(dbutils.BlockBodyPrefix)
	k, v, err = c.First()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(k, dbutils.BlockBodyKey(1, common.Hash{1})) {
		t.Fatal(k, v)
	}

	k, v, err = c.Next()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(k, dbutils.BlockBodyKey(2, common.Hash{2})) {
		t.Fatal()
	}

	k, v, err = c.Next()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(k, dbutils.BlockBodyKey(4, common.Hash{4})) {
		t.Fatal()
	}
	k, v, err = c.Next()
	if k != nil || v != nil || err != nil {
		t.Fatal(k, v, err)
	}
}

func genStateData(data []kvData) (KV, error) {
	snapshot := NewLMDB().WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
		return dbutils.BucketsCfg{
			dbutils.PlainStateBucket: dbutils.BucketConfigItem{},
		}
	}).InMem().MustOpen()

	err := snapshot.Update(context.Background(), func(tx Tx) error {
		c := tx.Cursor(dbutils.PlainStateBucket)
		for i := range data {
			innerErr := c.Put(data[i].K, data[i].V)
			if innerErr != nil {
				return innerErr
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return snapshot, nil
}

type kvData struct {
	K []byte
	V []byte
}

func TestSnapshot2WritableTxWalkReplaceAndCreateNewKey(t *testing.T) {
	data := []kvData{}
	for i := 1; i < 3; i++ {
		for j := 1; j < 3; j++ {
			data = append(data, kvData{
				K: dbutils.PlainGenerateCompositeStorageKey(common.Address{uint8(i) * 2}, 1, common.Hash{uint8(j) * 2}),
				V: []byte{uint8(i) * 2, uint8(j) * 2},
			})
		}
	}
	snapshotDB, err := genStateData(data)
	if err != nil {
		t.Fatal(err)
	}
	mainDB := NewLMDB().InMem().MustOpen()

	kv := NewSnapshot2KV().DB(mainDB).SnapshotDB([]string{dbutils.PlainStateBucket}, snapshotDB).
		MustOpen()

	tx, err := kv.Begin(context.Background(), nil, RW)
	if err != nil {
		t.Fatal(err)
	}

	c := tx.Cursor(dbutils.PlainStateBucket)
	replaceKey := dbutils.PlainGenerateCompositeStorageKey(common.Address{2}, 1, common.Hash{4})
	replaceValue := []byte{2, 4, 4}
	newKey := dbutils.PlainGenerateCompositeStorageKey(common.Address{2}, 1, common.Hash{5})
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
	data := []kvData{
		{K: []byte{1}, V: []byte{1}},
		{K: []byte{2}, V: []byte{2}},
		{K: []byte{3}, V: []byte{3}},
		{K: []byte{4}, V: []byte{4}},
		{K: []byte{5}, V: []byte{5}},
	}
	snapshotDB, err := genStateData(data)
	if err != nil {
		t.Fatal(err)
	}

	mainDB := NewLMDB().InMem().MustOpen()
	kv := NewSnapshot2KV().DB(mainDB).SnapshotDB([]string{dbutils.PlainStateBucket}, snapshotDB).
		MustOpen()

	tx, err := kv.Begin(context.Background(), nil, RW)
	if err != nil {
		t.Fatal(err)
	}

	c := tx.Cursor(dbutils.PlainStateBucket)
	deleteCursor := tx.Cursor(dbutils.PlainStateBucket)

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

	t.Log(c.First())
	t.Log(c.Next())
	t.Log(c.Next())
	t.Log(c.Next())
	k, v, err = c.Prev()
	if err != nil {
		t.Fatal(err)
	}
	checkKV(t, k, v, data[3].K, data[3].V)

	k, v, err = c.Prev()
	if err != nil {
		t.Fatal(err)
	}
	checkKV(t, k, v, data[0].K, data[0].V)

}


func TestSnapshot2WritableTxNextAndPrevAndDeleteKey(t *testing.T) {
	data := []kvData{
		{K: []byte{1}, V: []byte{1}},
		{K: []byte{2}, V: []byte{2}},
		{K: []byte{3}, V: []byte{3}},
		{K: []byte{4}, V: []byte{4}},
		{K: []byte{5}, V: []byte{5}},
	}
	snapshotDB, err := genStateData(data)
	if err != nil {
		t.Fatal(err)
	}

	mainDB := NewLMDB().InMem().MustOpen()
	kv := NewSnapshot2KV().DB(mainDB).SnapshotDB([]string{dbutils.PlainStateBucket}, snapshotDB).
		MustOpen()

	tx, err := kv.Begin(context.Background(), nil, RW)
	if err != nil {
		t.Fatal(err)
	}

	c := tx.Cursor(dbutils.PlainStateBucket)
	//deleteCursor := tx.Cursor(dbutils.PlainStateBucket)

	//get first correct k&v
	k, v, err := c.First()
	if err != nil {
		t.Fatal(err)
	}
	checkKV(t, k, v, data[0].K, data[0].V)

	for i:=1; i<len(data); i++ {
		k, v, err = c.Next()
		if err != nil {
			t.Fatal(err)
		}
		checkKV(t, k, v, data[i].K, data[i].V)
		k, v, err = c.Current()
		checkKV(t, k, v, data[i].K, data[i].V)

	}

	// check the key that we've replaced value
	k, v, err = c.Next()
	if err != nil {
		t.Fatal(err)
	}
	checkKV(t, k, v, nil, nil)

	for i:=len(data)-2; i>=0; i-- {
		k, v, err = c.Prev()
		if err != nil {
			t.Fatal(err)
		}
		checkKV(t, k, v, data[i].K, data[i].V)
		k, v, err = c.Current()
		checkKV(t, k, v, data[i].K, data[i].V)
	}
}
func TestSnapshot2WritableTxWalkLastElementIsSnapshot(t *testing.T) {
	snapshotData := []kvData{
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
	mainData := []kvData{
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
	snapshotDB, err := genStateData(snapshotData)
	if err != nil {
		t.Fatal(err)
	}
	mainDB, err := genStateData(mainData)

	kv := NewSnapshot2KV().DB(mainDB).SnapshotDB([]string{dbutils.PlainStateBucket}, snapshotDB).
		MustOpen()

	tx, err := kv.Begin(context.Background(), nil, RW)
	if err != nil {
		t.Fatal(err)
	}

	c := tx.Cursor(dbutils.PlainStateBucket)
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
	snapshotData := []kvData{
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
	mainData := []kvData{
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
	data:=[]kvData {
		mainData[0],
		mainData[1],
		mainData[2],
		snapshotData[1],
	}
	snapshotDB, err := genStateData(snapshotData)
	if err != nil {
		t.Fatal(err)
	}
	mainDB, err := genStateData(mainData)

	kv := NewSnapshot2KV().DB(mainDB).SnapshotDB([]string{dbutils.PlainStateBucket}, snapshotDB).
		MustOpen()

	tx, err := kv.Begin(context.Background(), nil, RW)
	if err != nil {
		t.Fatal(err)
	}

	c := tx.Cursor(dbutils.PlainStateBucket)
	//get first correct k&v
	k, v, err := c.First()
	if err != nil {
		t.Fatal(err)
	}
	checkKV(t, k, v, data[0].K, data[0].V)

	for i:=1; i< len(data); i++ {
		k, v, err = c.Next()
		if err != nil {
			t.Fatal(err)
		}
		checkKV(t, k, v, data[i].K, data[i].V)
		k, v, err = c.Current()
		checkKV(t, k, v, data[i].K, data[i].V)

	}

	for i:=len(data)-2; i>0; i-- {
		k, v, err = c.Prev()
		if err != nil {
			t.Fatal(err)
		}
		checkKV(t, k, v, data[i].K, data[i].V)
		k, v, err = c.Current()
		checkKV(t, k, v, data[i].K, data[i].V)
	}

	k,v,err=c.Last()
	if err != nil {
		t.Fatal(err)
	}
	checkKV(t, k, v, data[len(data)-1].K, data[len(data)-1].V)
	k, v, err = c.Current()
	checkKV(t, k, v, data[len(data)-1].K, data[len(data)-1].V)


	for i:=len(data)-2; i>0; i-- {
		k, v, err = c.Prev()
		if err != nil {
			t.Fatal(err)
		}
		checkKV(t, k, v, data[i].K, data[i].V)
		k, v, err = c.Current()
		checkKV(t, k, v, data[i].K, data[i].V)
	}

}

func printBucket(kv KV, bucket string) {
	fmt.Println("+Print bucket", bucket)
	defer func() {
		fmt.Println("-Print bucket", bucket)
	}()
	err := kv.View(context.Background(), func(tx Tx) error {
		c := tx.Cursor(bucket)
		k, v, err := c.First()
		if err != nil {
			panic(fmt.Errorf("First err: %w", err))
		}
		for k != nil && v != nil {
			fmt.Println("k:=", common.Bytes2Hex(k), "v:=", common.Bytes2Hex(v))
			k, v, err = c.Next()
			if err != nil {
				panic(fmt.Errorf("Next err: %w", err))
			}
		}
		return nil
	})
	fmt.Println("Print err", err)
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
