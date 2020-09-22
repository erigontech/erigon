package bitmapdb

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/RoaringBitmap/roaring"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

// PutMergeByOr - puts bitmap with recent changes into database by merging it with existing bitmap. Merge by OR.
func PutMergeByOr(db ethdb.DbWithPendingMutations, bucket string, k []byte, delta *roaring.Bitmap) error {
	v, err := db.Get(bucket, k)
	if err != nil && !errors.Is(err, ethdb.ErrKeyNotFound) {
		return err
	}

	if len(v) > 0 { // if found record in db - then get 'min' from db's value, otherwise get it from incoming bitmap
		existing := roaring.New()
		_, err = existing.FromBuffer(v)
		if err != nil {
			return err
		}

		delta.Or(existing)
		fmt.Printf("1: %d\n", delta.GetSerializedSizeInBytes())
		existing.Or(delta)
		fmt.Printf("2: %d\n", existing.GetSerializedSizeInBytes())
	}

	//delta.RunOptimize()
	newV, err := db.Reserve(bucket, k, int(delta.GetSerializedSizeInBytes()))
	if err != nil {
		return err
	}
	_, err = delta.WriteTo(bytes.NewBuffer(newV[:0]))
	return err
}

// RemoveRange - gets existing bitmap in db and call RemoveRange operator on it.
// !Important: [from, to)
func RemoveRange(db ethdb.DbWithPendingMutations, bucket string, k []byte, from, to uint64) error {
	v, err := db.Get(bucket, k)
	if err != nil {
		if errors.Is(err, ethdb.ErrKeyNotFound) {
			return nil
		}
		return err
	}

	bm := roaring.New()
	_, err = bm.FromBuffer(v)
	if err != nil {
		return err
	}

	bm.RemoveRange(from, to)

	if bm.GetCardinality() == 0 { // don't store empty bitmaps
		return db.Delete(bucket, k)
	}

	//bm.RunOptimize()
	newV, err := db.Reserve(bucket, k, int(bm.GetSerializedSizeInBytes()))
	if err != nil {
		return err
	}
	_, err = bm.WriteTo(bytes.NewBuffer(newV[:0]))
	return err
}

// Get - gets bitmap from database
func Get(db ethdb.Getter, bucket string, k []byte) (*roaring.Bitmap, error) {
	v, err := db.Get(bucket, k)
	if err != nil {
		return nil, err
	}
	bm := roaring.New()
	_, err = bm.FromBuffer(v)
	return bm, err
}
