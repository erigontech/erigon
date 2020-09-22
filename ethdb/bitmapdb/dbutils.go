package bitmapdb

import (
	"bytes"
	"errors"
	"github.com/RoaringBitmap/roaring"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

// PutMergeByOr - puts bitmap with recent changes into database by merging it with existing bitmap. Merge by OR.
func PutMergeByOr(c ethdb.Cursor, k []byte, delta *roaring.Bitmap) error {
	//defer func(t time.Time) { fmt.Printf("dbutils.go:14: %s\n", time.Since(t)) }(time.Now())
	v, err := c.SeekExact(k)
	if err != nil {
		return err
	}

	if len(v) > 0 { // if found record in db - then get 'min' from db's value, otherwise get it from incoming bitmap
		existing := roaring.New()
		//_, err = existing.FromBuffer(v)
		_, err = existing.ReadFrom(bytes.NewReader(v))
		if err != nil {
			return err
		}
		delta.Or(existing)
	}

	delta.RunOptimize()
	newV, err := c.Reserve(k, int(delta.GetSerializedSizeInBytes()))
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
	_, err = bm.ReadFrom(bytes.NewReader(v))
	//_, err = bm.FromBuffer(v) // by some
	if err != nil {
		return err
	}

	bm.RemoveRange(from, to)

	if bm.GetCardinality() == 0 { // don't store empty bitmaps
		return db.Delete(bucket, k)
	}

	bm.RunOptimize()
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
	_, err = bm.ReadFrom(bytes.NewReader(v))
	//_, err = bm.FromBuffer(v)
	return bm, err
}
