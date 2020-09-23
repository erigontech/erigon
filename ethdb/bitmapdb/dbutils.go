package bitmapdb

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/RoaringBitmap/roaring"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"time"
)

func PutMergeByOr(c ethdb.Cursor, k []byte, delta *roaring.Bitmap) error {
	t := time.Now()
	v, err := c.SeekExact(k)
	if err != nil {
		panic(err)
	}

	if len(v) > 0 {
		existing := roaring.New()
		_, err = existing.ReadFrom(bytes.NewReader(v))
		if err != nil {
			return err
		}

		delta.Or(existing)
	}

	//delta.RunOptimize()
	bufBytes, err := c.Reserve(k, int(delta.GetSerializedSizeInBytes()))
	if err != nil {
		panic(err)
	}

	_, err = delta.WriteTo(bytes.NewBuffer(bufBytes[:0]))
	if err != nil {
		return err
	}
	s := time.Since(t)
	if s > 10*time.Millisecond {
		fmt.Printf("2: %x %s %d\n", k, s, len(bufBytes))
		delta.RunOptimize()
		fmt.Printf("2: card=%d, serializeSize=%d\n", delta.GetCardinality(), delta.GetSerializedSizeInBytes())
	}
	return nil
}

// RemoveRange - gets existing bitmap in db and call RemoveRange operator on it.
// !Important: [from, to)
func RemoveRange(db ethdb.MinDatabase, bucket string, k []byte, from, to uint64) error {
	t := time.Now()
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
	newV := make([]byte, int(bm.GetSerializedSizeInBytes()))
	_, err = bm.WriteTo(bytes.NewBuffer(newV[:0]))
	if err != nil {
		return err
	}
	err = db.Put(bucket, k, newV)
	if err != nil {
		return err
	}
	s := time.Since(t)
	if s > 10*time.Millisecond {
		fmt.Printf("3: %x %s %d\n", k, s, len(newV))
		fmt.Printf("3: card=%d, serializeSize=%d\n", bm.GetCardinality(), bm.GetSerializedSizeInBytes())
	}
	return nil
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
