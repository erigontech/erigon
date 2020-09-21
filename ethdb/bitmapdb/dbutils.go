package bitmapdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"github.com/RoaringBitmap/roaring"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

// PutMergeByOr - puts bitmap with recent changes into database by merging it with existing bitmap. Merge by OR.
func PutMergeByOr(c ethdb.Cursor, k []byte, delta *roaring.Bitmap) error {
	v, err := c.SeekExact(k)
	if err != nil && !errors.Is(err, ethdb.ErrKeyNotFound) {
		return err
	}

	existing := roaring.New()
	_, err = existing.ReadFrom(bytes.NewReader(v))
	if err != nil {
		return err
	}

	delta.Or(existing)

	bufBytes, err := c.Reserve(k, int(delta.GetSerializedSizeInBytes()))
	if err != nil {
		return err
	}

	_, err = delta.WriteTo(bytes.NewBuffer(bufBytes[:0]))
	if err != nil {
		return err
	}
	return nil
}

// RemoveRange - gets existing bitmap in db and call RemoveRange operator on it.
func RemoveRange(c ethdb.Cursor, k []byte, from, to uint64) error {
	v, err := c.SeekExact(k)
	if err != nil {
		if errors.Is(err, ethdb.ErrKeyNotFound) {
			return nil
		}
		return err
	}

	bm := roaring.New()
	_, err = bm.ReadFrom(bytes.NewReader(v))
	if err != nil {
		return err
	}

	bm.RemoveRange(from, to)

	if bm.GetCardinality() == 0 { // don't store empty bitmaps
		return c.DeleteCurrent()
	}

	bufBytes, err := c.Reserve(k, int(bm.GetSerializedSizeInBytes()))
	if err != nil {
		return err
	}

	buf := bytes.NewBuffer(bufBytes[:0])
	_, err = bm.WriteTo(buf)
	if err != nil {
		return err
	}
	return nil
}

// Get - gets bitmap from database
func Get(db ethdb.Getter, bucket string, k []byte) (*roaring.Bitmap, error) {
	bitmapBytes, err := db.Get(bucket, k)
	if err != nil {
		return nil, err
	}
	m := roaring.New()
	_, err = m.ReadFrom(bytes.NewReader(bitmapBytes))
	return m, err
}

type noLeadingZeroes struct{}

// NoLeadingZeroes - set of methods - do work with compressed data in database
// compression is done by deducting 'min' element from whole set before serialization
// compressedBitmap := AddOffset(bm, -bm.Minimum())
// it using first 4 bytes of value in database to store minimum_u32
//
// uses roaring.AddOffset64 method - because roaring.AddOffset is just an alias which doesn't support negative offsets
var NoLeadingZeroes noLeadingZeroes

// PutMergeByOr - puts bitmap with recent changes into database by merging it with existing bitmap. Merge by OR.
func (noLeadingZeroes) PutMergeByOr(db ethdb.MinDatabase, bucket string, k []byte, delta *roaring.Bitmap) error {
	v, err := db.Get(bucket, k)
	if err != nil && !errors.Is(err, ethdb.ErrKeyNotFound) {
		return err
	}

	var min uint32
	if len(v) > 0 { // if found record in db - then get 'min' from db's value, otherwise get it from incoming bitmap
		min = binary.BigEndian.Uint32(v[:4])
		delta = roaring.AddOffset64(delta, -int64(min))
		existing := roaring.New()
		_, err = existing.ReadFrom(bytes.NewReader(v[4:]))
		if err != nil {
			return err
		}
		delta.Or(existing)
	} else {
		min = delta.Minimum()
		delta = roaring.AddOffset64(delta, -int64(min))
	}

	delta.RunOptimize()
	newV := make([]byte, int(4+delta.GetSerializedSizeInBytes()))
	binary.BigEndian.PutUint32(newV[:4], min)
	bufForBitmap := newV[4:]
	_, err = delta.WriteTo(bytes.NewBuffer(bufForBitmap[:0]))
	if err != nil {
		return err
	}
	return db.Put(bucket, k, newV)
}

// RemoveRange - gets existing bitmap in db and call roaring.Bitmap.RemoveRange operator on it.
// [from, to)
func (noLeadingZeroes) RemoveRange(db ethdb.MinDatabase, bucket string, k []byte, from, to uint64) error {
	v, err := db.Get(bucket, k)
	if err != nil {
		if errors.Is(err, ethdb.ErrKeyNotFound) {
			return nil
		}
		return err
	}

	bm := roaring.New()
	min := binary.BigEndian.Uint32(v[:4])
	_, err = bm.ReadFrom(bytes.NewReader(v[4:]))
	if err != nil {
		panic(err)
	}

	bm.RemoveRange(from-uint64(min), to-uint64(min))
	if bm.GetCardinality() == 0 { // don't store empty bitmaps
		return db.Delete(bucket, k)
	}

	bm.RunOptimize()
	newV := make([]byte, int(4+bm.GetSerializedSizeInBytes()))
	binary.BigEndian.PutUint32(newV[:4], min)
	bufForBitmap := newV[4:]
	_, err = bm.WriteTo(bytes.NewBuffer(bufForBitmap[:0]))
	if err != nil {
		return err
	}
	return db.Put(bucket, k, newV)
}

func (noLeadingZeroes) Get(db ethdb.Getter, bucket string, k []byte) (*roaring.Bitmap, error) {
	v, err := db.Get(bucket, k)
	if err != nil {
		return nil, err
	}

	min := binary.BigEndian.Uint32(v[:4])
	bm := roaring.New()
	_, err = bm.ReadFrom(bytes.NewReader(v[4:]))
	if err != nil {
		return nil, err
	}
	bm = roaring.AddOffset64(bm, int64(min))
	return bm, err
}
