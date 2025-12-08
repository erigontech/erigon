// Copyright 2022 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package bitmapdb

import (
	"bytes"
	"encoding/binary"
	"math"
	"sort"
	"sync"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/c2h5oh/datasize"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/db/kv"
)

const MaxUint32 = 1<<32 - 1

type ToBitamp interface {
	ToBitmap() (*roaring64.Bitmap, error)
}

var roaringPool = sync.Pool{
	New: func() any {
		return roaring.New()
	},
}

func NewBitmap() *roaring.Bitmap {
	a := roaringPool.Get().(*roaring.Bitmap)
	a.Clear()
	return a
}
func ReturnToPool(a *roaring.Bitmap) {
	if a == nil {
		return
	}
	roaringPool.Put(a)
}

var roaring64Pool = sync.Pool{
	New: func() any {
		return roaring64.New()
	},
}

func NewBitmap64() *roaring64.Bitmap {
	a := roaring64Pool.Get().(*roaring64.Bitmap)
	a.Clear()
	return a
}
func ReturnToPool64(a *roaring64.Bitmap) {
	if a == nil {
		return
	}
	roaring64Pool.Put(a)
}

const ChunkLimit = uint64(1950 * datasize.B) // threshold beyond which MDBX overflow pages appear: 4096 / 2 - (keySize + 8)

// CutLeft - cut from bitmap `targetSize` bytes from left
// removing lft part from `bm`
// returns nil on zero cardinality
func CutLeft(bm *roaring.Bitmap, sizeLimit uint64) *roaring.Bitmap {
	if bm.GetCardinality() == 0 {
		return nil
	}

	sz := bm.GetSerializedSizeInBytes()
	if sz <= sizeLimit {
		lft := roaring.New()
		lft.AddRange(uint64(bm.Minimum()), uint64(bm.Maximum())+1)
		lft.And(bm)
		lft.RunOptimize()
		bm.Clear()
		return lft
	}

	from := uint64(bm.Minimum())
	minMax := bm.Maximum() - bm.Minimum()
	to := sort.Search(int(minMax), func(i int) bool { // can be optimized to avoid "too small steps", but let's leave it for readability
		lft := roaring.New() // bitmap.Clear() method intentionally not used here, because then serialized size of bitmap getting bigger
		lft.AddRange(from, from+uint64(i)+1)
		lft.And(bm)
		lft.RunOptimize()
		return lft.GetSerializedSizeInBytes() > sizeLimit
	})

	lft := roaring.New()
	lft.AddRange(from, from+uint64(to)) // no +1 because sort.Search returns element which is just higher threshold - but we need lower
	lft.And(bm)
	bm.RemoveRange(from, from+uint64(to))
	lft.RunOptimize()
	return lft
}

func WalkChunks(bm *roaring.Bitmap, sizeLimit uint64, f func(chunk *roaring.Bitmap, isLast bool) error) error {
	for bm.GetCardinality() > 0 {
		if err := f(CutLeft(bm, sizeLimit), bm.GetCardinality() == 0); err != nil {
			return err
		}
	}
	return nil
}

func WalkChunkWithKeys(k []byte, m *roaring.Bitmap, sizeLimit uint64, f func(chunkKey []byte, chunk *roaring.Bitmap) error) error {
	return WalkChunks(m, sizeLimit, func(chunk *roaring.Bitmap, isLast bool) error {
		chunkKey := make([]byte, len(k)+4)
		copy(chunkKey, k)
		if isLast {
			binary.BigEndian.PutUint32(chunkKey[len(k):], ^uint32(0))
		} else {
			binary.BigEndian.PutUint32(chunkKey[len(k):], chunk.Maximum())
		}
		return f(chunkKey, chunk)
	})
}

// TruncateRange - gets existing bitmap in db and call RemoveRange operator on it.
// starts from hot shard, stops when shard not overlap with [from-to)
// !Important: [from, to)
func TruncateRange(db kv.RwTx, bucket string, key []byte, to uint32) error {
	chunkKey := make([]byte, len(key)+4)
	copy(chunkKey, key)
	binary.BigEndian.PutUint32(chunkKey[len(chunkKey)-4:], to)
	bm, err := Get(db, bucket, key, to, MaxUint32)
	if err != nil {
		return err
	}

	if bm.GetCardinality() > 0 && to <= bm.Maximum() {
		bm.RemoveRange(uint64(to), uint64(bm.Maximum())+1)
	}

	c, err := db.Cursor(bucket)
	if err != nil {
		return err
	}
	defer c.Close()
	if err := Walk(c, chunkKey, 0, func(k, v []byte) (bool, error) {
		if !bytes.HasPrefix(k, key) {
			return false, nil
		}
		if err := db.Delete(bucket, k); err != nil {
			return false, err
		}
		return true, nil
	}); err != nil {
		return err
	}

	buf := bytes.NewBuffer(nil)
	return WalkChunkWithKeys(key, bm, ChunkLimit, func(chunkKey []byte, chunk *roaring.Bitmap) error {
		buf.Reset()
		if _, err := chunk.WriteTo(buf); err != nil {
			return err
		}
		return db.Put(bucket, chunkKey, common.Copy(buf.Bytes()))
	})
}

// Get - reading as much chunks as needed to satisfy [from, to] condition
// join all chunks to 1 bitmap by Or operator
func Get(db kv.Tx, bucket string, key []byte, from, to uint32) (*roaring.Bitmap, error) {
	var chunks []*roaring.Bitmap

	fromKey := make([]byte, len(key)+4)
	copy(fromKey, key)
	binary.BigEndian.PutUint32(fromKey[len(fromKey)-4:], from)
	c, err := db.Cursor(bucket)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	for k, v, err := c.Seek(fromKey); k != nil; k, v, err = c.Next() {
		if err != nil {
			return nil, err
		}
		if !bytes.HasPrefix(k, key) {
			break
		}
		bm := NewBitmap()
		defer ReturnToPool(bm)
		if _, err := bm.ReadFrom(bytes.NewReader(v)); err != nil {
			return nil, err
		}
		chunks = append(chunks, bm)
		if binary.BigEndian.Uint32(k[len(k)-4:]) >= to {
			break
		}
	}
	if len(chunks) == 0 {
		return roaring.New(), nil
	}
	return roaring.FastOr(chunks...), nil
}

// SeekInBitmap - returns value in bitmap which is >= n
//
//nolint:deadcode
func SeekInBitmap(m *roaring.Bitmap, n uint32) (found uint32, ok bool) {
	i := m.Iterator()
	i.AdvanceIfNeeded(n)
	ok = i.HasNext()
	if ok {
		found = i.Next()
	}
	return found, ok
}

// CutLeft - cut from bitmap `targetSize` bytes from left
// removing lft part from `bm`
// returns nil on zero cardinality
func CutLeft64(bm *roaring64.Bitmap, sizeLimit uint64) *roaring64.Bitmap {
	if bm.GetCardinality() == 0 {
		return nil
	}

	sz := bm.GetSerializedSizeInBytes()
	if sz <= sizeLimit {
		lft := roaring64.New()
		lft.AddRange(bm.Minimum(), bm.Maximum()+1)
		lft.And(bm)
		lft.RunOptimize()
		bm.Clear()
		return lft
	}

	from := bm.Minimum()
	minMax := bm.Maximum() - bm.Minimum()
	to := sort.Search(int(minMax), func(i int) bool { // can be optimized to avoid "too small steps", but let's leave it for readability
		lft := roaring64.New() // bitmap.Clear() method intentionally not used here, because then serialized size of bitmap getting bigger
		lft.AddRange(from, from+uint64(i)+1)
		lft.And(bm)
		lft.RunOptimize()
		return lft.GetSerializedSizeInBytes() > sizeLimit
	})

	lft := roaring64.New()
	lft.AddRange(from, from+uint64(to)) // no +1 because sort.Search returns element which is just higher threshold - but we need lower
	lft.And(bm)
	bm.RemoveRange(from, from+uint64(to))
	lft.RunOptimize()
	return lft
}

func WalkChunks64(bm *roaring64.Bitmap, sizeLimit uint64, f func(chunk *roaring64.Bitmap, isLast bool) error) error {
	for bm.GetCardinality() > 0 {
		if err := f(CutLeft64(bm, sizeLimit), bm.GetCardinality() == 0); err != nil {
			return err
		}
	}
	return nil
}

func WalkChunkWithKeys64(k []byte, m *roaring64.Bitmap, sizeLimit uint64, f func(chunkKey []byte, chunk *roaring64.Bitmap) error) error {
	return WalkChunks64(m, sizeLimit, func(chunk *roaring64.Bitmap, isLast bool) error {
		chunkKey := make([]byte, len(k)+8)
		copy(chunkKey, k)
		if isLast {
			binary.BigEndian.PutUint64(chunkKey[len(k):], ^uint64(0))
		} else {
			binary.BigEndian.PutUint64(chunkKey[len(k):], chunk.Maximum())
		}
		return f(chunkKey, chunk)
	})
}

// TruncateRange - gets existing bitmap in db and call RemoveRange operator on it.
// starts from hot shard, stops when shard not overlap with [from-to)
// !Important: [from, to)
func TruncateRange64(db kv.RwTx, bucket string, key []byte, to uint64) error {
	chunkKey := make([]byte, len(key)+8)
	copy(chunkKey, key)
	binary.BigEndian.PutUint64(chunkKey[len(chunkKey)-8:], to)
	bm, err := Get64(db, bucket, key, to, math.MaxUint64)
	if err != nil {
		return err
	}

	if bm.GetCardinality() > 0 && to <= bm.Maximum() {
		bm.RemoveRange(to, bm.Maximum()+1)
	}

	c, err := db.Cursor(bucket)
	if err != nil {
		return err
	}
	defer c.Close()
	cDel, err := db.RwCursor(bucket)
	if err != nil {
		return err
	}
	defer cDel.Close()
	if err := Walk(c, chunkKey, 0, func(k, v []byte) (bool, error) {
		if !bytes.HasPrefix(k, key) {
			return false, nil
		}
		if err := cDel.Delete(k); err != nil {
			return false, err
		}
		return true, nil
	}); err != nil {
		return err
	}

	buf := bytes.NewBuffer(nil)
	return WalkChunkWithKeys64(key, bm, ChunkLimit, func(chunkKey []byte, chunk *roaring64.Bitmap) error {
		buf.Reset()
		if _, err := chunk.WriteTo(buf); err != nil {
			return err
		}
		return db.Put(bucket, chunkKey, common.Copy(buf.Bytes()))
	})
}

// Get - reading as much chunks as needed to satisfy [from, to] condition
// join all chunks to 1 bitmap by Or operator
func Get64(db kv.Tx, bucket string, key []byte, from, to uint64) (*roaring64.Bitmap, error) {
	var chunks []*roaring64.Bitmap

	fromKey := make([]byte, len(key)+8)
	copy(fromKey, key)
	binary.BigEndian.PutUint64(fromKey[len(fromKey)-8:], from)

	c, err := db.Cursor(bucket)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	for k, v, err := c.Seek(fromKey); k != nil; k, v, err = c.Next() {
		if err != nil {
			return nil, err
		}
		if !bytes.HasPrefix(k, key) {
			break
		}
		bm := NewBitmap64()
		defer ReturnToPool64(bm)
		_, err := bm.ReadFrom(bytes.NewReader(v))
		if err != nil {
			return nil, err
		}
		chunks = append(chunks, bm)
		if binary.BigEndian.Uint64(k[len(k)-8:]) >= to {
			break
		}
	}

	if len(chunks) == 0 {
		return roaring64.New(), nil
	}
	return roaring64.FastOr(chunks...), nil
}

// SeekInBitmap - returns value in bitmap which is >= n
func SeekInBitmap64(m *roaring64.Bitmap, n uint64) (found uint64, ok bool) {
	if m.IsEmpty() {
		return 0, false
	}
	if n == 0 {
		return m.Minimum(), true
	}
	searchRank := m.Rank(n - 1)
	if searchRank >= m.GetCardinality() {
		return 0, false
	}
	found, _ = m.Select(searchRank)
	return found, true
}

func Walk(c kv.Cursor, startkey []byte, fixedbits int, walker func(k, v []byte) (bool, error)) error {
	fixedbytes, mask := Bytesmask(fixedbits)
	k, v, err := c.Seek(startkey)
	if err != nil {
		return err
	}
	for k != nil && len(k) >= fixedbytes && (fixedbits == 0 || bytes.Equal(k[:fixedbytes-1], startkey[:fixedbytes-1]) && (k[fixedbytes-1]&mask) == (startkey[fixedbytes-1]&mask)) {
		goOn, err := walker(k, v)
		if err != nil {
			return err
		}
		if !goOn {
			break
		}
		k, v, err = c.Next()
		if err != nil {
			return err
		}
	}
	return nil
}

func Bytesmask(fixedbits int) (fixedbytes int, mask byte) {
	fixedbytes = common.BitLenToByteLen(fixedbits)
	shiftbits := fixedbits & 7
	mask = byte(0xff)
	if shiftbits != 0 {
		mask = 0xff << (8 - shiftbits)
	}
	return fixedbytes, mask
}

type ToBitmap interface {
	ToBitmap() (*roaring64.Bitmap, error)
}

func ToIter(it roaring64.IntIterable64) *ToIterInterface { return &ToIterInterface{it: it} }

type ToIterInterface struct{ it roaring64.IntIterable64 }

func (i *ToIterInterface) HasNext() bool         { return i.it.HasNext() }
func (i *ToIterInterface) Next() (uint64, error) { return i.it.Next(), nil }
