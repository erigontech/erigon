package bitmapdb

import (
	"bytes"
	"encoding/binary"
	"sort"

	"github.com/RoaringBitmap/roaring"
	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

const ChunkLimit = uint64(1900 * datasize.B) // threshold after which appear LMDB OverflowPages

func ChunkIterator(bm *roaring.Bitmap, target uint64) func() *roaring.Bitmap {
	return func() *roaring.Bitmap {
		return CutLeft(bm, target)
	}
}

// CutLeft - cut from bitmap `targetSize` bytes from left
// removing lft part from `bm`
// returns nil on zero cardinality
func CutLeft(bm *roaring.Bitmap, targetSize uint64) *roaring.Bitmap {
	if bm.GetCardinality() == 0 {
		return nil
	}

	sz := bm.GetSerializedSizeInBytes()
	if sz <= targetSize {
		lft := roaring.New()
		lft.Or(bm)
		bm.Clear()
		return lft
	}

	lft := roaring.New()
	from := uint64(bm.Minimum())
	minMax := bm.Maximum() - bm.Minimum()             // +1 because AddRange has semantic [from,to)
	to := sort.Search(int(minMax), func(i int) bool { // can be optimized to avoid "too small steps", but let's leave it for readability
		lft.Clear()
		lft.AddRange(from, from+uint64(i)+1)
		lft.And(bm)
		return lft.GetSerializedSizeInBytes() > targetSize
	})

	lft.Clear()
	lft.AddRange(from, from+uint64(to)+1)
	lft.And(bm)
	bm.RemoveRange(from, from+uint64(to)+1)
	return lft
}

// TruncateRange - gets existing bitmap in db and call RemoveRange operator on it.
// starts from hot shard, stops when shard not overlap with [from-to)
// !Important: [from, to)
func TruncateRange(tx ethdb.Tx, bucket string, key []byte, from, to uint64) error {
	chunkKey := make([]byte, len(key)+4)
	copy(chunkKey, key)
	binary.BigEndian.PutUint32(chunkKey[len(chunkKey)-4:], uint32(from))
	c := tx.Cursor(bucket)
	defer c.Close()
	cForDelete := tx.Cursor(bucket) // use dedicated cursor for delete operation, but in near future will change to ETL
	defer cForDelete.Close()

	for k, v, err := c.Seek(chunkKey); k != nil; k, v, err = c.Next() {
		if err != nil {
			return err
		}

		if !bytes.HasPrefix(k, key) {
			break
		}

		bm := roaring.New()
		_, err := bm.FromBuffer(v)
		if err != nil {
			return err
		}
		noReasonToCheckNextChunk := (uint64(bm.Minimum()) <= from && uint64(bm.Maximum()) >= to) || binary.BigEndian.Uint32(k[len(k)-4:]) == ^uint32(0)

		bm.RemoveRange(from, to)
		if bm.GetCardinality() == 0 { // don't store empty bitmaps
			err = cForDelete.Delete(k, nil)
			if err != nil {
				return err
			}
			if noReasonToCheckNextChunk {
				break
			}
			continue
		}

		bm.RunOptimize()
		newV := bytes.NewBuffer(make([]byte, 0, bm.GetSerializedSizeInBytes()))
		_, err = bm.WriteTo(newV)
		if err != nil {
			return err
		}
		err = c.Put(common.CopyBytes(k), newV.Bytes())
		if err != nil {
			return err
		}

		if noReasonToCheckNextChunk {
			break
		}
	}

	// rename last chunk if it has no finality marker
	binary.BigEndian.PutUint32(chunkKey[len(chunkKey)-4:], ^uint32(0))
	k, v, err := c.Seek(chunkKey)
	if err != nil {
		return err
	}
	if k == nil || !bytes.HasPrefix(k, key) {
		k, v, err = c.Prev()
		if err != nil {
			return err
		}

		// case when all chunks were delted
		if k == nil || !bytes.HasPrefix(k, key) {
			return nil
		}
	}

	copyV := common.CopyBytes(v)
	err = cForDelete.Delete(k, nil)
	if err != nil {
		return err
	}

	err = c.Put(chunkKey, copyV)
	if err != nil {
		return err
	}

	return nil
}

// Get - reading as much chunks as needed to satisfy [from, to] condition
// join all chunks to 1 bitmap by Or operator
func Get(db ethdb.Getter, bucket string, key []byte, from, to uint32) (*roaring.Bitmap, error) {
	var chunks []*roaring.Bitmap

	fromKey := make([]byte, len(key)+4)
	copy(fromKey, key)
	binary.BigEndian.PutUint32(fromKey[len(fromKey)-4:], from)

	if err := db.Walk(bucket, fromKey, len(key)*8, func(k, v []byte) (bool, error) {
		bm := roaring.New()
		_, err := bm.FromBuffer(v)
		if err != nil {
			return false, err
		}
		chunks = append(chunks, bm)

		if binary.BigEndian.Uint32(k[len(k)-4:]) >= to {
			return false, nil
		}
		return true, nil
	}); err != nil {
		return nil, err
	}

	if len(chunks) == 0 {
		return roaring.New(), nil
	}
	return roaring.FastOr(chunks...), nil
}
