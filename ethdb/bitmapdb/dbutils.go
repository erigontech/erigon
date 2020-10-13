package bitmapdb

import (
	"bytes"
	"encoding/binary"
	"github.com/RoaringBitmap/roaring"
	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"sort"
)

const ChunkLimit = uint64(1900 * datasize.B) // threshold after which appear LMDB OverflowPages

// AppendMergeByOr - appending delta to existing data in db, merge by Or
// Method maintains sharding - because some bitmaps are >1Mb and when new incoming blocks process it
//	 updates ~300 of bitmaps - by append small amount new values. It cause much big writes (LMDB does copy-on-write).
//
// if last existing chunk size merge it with delta
// if serialized size of delta > ChunkLimit - break down to multiple shards
// shard number - it's biggest value in bitmap
func AppendMergeByOr(c ethdb.Cursor, key []byte, delta *roaring.Bitmap) error {
	lastChunkKey := make([]byte, len(key)+4)
	copy(lastChunkKey, key)
	binary.BigEndian.PutUint32(lastChunkKey[len(lastChunkKey)-4:], ^uint32(0))

	currentLastV, seekErr := c.SeekExact(lastChunkKey)
	if seekErr != nil {
		return seekErr
	}

	if currentLastV == nil { // no existing shards, then just create one
		err := writeBitmapChunked(c, key, delta)
		if err != nil {
			return err
		}
		return nil
	}
	last := roaring.New()
	_, err := last.FromBuffer(currentLastV)
	if err != nil {
		return err
	}

	delta = roaring.Or(delta, last)

	err = writeBitmapChunked(c, key, delta)
	if err != nil {
		return err
	}
	return nil
}

// writeBitmapChunked - write bitmap to db, perform sharding if delta > ChunkLimit
func writeBitmapChunked(c ethdb.Cursor, key []byte, delta *roaring.Bitmap) error {
	chunkKey := make([]byte, len(key)+4)
	copy(chunkKey, key)
	sz := int(delta.GetSerializedSizeInBytes())
	if sz <= int(ChunkLimit) {
		newV := bytes.NewBuffer(make([]byte, 0, delta.GetSerializedSizeInBytes()))
		_, err := delta.WriteTo(newV)
		if err != nil {
			return err
		}
		binary.BigEndian.PutUint32(chunkKey[len(chunkKey)-4:], ^uint32(0))
		err = c.Put(common.CopyBytes(chunkKey), newV.Bytes())
		if err != nil {
			return err
		}
		return nil
	}

	shardsAmount := uint32(sz / int(ChunkLimit))
	if shardsAmount == 0 {
		shardsAmount = 1
	}
	step := (delta.Maximum() - delta.Minimum()) / shardsAmount
	step = step / 16
	if step == 0 {
		step = 1
	}
	shard, tmp := roaring.New(), roaring.New() // shard will write to db, tmp will use to add data to shard
	for delta.GetCardinality() > 0 {
		from := uint64(delta.Minimum())
		to := from + uint64(step)
		tmp.Clear()
		tmp.AddRange(from, to)
		tmp.And(delta)
		shard.Or(tmp)
		shard.RunOptimize()
		delta.RemoveRange(from, to)
		if delta.GetCardinality() == 0 {
			break
		}
		if shard.GetSerializedSizeInBytes() >= ChunkLimit {
			newV := bytes.NewBuffer(make([]byte, 0, shard.GetSerializedSizeInBytes()))
			_, err := shard.WriteTo(newV)
			if err != nil {
				return err
			}
			binary.BigEndian.PutUint32(chunkKey[len(chunkKey)-4:], shard.Maximum())

			err = c.Put(common.CopyBytes(chunkKey), newV.Bytes())
			if err != nil {
				return err
			}
			shard.Clear()
		}
	}

	if shard.GetSerializedSizeInBytes() > 0 {
		newV := bytes.NewBuffer(make([]byte, 0, shard.GetSerializedSizeInBytes()))
		_, err := shard.WriteTo(newV)
		if err != nil {
			return err
		}
		binary.BigEndian.PutUint32(chunkKey[len(chunkKey)-4:], ^uint32(0))
		err = c.Put(common.CopyBytes(chunkKey), newV.Bytes())
		if err != nil {
			return err
		}
		return nil
	}

	return nil
}

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
			err = cForDelete.Delete(k)
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

	// rename last chunk
	k, v, err := c.Current()
	if err != nil {
		return err
	}
	if k == nil { // if last chunk was deleted, do 1 step back
		k, v, err = c.Prev()
		if err != nil {
			return err
		}
	}

	if binary.BigEndian.Uint32(k[len(k)-4:]) == ^uint32(0) { // nothing to return
		return nil
	}
	if !bytes.HasPrefix(k, key) {
		return nil
	}

	copyV := common.CopyBytes(v)
	err = cForDelete.Delete(k)
	if err != nil {
		return err
	}

	binary.BigEndian.PutUint32(chunkKey[len(chunkKey)-4:], ^uint32(0))
	err = c.Put(chunkKey, copyV)
	if err != nil {
		return err
	}

	return nil
}

// Get - reading as much chunks as needed to satisfy [from, to] condition
// join all chunks to 1 bitmap by Or operator
func Get(c ethdb.Cursor, key []byte, from, to uint32) (*roaring.Bitmap, error) {
	var chunks []*roaring.Bitmap

	fromKey := make([]byte, len(key)+4)
	copy(fromKey, key)
	binary.BigEndian.PutUint32(fromKey[len(fromKey)-4:], from)
	for k, v, err := c.Seek(fromKey); k != nil; k, v, err = c.Next() {
		if err != nil {
			return nil, err
		}

		if !bytes.HasPrefix(k, key) {
			break
		}

		bm := roaring.New()
		_, err := bm.FromBuffer(v)
		if err != nil {
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
