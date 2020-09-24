package bitmapdb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/RoaringBitmap/roaring"
	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

const ColdShardLimit = 256 * datasize.KB
const HotShardLimit = 4 * datasize.KB

// AppendMergeByOr - appending delta to existing data in db, merge by Or
// Method maintains sharding - because some bitmaps are >1Mb and when new incoming blocks process it
//	 updates ~300 of bitmaps - by append small amount new values. It cause much big writes (LMDB does copy-on-write).
//
// 3 terms are used: cold_shard, hot_shard, delta
//   delta - most recent changes (appendable)
//   hot_shard - merge delta here until hot_shard size < HotShardLimit, otherwise merge hot to cold
//   cold_shard - merge hot_shard here until cold_shard size < ColdShardLimit, otherwise mark hot as cold, create new hot from delta
// if no cold shard, create new hot from delta - it will turn hot to cold automatically
// never merges cold with cold for compaction - because it's expensive operation
func AppendMergeByOr(c ethdb.Cursor, key []byte, delta *roaring.Bitmap) error {
	t := time.Now()

	shardNForDelta := uint16(0)

	hotK, hotV, err := c.Seek(key)
	if err != nil {
		return err
	}
	if hotK != nil && bytes.HasPrefix(hotK, key) {
		hotShardN := ^binary.BigEndian.Uint16(hotK[len(hotK)-2:])
		if len(hotV) > int(HotShardLimit) { // merge hot to cold
			shardNForDelta, err = hotShardOverflow(c, hotShardN, hotV)
			if err != nil {
				return err
			}
		} else { // merge delta to hot, write result to `hotShardN`
			hot := roaring.New()
			_, err = hot.FromBuffer(hotV)
			if err != nil {
				return err
			}

			delta.Or(hot)
			shardNForDelta = hotShardN
		}
	}

	newK := make([]byte, len(key)+2)
	copy(newK, key)
	binary.BigEndian.PutUint16(newK[len(newK)-2:], ^shardNForDelta)

	delta.RunOptimize()
	bufBytes, err := c.Reserve(newK, int(delta.GetSerializedSizeInBytes()))
	if err != nil {
		panic(err)
	}

	_, err = delta.WriteTo(bytes.NewBuffer(bufBytes[:0]))
	if err != nil {
		return err
	}

	s := time.Since(t)
	if s > 50*time.Millisecond {
		fmt.Printf("1: time=%s, card=%d, serializeSize=%d shard=%d\n", s, delta.GetCardinality(), delta.GetSerializedSizeInBytes(), shardNForDelta)
	}
	return nil
}

func hotShardOverflow(c ethdb.Cursor, hotShardN uint16, hotV []byte) (shardNForDelta uint16, err error) {
	if hotShardN == 0 { // no cold shards, create new hot from delta - it will turn hot to cold automatically
		return 1, nil
	}

	coldK, coldV, err := c.Next() // get cold shard from db
	if err != nil {
		return 0, err
	}

	if len(coldV) > int(ColdShardLimit) { // cold shard is too big, write delta to `hotShardN + 1` - it will turn hot to cold automatically
		return hotShardN + 1, nil
	}

	// merge hot to cold and replace hot by delta (by write delta to `hotShardN`)
	cold := roaring.New()
	_, err = cold.FromBuffer(coldV)
	if err != nil {
		return 0, err
	}

	hot := roaring.New()
	_, err = hot.FromBuffer(hotV)
	if err != nil {
		return 0, err
	}

	cold.Or(hot)

	cold.RunOptimize()
	coldBytes := make([]byte, int(cold.GetSerializedSizeInBytes()))
	_, err = cold.WriteTo(bytes.NewBuffer(coldBytes[:0]))
	err = c.PutCurrent(coldK, coldBytes)
	if err != nil {
		return 0, err
	}

	return hotShardN, nil
}

// RemoveRange - gets existing bitmap in db and call RemoveRange operator on it.
// !Important: [from, to)
func TrimShardedRange(c ethdb.Cursor, key []byte, from, to uint64) error {
	t := time.Now()
	updated := 0
	for k, v, err := c.Seek(key); k != nil; k, v, err = c.Next() {
		if err != nil {
			return err
		}

		if !bytes.HasPrefix(k, key) {
			break
		}
		bm := roaring.New()
		_, err = bm.FromBuffer(v)
		if err != nil {
			return err
		}
		if uint64(bm.Maximum()) < from {
			break
		}
		noReasonToCheckNextShard := uint64(bm.Minimum()) <= from && uint64(bm.Maximum()) >= to

		updated++
		bm.RemoveRange(from, to)
		if bm.GetCardinality() == 0 { // don't store empty bitmaps
			err = c.DeleteCurrent()
			if err != nil {
				return err
			}
			continue
		}

		bm.RunOptimize()
		newV := make([]byte, int(bm.GetSerializedSizeInBytes()))
		_, err = bm.WriteTo(bytes.NewBuffer(newV[:0]))
		if err != nil {
			return err
		}
		err = c.Put(k, newV)
		if err != nil {
			return err
		}

		if noReasonToCheckNextShard {
			break
		}
	}

	s := time.Since(t)
	if s > 20*time.Millisecond {
		fmt.Printf("3: time=%s, updated=%d\n", s, updated)
	}
	return nil
}

func Get(c ethdb.Cursor, key []byte) (*roaring.Bitmap, error) {
	var shards []*roaring.Bitmap
	for k, v, err := c.Seek(key); k != nil; k, v, err = c.Next() {
		if err != nil {
			return nil, err
		}

		if !bytes.HasPrefix(k, key) {
			break
		}
		bm := roaring.New()
		_, err = bm.FromBuffer(v)
		if err != nil {
			return nil, err
		}
		shards = append(shards, bm)
	}

	return roaring.FastOr(shards...), nil
}
