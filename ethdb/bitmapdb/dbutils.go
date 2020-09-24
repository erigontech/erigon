package bitmapdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/RoaringBitmap/roaring"
	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/math"
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
		_, err = existing.FromBuffer(v)
		if err != nil {
			return err
		}

		delta.Or(existing)
	}

	delta.RunOptimize()
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

const shard uint32 = 1_000_000

func AppendShardedMergeByOr(c ethdb.Cursor, k []byte, delta *roaring.Bitmap) error {
	t := time.Now()

	type shardT struct {
		sN uint32
		bm *roaring.Bitmap
	}
	var bitmaps []shardT

	var sN uint32
	for delta.GetCardinality() > 0 {
		min := delta.Minimum()
		localSn := min / shard
		sN += localSn
		st := shardT{
			sN: sN,
		}
		st.bm = roaring.AddOffset64(delta, -int64(localSn*shard))
		st.bm.RemoveRange(uint64(shard), math.MaxInt32)
		bitmaps = append(bitmaps, st)
		delta.RemoveRange(0, uint64(shard*(localSn+1)))
	}

	kk, v, err := c.Seek(k)
	if err != nil {
		return err
	}
	if kk != nil && bytes.HasPrefix(kk, k) {
		existing := roaring.New()
		_, err = existing.FromBuffer(v)
		if err != nil {
			return err
		}
		sN := ^binary.BigEndian.Uint16(kk[len(kk)-2:])

		for i := range bitmaps {
			if uint16(bitmaps[i].sN) != sN {
				continue
			}
			bitmaps[i].bm.Or(existing)
			err = c.DeleteCurrent()
			if err != nil {
				return err
			}
		}
	}

	for i := range bitmaps {
		bm := bitmaps[i].bm
		bm.RunOptimize()
		newV := make([]byte, int(bm.GetSerializedSizeInBytes()))
		newK := make([]byte, len(k)+2)
		copy(newK, k)
		binary.BigEndian.PutUint16(newK[len(newK)-2:], ^uint16(bitmaps[i].sN))
		_, err = bm.WriteTo(bytes.NewBuffer(newV[:0]))
		err = c.Put(newK, newV)
		if err != nil {
			return err
		}
	}

	s := time.Since(t)
	if s > 10*time.Millisecond {
		fmt.Printf("2: %x %s %d\n", k, s, len(bitmaps))
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

	bm.RunOptimize()
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

func GetSharded(c ethdb.Cursor, key []byte) (*roaring.Bitmap, error) {
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
		sN := ^binary.BigEndian.Uint16(k[len(k)-2:])
		bm = roaring.AddOffset64(bm, int64(uint32(sN)*shard))
		shards = append(shards, bm)
	}

	return roaring.FastOr(shards...), nil
}

const shard2 = 512 * datasize.KB

func AppendShardedMergeByOr2(c ethdb.Cursor, key []byte, delta *roaring.Bitmap) error {
	t := time.Now()

	createNewShard := true
	sN := uint16(0)

	k, v, err := c.Seek(key)
	if err != nil {
		return err
	}
	if k != nil && bytes.HasPrefix(k, key) {
		existing := roaring.New()
		_, err = existing.FromBuffer(v)
		if err != nil {
			return err
		}

		if len(v) < int(shard2) {
			createNewShard = false
			delta.Or(existing)
		} else {
			createNewShard = true
			sN = ^binary.BigEndian.Uint16(k[len(k)-2:]) + 1
		}
	}

	if createNewShard {
		delta.RunOptimize()
		newV := make([]byte, int(delta.GetSerializedSizeInBytes()))
		newK := make([]byte, len(key)+2)
		copy(newK, key)
		binary.BigEndian.PutUint16(newK[len(newK)-2:], ^sN)
		_, err = delta.WriteTo(bytes.NewBuffer(newV[:0]))
		err = c.Put(newK, newV)
		if err != nil {
			return err
		}
		s := time.Since(t)
		if s > 50*time.Millisecond {
			fmt.Printf("1: time=%s, card=%d, serializeSize=%d shard=%d\n", s, delta.GetCardinality(), delta.GetSerializedSizeInBytes(), sN)
		}
		return nil
	}

	delta.RunOptimize()
	newV := make([]byte, int(delta.GetSerializedSizeInBytes()))
	_, err = delta.WriteTo(bytes.NewBuffer(newV[:0]))
	err = c.Put(k, newV)
	if err != nil {
		return err
	}

	s := time.Since(t)
	if s > 50*time.Millisecond {
		fmt.Printf("1: time=%s, card=%d, serializeSize=%d shard=%d\n", s, delta.GetCardinality(), delta.GetSerializedSizeInBytes(), sN)
	}
	return nil
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
	}

	s := time.Since(t)
	if s > 20*time.Millisecond {
		fmt.Printf("3: time=%s, updated=%d\n", s, updated)
	}
	return nil
}

func GetSharded2(c ethdb.Cursor, key []byte) (*roaring.Bitmap, error) {
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

const ColdShardLimit = 512 * datasize.KB
const HotShardLimit = 4 * datasize.KB

// 3 terms are used: cold_shard, hot_shard, delta
// delta - most recent changes (appendable)
// hot_shard - merge delta here until hot_shard size < HotShardLimit, otherwise merge hot to cold
// cold_shard - merge hot_shard here until cold_shard size < ColdShardLimit, otherwise mark hot as cold, create new hot from delta
// no cold shards, create new hot from delta - it will turn hot to cold automatically
// never merge cold with cold for compaction - because it's expensive operation
func AppendShardedMergeByOr3(c ethdb.Cursor, key []byte, delta *roaring.Bitmap) error {
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

	delta.RunOptimize()
	newV := make([]byte, int(delta.GetSerializedSizeInBytes()))
	_, err = delta.WriteTo(bytes.NewBuffer(newV[:0]))
	if err != nil {
		return err
	}

	newK := make([]byte, len(key)+2)
	copy(newK, key)
	binary.BigEndian.PutUint16(newK[len(newK)-2:], ^shardNForDelta)
	err = c.Put(newK, newV)
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
	err = c.Put(common.CopyBytes(coldK), coldBytes)
	if err != nil {
		return 0, err
	}

	return hotShardN, nil
}
