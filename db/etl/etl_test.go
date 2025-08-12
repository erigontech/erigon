// Copyright 2021 The Erigon Authors
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

package etl

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/memdb"
)

func decodeHex(in string) []byte {
	payload, err := hex.DecodeString(in)
	if err != nil {
		panic(err)
	}
	return payload
}

func TestEmptyValueIsNotANil(t *testing.T) {
	logger := log.New()
	t.Run("sortable", func(t *testing.T) {
		collector := NewCollector(t.Name(), "", NewSortableBuffer(1), logger)
		defer collector.Close()
		require := require.New(t)
		require.NoError(collector.Collect([]byte{1}, []byte{}))
		require.NoError(collector.Collect([]byte{2}, nil))
		require.NoError(collector.Load(nil, "", func(k, v []byte, table CurrentTableReader, next LoadNextFunc) error {
			if k[0] == 1 {
				require.Equal([]byte{}, v)
			} else {
				require.Nil(v)
			}
			return nil
		}, TransformArgs{}))
	})
	t.Run("append", func(t *testing.T) {
		// append buffer doesn't support nil values
		collector := NewCollector(t.Name(), "", NewAppendBuffer(1), logger)
		defer collector.Close()
		require := require.New(t)
		require.NoError(collector.Collect([]byte{1}, []byte{}))
		require.NoError(collector.Collect([]byte{2}, nil))
		require.NoError(collector.Load(nil, "", func(k, v []byte, table CurrentTableReader, next LoadNextFunc) error {
			require.Nil(v)
			return nil
		}, TransformArgs{}))
	})
	t.Run("oldest", func(t *testing.T) {
		collector := NewCollector(t.Name(), "", NewOldestEntryBuffer(1), logger)
		defer collector.Close()
		require := require.New(t)
		require.NoError(collector.Collect([]byte{1}, []byte{}))
		require.NoError(collector.Collect([]byte{2}, nil))
		require.NoError(collector.Load(nil, "", func(k, v []byte, table CurrentTableReader, next LoadNextFunc) error {
			if k[0] == 1 {
				require.Equal([]byte{}, v)
			} else {
				require.Nil(v)
			}
			return nil
		}, TransformArgs{}))
	})
}

func TestEmptyKeyValue(t *testing.T) {
	logger := log.New()
	_, tx := memdb.NewTestTx(t)
	require := require.New(t)
	table := kv.ChaindataTables[0]
	collector := NewCollector(t.Name(), "", NewSortableBuffer(1), logger)
	defer collector.Close()
	require.NoError(collector.Collect([]byte{2}, []byte{}))
	require.NoError(collector.Collect([]byte{1}, []byte{1}))
	require.NoError(collector.Load(tx, table, IdentityLoadFunc, TransformArgs{}))
	v, err := tx.GetOne(table, []byte{2})
	require.NoError(err)
	require.Equal([]byte{}, v)
	v, err = tx.GetOne(table, []byte{1})
	require.NoError(err)
	require.Equal([]byte{1}, v)

	collector = NewCollector(t.Name(), "", NewSortableBuffer(1), logger)
	defer collector.Close()
	require.NoError(collector.Collect([]byte{}, nil))
	require.NoError(collector.Load(tx, table, IdentityLoadFunc, TransformArgs{}))
	v, err = tx.GetOne(table, []byte{})
	require.NoError(err)
	require.Nil(v)
}

func TestWriteAndReadBufferEntry(t *testing.T) {
	b := NewSortableBuffer(128)
	buffer := bytes.NewBuffer(make([]byte, 0))

	entries := make([]sortableBufferEntry, 100)
	for i := range entries {
		entries[i].key = []byte(fmt.Sprintf("key-%d", i))
		entries[i].value = []byte(fmt.Sprintf("value-%d", i))
		b.Put(entries[i].key, entries[i].value)
	}

	if err := b.Write(buffer); err != nil {
		t.Error(err)
	}

	bb := buffer.Bytes()

	readBuffer := bytes.NewReader(bb)

	for i := range entries {
		k, v, err := readElementFromDisk(readBuffer, readBuffer, nil, nil)
		if err != nil {
			t.Error(err)
		}
		assert.Equal(t, string(entries[i].key), string(k))
		assert.Equal(t, string(entries[i].value), string(v))
	}

	_, _, err := readElementFromDisk(readBuffer, readBuffer, nil, nil)
	assert.Equal(t, io.EOF, err)
}

func TestNextKey(t *testing.T) {
	for _, tc := range []string{
		"00000001->00000002",
		"000000FF->00000100",
		"FEFFFFFF->FF000000",
	} {
		parts := strings.Split(tc, "->")
		input := decodeHex(parts[0])
		expectedOutput := decodeHex(parts[1])
		actualOutput, err := NextKey(input)
		require.NoError(t, err)
		assert.Equal(t, expectedOutput, actualOutput)
	}
}

func TestNextKeyErr(t *testing.T) {
	for _, tc := range []string{
		"",
		"FFFFFF",
	} {
		input := decodeHex(tc)
		_, err := NextKey(input)
		assert.Error(t, err)
	}
}

func TestFileDataProviders(t *testing.T) {
	logger := log.New()
	// test invariant when we go through files (> 1 buffer)
	_, tx := memdb.NewTestTx(t)
	sourceBucket := kv.ChaindataTables[0]

	generateTestData(t, tx, sourceBucket, 10)

	collector := NewCollector(t.Name(), "", NewSortableBuffer(1), logger)

	err := extractBucketIntoFiles("logPrefix", tx, sourceBucket, nil, nil, collector, testExtractToMapFunc, nil, nil, logger)
	require.NoError(t, err)

	assert.Len(t, collector.dataProviders, 10)

	for _, p := range collector.dataProviders {
		fp, ok := p.(*fileDataProvider)
		assert.True(t, ok)
		err := fp.Wait()
		require.NoError(t, err)
		_, err = os.Stat(fp.file.Name())
		require.NoError(t, err)
	}

	collector.Close()

	for _, p := range collector.dataProviders {
		fp, ok := p.(*fileDataProvider)
		assert.True(t, ok)
		_, err = os.Stat(fp.file.Name())
		assert.True(t, os.IsNotExist(err))
	}
}

func TestRAMDataProviders(t *testing.T) {
	logger := log.New()
	// test invariant when we go through memory (1 buffer)
	_, tx := memdb.NewTestTx(t)
	sourceBucket := kv.ChaindataTables[0]
	generateTestData(t, tx, sourceBucket, 10)

	collector := NewCollector(t.Name(), "", NewSortableBuffer(BufferOptimalSize), logger)
	err := extractBucketIntoFiles("logPrefix", tx, sourceBucket, nil, nil, collector, testExtractToMapFunc, nil, nil, logger)
	require.NoError(t, err)

	assert.Len(t, collector.dataProviders, 1)

	for _, p := range collector.dataProviders {
		mp, ok := p.(*memoryDataProvider)
		assert.True(t, ok)
		assert.Equal(t, 10, mp.buffer.Len())
	}
}

func TestTransformRAMOnly(t *testing.T) {
	logger := log.New()
	// test invariant when we only have one buffer and it fits into RAM (exactly 1 buffer)
	_, tx := memdb.NewTestTx(t)

	sourceBucket := kv.ChaindataTables[0]
	destBucket := kv.ChaindataTables[1]
	generateTestData(t, tx, sourceBucket, 20)
	err := Transform(
		"logPrefix",
		tx,
		sourceBucket,
		destBucket,
		"", // temp dir
		testExtractToMapFunc,
		testLoadFromMapFunc,
		TransformArgs{},
		logger,
	)
	require.NoError(t, err)
	compareBuckets(t, tx, sourceBucket, destBucket, nil)
}

func TestEmptySourceBucket(t *testing.T) {
	logger := log.New()
	_, tx := memdb.NewTestTx(t)
	sourceBucket := kv.ChaindataTables[0]
	destBucket := kv.ChaindataTables[1]
	err := Transform(
		"logPrefix",
		tx,
		sourceBucket,
		destBucket,
		"", // temp dir
		testExtractToMapFunc,
		testLoadFromMapFunc,
		TransformArgs{},
		logger,
	)
	require.NoError(t, err)
	compareBuckets(t, tx, sourceBucket, destBucket, nil)
}

func TestTransformExtractStartKey(t *testing.T) {
	logger := log.New()
	// test invariant when we only have one buffer and it fits into RAM (exactly 1 buffer)
	_, tx := memdb.NewTestTx(t)
	sourceBucket := kv.ChaindataTables[0]
	destBucket := kv.ChaindataTables[1]
	generateTestData(t, tx, sourceBucket, 10)
	err := Transform(
		"logPrefix",
		tx,
		sourceBucket,
		destBucket,
		"", // temp dir
		testExtractToMapFunc,
		testLoadFromMapFunc,
		TransformArgs{ExtractStartKey: []byte(fmt.Sprintf("%10d-key-%010d", 5, 5))},
		logger,
	)
	require.NoError(t, err)
	compareBuckets(t, tx, sourceBucket, destBucket, []byte(fmt.Sprintf("%10d-key-%010d", 5, 5)))
}

func TestTransformThroughFiles(t *testing.T) {
	logger := log.New()
	// test invariant when we go through files (> 1 buffer)
	_, tx := memdb.NewTestTx(t)
	sourceBucket := kv.ChaindataTables[0]
	destBucket := kv.ChaindataTables[1]
	generateTestData(t, tx, sourceBucket, 10)
	err := Transform(
		"logPrefix",
		tx,
		sourceBucket,
		destBucket,
		"", // temp dir
		testExtractToMapFunc,
		testLoadFromMapFunc,
		TransformArgs{
			BufferSize: 1,
		},
		logger,
	)
	require.NoError(t, err)
	compareBuckets(t, tx, sourceBucket, destBucket, nil)
}

func TestTransformDoubleOnExtract(t *testing.T) {
	logger := log.New()
	// test invariant when extractFunc multiplies the data 2x
	_, tx := memdb.NewTestTx(t)
	sourceBucket := kv.ChaindataTables[0]
	destBucket := kv.ChaindataTables[1]
	generateTestData(t, tx, sourceBucket, 10)
	err := Transform(
		"logPrefix",
		tx,
		sourceBucket,
		destBucket,
		"", // temp dir
		testExtractDoubleToMapFunc,
		testLoadFromMapFunc,
		TransformArgs{},
		logger,
	)
	require.NoError(t, err)
	compareBucketsDouble(t, tx, sourceBucket, destBucket)
}

func TestTransformDoubleOnLoad(t *testing.T) {
	logger := log.New()
	// test invariant when loadFunc multiplies the data 2x
	_, tx := memdb.NewTestTx(t)
	sourceBucket := kv.ChaindataTables[0]
	destBucket := kv.ChaindataTables[1]
	generateTestData(t, tx, sourceBucket, 10)
	err := Transform(
		"logPrefix",
		tx,
		sourceBucket,
		destBucket,
		"", // temp dir
		testExtractToMapFunc,
		testLoadFromMapDoubleFunc,
		TransformArgs{},
		logger,
	)
	require.NoError(t, err)
	compareBucketsDouble(t, tx, sourceBucket, destBucket)
}

func generateTestData(t *testing.T, db kv.Putter, bucket string, count int) {
	t.Helper()
	for i := 0; i < count; i++ {
		k := []byte(fmt.Sprintf("%10d-key-%010d", i, i))
		v := []byte(fmt.Sprintf("val-%099d", i))
		err := db.Put(bucket, k, v)
		require.NoError(t, err)
	}
}

func testExtractToMapFunc(k, v []byte, next ExtractNextFunc) error {
	valueMap := make(map[string][]byte)
	valueMap["value"] = v
	out, err := json.Marshal(valueMap)
	if err != nil {
		return err
	}
	return next(k, k, out)
}

func testExtractDoubleToMapFunc(k, v []byte, next ExtractNextFunc) error {
	var err error
	valueMap := make(map[string][]byte)
	valueMap["value"] = append(v, 0xAA)
	k1 := append(k, 0xAA)
	out, err := json.Marshal(valueMap)
	if err != nil {
		panic(err)
	}

	err = next(k, k1, out)
	if err != nil {
		return err
	}

	valueMap = make(map[string][]byte)
	valueMap["value"] = append(v, 0xBB)
	k2 := append(k, 0xBB)
	out, err = json.Marshal(valueMap)
	if err != nil {
		panic(err)
	}
	return next(k, k2, out)
}

func testLoadFromMapFunc(k []byte, v []byte, _ CurrentTableReader, next LoadNextFunc) error {
	valueMap := make(map[string][]byte)
	err := json.Unmarshal(v, &valueMap)
	if err != nil {
		return err
	}
	realValue := valueMap["value"]
	return next(k, k, realValue)
}

func testLoadFromMapDoubleFunc(k []byte, v []byte, _ CurrentTableReader, next LoadNextFunc) error {
	valueMap := make(map[string][]byte)
	err := json.Unmarshal(v, &valueMap)
	if err != nil {
		return err
	}
	realValue := valueMap["value"]

	err = next(k, append(k, 0xAA), append(realValue, 0xAA))
	if err != nil {
		return err
	}
	return next(k, append(k, 0xBB), append(realValue, 0xBB))
}

func compareBuckets(t *testing.T, db kv.Tx, b1, b2 string, startKey []byte) {
	t.Helper()
	b1Map := make(map[string]string)
	err := db.ForEach(b1, startKey, func(k, v []byte) error {
		b1Map[fmt.Sprintf("%x", k)] = fmt.Sprintf("%x", v)
		return nil
	})
	require.NoError(t, err)
	b2Map := make(map[string]string)
	err = db.ForEach(b2, nil, func(k, v []byte) error {
		b2Map[fmt.Sprintf("%x", k)] = fmt.Sprintf("%x", v)
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, b1Map, b2Map)
}

func compareBucketsDouble(t *testing.T, db kv.Tx, b1, b2 string) {
	t.Helper()
	b1Map := make(map[string]string)
	err := db.ForEach(b1, nil, func(k, v []byte) error {
		b1Map[fmt.Sprintf("%x", append(k, 0xAA))] = fmt.Sprintf("%x", append(v, 0xAA))
		b1Map[fmt.Sprintf("%x", append(k, 0xBB))] = fmt.Sprintf("%x", append(v, 0xBB))
		return nil
	})
	require.NoError(t, err)
	b2Map := make(map[string]string)
	err = db.ForEach(b2, nil, func(k, v []byte) error {
		b2Map[fmt.Sprintf("%x", k)] = fmt.Sprintf("%x", v)
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, b1Map, b2Map)
}

func TestReuseCollectorAfterLoad(t *testing.T) {
	logger := log.New()
	buf := NewSortableBuffer(128)
	c := NewCollector("", t.TempDir(), buf, logger)

	err := c.Collect([]byte{1}, []byte{2})
	require.NoError(t, err)
	see := 0
	err = c.Load(nil, "", func(k, v []byte, table CurrentTableReader, next LoadNextFunc) error {
		see++
		return nil
	}, TransformArgs{})
	require.NoError(t, err)
	require.Equal(t, 1, see)

	// buffers are not lost
	require.Empty(t, buf.data)
	require.Empty(t, buf.lens)
	require.Empty(t, buf.offsets)
	require.NotZero(t, cap(buf.data))
	require.NotZero(t, cap(buf.lens))
	require.NotZero(t, cap(buf.offsets))

	// teset that no data visible
	see = 0
	err = c.Load(nil, "", func(k, v []byte, table CurrentTableReader, next LoadNextFunc) error {
		see++
		return nil
	}, TransformArgs{})
	require.NoError(t, err)
	require.Equal(t, 0, see)

	// reuse
	see = 0
	err = c.Load(nil, "", func(k, v []byte, table CurrentTableReader, next LoadNextFunc) error {
		see++
		return nil
	}, TransformArgs{})
	require.NoError(t, err)
	require.Equal(t, 0, see)

	err = c.Collect([]byte{3}, []byte{4})
	require.NoError(t, err)
	see = 0
	err = c.Load(nil, "", func(k, v []byte, table CurrentTableReader, next LoadNextFunc) error {
		see++
		return nil
	}, TransformArgs{})
	require.NoError(t, err)
	require.Equal(t, 1, see)
}

func TestAppendAndSortPrefixes(t *testing.T) {
	collector := NewCollector(t.Name(), "", NewAppendBuffer(4), log.New())
	defer collector.Close()
	require := require.New(t)

	key := common.FromHex("ed7229d50cde8de174cc64a882a0833ca5f11669")
	key1 := append(common.Copy(key), make([]byte, 16)...)

	keys := make([]string, 0)
	for i := 10; i >= 0; i-- {
		binary.BigEndian.PutUint64(key1[len(key):], uint64(i))
		binary.BigEndian.PutUint64(key1[len(key)+8:], uint64(i))
		kl := len(key1)
		if i%5 == 0 && i != 0 {
			kl = len(key) + 8
		}
		keys = append(keys, fmt.Sprintf("%x", key1[:kl]))
		require.NoError(collector.Collect(key1[:kl], key1[len(key):]))
	}

	sort.Strings(keys)
	i := 0

	err := collector.Load(nil, "", func(k, v []byte, table CurrentTableReader, next LoadNextFunc) error {
		t.Logf("collated %x %x\n", k, v)
		require.Equalf(keys[i], fmt.Sprintf("%x", k), "i=%d", i)
		i++
		return nil
	}, TransformArgs{})
	require.NoError(err)
}

func TestAppend(t *testing.T) {
	// append buffer doesn't support nil values
	collector := NewCollector(t.Name(), "", NewAppendBuffer(4), log.New())
	defer collector.Close()
	require := require.New(t)
	require.NoError(collector.Collect([]byte{1}, []byte{1}))
	require.NoError(collector.Collect([]byte{1}, []byte{2}))
	require.NoError(collector.Collect([]byte{1}, []byte{3}))
	require.NoError(collector.Collect([]byte{1}, []byte{4}))
	require.NoError(collector.Collect([]byte{1}, []byte{5}))
	require.NoError(collector.Collect([]byte{1}, []byte{6}))
	require.NoError(collector.Collect([]byte{1}, []byte{7}))
	require.NoError(collector.Collect([]byte{2}, []byte{10}))
	require.NoError(collector.Collect([]byte{2}, []byte{20}))
	require.NoError(collector.Collect([]byte{2}, []byte{30}))
	require.NoError(collector.Collect([]byte{2}, []byte{40}))
	require.NoError(collector.Collect([]byte{2}, []byte{50}))
	require.NoError(collector.Collect([]byte{2}, []byte{}))
	require.NoError(collector.Collect([]byte{2}, nil))
	require.NoError(collector.Collect([]byte{3}, nil))
	require.NoError(collector.Load(nil, "", func(k, v []byte, table CurrentTableReader, next LoadNextFunc) error {
		fmt.Printf("%x %x\n", k, v)
		if k[0] == 1 {
			require.Equal([]byte{1, 2, 3, 4, 5, 6, 7}, v)
		} else if k[0] == 2 {
			require.Equal([]byte{10, 20, 30, 40, 50}, v)
		} else {
			require.Nil(v)
		}
		return nil
	}, TransformArgs{}))
}

func TestOldest(t *testing.T) {
	collector := NewCollector(t.Name(), "", NewOldestEntryBuffer(1), log.New())
	defer collector.Close()
	require := require.New(t)
	require.NoError(collector.Collect([]byte{1}, []byte{1}))
	require.NoError(collector.Collect([]byte{1}, []byte{2}))
	require.NoError(collector.Collect([]byte{1}, []byte{3}))
	require.NoError(collector.Collect([]byte{1}, []byte{4}))
	require.NoError(collector.Collect([]byte{1}, []byte{5}))
	require.NoError(collector.Collect([]byte{1}, []byte{6}))
	require.NoError(collector.Collect([]byte{1}, []byte{7}))
	require.NoError(collector.Collect([]byte{2}, nil))
	require.NoError(collector.Load(nil, "", func(k, v []byte, table CurrentTableReader, next LoadNextFunc) error {
		if k[0] == 1 {
			require.Equal([]byte{1}, v)
		} else {
			require.Nil(v)
		}
		return nil
	}, TransformArgs{}))
}

func TestSortable(t *testing.T) {
	collector := NewCollector(t.Name(), "", NewSortableBuffer(1), log.New())
	defer collector.Close()
	require := require.New(t)
	require.NoError(collector.Collect([]byte{1}, []byte{1}))
	require.NoError(collector.Collect([]byte{1}, []byte{2}))
	require.NoError(collector.Collect([]byte{1}, []byte{3}))
	require.NoError(collector.Collect([]byte{1}, []byte{4}))
	require.NoError(collector.Collect([]byte{1}, []byte{5}))
	require.NoError(collector.Collect([]byte{1}, []byte{6}))
	require.NoError(collector.Collect([]byte{1}, []byte{7}))
	require.NoError(collector.Collect([]byte{2}, []byte{1}))
	require.NoError(collector.Collect([]byte{2}, []byte{20}))
	require.NoError(collector.Collect([]byte{2}, nil))

	keys, vals := [][]byte{}, [][]byte{}
	require.NoError(collector.Load(nil, "", func(k, v []byte, table CurrentTableReader, next LoadNextFunc) error {
		keys = append(keys, k)
		vals = append(vals, v)
		return nil
	}, TransformArgs{}))

	require.Equal([][]byte{{1}, {1}, {1}, {1}, {1}, {1}, {1}, {2}, {2}, {2}}, keys)
	require.Equal([][]byte{{1}, {2}, {3}, {4}, {5}, {6}, {7}, {1}, {20}, nil}, vals)

}
