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
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
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
		entries[i].key = fmt.Appendf(nil, "key-%d", i)
		entries[i].value = fmt.Appendf(nil, "value-%d", i)
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
		TransformArgs{ExtractStartKey: fmt.Appendf(nil, "%10d-key-%010d", 5, 5)},
		logger,
	)
	require.NoError(t, err)
	compareBuckets(t, tx, sourceBucket, destBucket, fmt.Appendf(nil, "%10d-key-%010d", 5, 5))
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
		k := fmt.Appendf(nil, "%10d-key-%010d", i, i)
		v := fmt.Appendf(nil, "val-%099d", i)
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
	require.Empty(t, buf.entries)
	require.NotZero(t, cap(buf.data))
	require.NotZero(t, cap(buf.entries))

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

	slices.Sort(keys)
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

// TestAppendAcrossProviders tests that SortableAppendBuffer correctly concatenates
// values for the same key when they span multiple providers (file flushes).
func TestAppendAcrossProviders(t *testing.T) {
	tmpdir := t.TempDir()
	// Use buffer size of 1 to force every Collect to flush to a separate file provider.
	// Same key {1} appears in multiple providers — merge sort must concatenate values.
	collector := NewCollector(t.Name(), tmpdir, NewAppendBuffer(1), log.New())
	defer collector.Close()
	require := require.New(t)
	require.NoError(collector.Collect([]byte{1}, []byte{10}))
	require.NoError(collector.Collect([]byte{1}, []byte{20}))
	require.NoError(collector.Collect([]byte{1}, []byte{30}))
	require.NoError(collector.Collect([]byte{2}, []byte{40}))
	require.NoError(collector.Collect([]byte{2}, []byte{50}))

	require.NoError(collector.Load(nil, "", func(k, v []byte, table CurrentTableReader, next LoadNextFunc) error {
		if k[0] == 1 {
			require.Equal([]byte{10, 20, 30}, v, "key 1: values should be concatenated across providers")
		} else if k[0] == 2 {
			require.Equal([]byte{40, 50}, v, "key 2: values should be concatenated across providers")
		}
		return nil
	}, TransformArgs{}))
}

// TestAppendAcrossMemProviders tests that value concatenation works correctly
// when multiple memoryDataProviders have the same key. GetRef returns zero-copy
// slices into sortableBuffer.data — appending to prevV without copying would
// corrupt adjacent entries in the buffer.
func TestAppendAcrossMemProviders(t *testing.T) {
	tmpdir := t.TempDir()

	// Provider 0 (file): flushed to disk, read back via mmap zero-copy
	// Provider 1 (memory): kept in RAM via KeepInRAM
	// Same keys across both — merge sort must concatenate values.
	//
	// Provider 0 (file): {1}→{10}, {3}→{30}, {4}→{100,101}
	// Provider 1 (mem):  {1}→{20}, {2}→{25}, {4}→{102,103}
	// Expected merge: {1}→{10,20}, {2}→{25}, {3}→{30}, {4}→{100,101,102,103}

	buf0 := NewAppendBuffer(BufferOptimalSize)
	buf0.Put([]byte{1}, []byte{10})
	buf0.Put([]byte{3}, []byte{30})
	buf0.Put([]byte{4}, []byte{100})
	buf0.Put([]byte{4}, []byte{101})
	fileProvider, err := FlushToDisk("test", buf0, tmpdir, log.LvlInfo)
	require.NoError(t, err)

	buf1 := NewAppendBuffer(BufferOptimalSize)
	buf1.Put([]byte{1}, []byte{20})
	buf1.Put([]byte{2}, []byte{25})
	buf1.Put([]byte{4}, []byte{102})
	buf1.Put([]byte{4}, []byte{103})
	buf1.Sort()

	providers := []dataProvider{fileProvider, KeepInRAM(buf1)}

	type kv struct{ k, v []byte }
	var results []kv
	loadFunc := func(k, v []byte) error {
		results = append(results, kv{common.Copy(k), common.Copy(v)})
		return nil
	}

	err = mergeSortFiles("test", providers, loadFunc,
		TransformArgs{BufferType: SortableAppendBuffer}, NewAppendBuffer(1))
	require.NoError(t, err)

	require.Len(t, results, 4)
	assert.Equal(t, []byte{1}, results[0].k)
	assert.Equal(t, []byte{10, 20}, results[0].v, "key 1: values must be concatenated")
	assert.Equal(t, []byte{2}, results[1].k)
	assert.Equal(t, []byte{25}, results[1].v)
	assert.Equal(t, []byte{3}, results[2].k)
	assert.Equal(t, []byte{30}, results[2].v, "key 3: must not be corrupted by append to key 1's value")
	assert.Equal(t, []byte{4}, results[3].k)
	assert.Equal(t, []byte{100, 101, 102, 103}, results[3].v, "key 4: must not be corrupted by append to key 1's value")

	for _, p := range providers {
		p.Dispose()
	}
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

// TestMixedProvidersMergeSortFiles tests the merge sort with both memoryDataProvider
// and fileDataProvider, verifying that zero-copy returns from both don't corrupt data.
func TestMixedProvidersMergeSortFiles(t *testing.T) {
	logger := log.New()
	tmpdir := t.TempDir()

	// Create entries that will be split across providers:
	// - some go into a memoryDataProvider (kept in RAM)
	// - some go into fileDataProviders (flushed to disk)
	//
	// Use a small buffer so most entries flush to file, but the last batch stays in RAM.

	// We'll manually create providers to control the mix.
	// Provider 0: fileDataProvider with keys "a01".."a05"
	// Provider 1: memoryDataProvider with keys "b01".."b05"

	// Build file provider
	fileBuf := NewSortableBuffer(BufferOptimalSize)
	for i := 0; i < 5; i++ {
		k := fmt.Appendf(nil, "a%02d", i)
		v := fmt.Appendf(nil, "file-val-%02d", i)
		fileBuf.Put(k, v)
	}
	fileProvider, err := FlushToDisk("test", fileBuf, tmpdir, log.LvlInfo)
	require.NoError(t, err)
	require.NotNil(t, fileProvider)

	// Build memory provider
	memBuf := NewSortableBuffer(BufferOptimalSize)
	for i := 0; i < 5; i++ {
		k := fmt.Appendf(nil, "b%02d", i)
		v := fmt.Appendf(nil, "mem-val-%02d", i)
		memBuf.Put(k, v)
	}
	memBuf.Sort()
	memProvider := KeepInRAM(memBuf)

	providers := []dataProvider{fileProvider, memProvider}

	// Collect results
	var results []sortableBufferEntry
	loadFunc := func(k, v []byte) error {
		// Must copy because providers return zero-copy references
		results = append(results, sortableBufferEntry{
			key:   common.Copy(k),
			value: common.Copy(v),
		})
		return nil
	}

	err = mergeSortFiles("test", providers, loadFunc, TransformArgs{}, NewSortableBuffer(1))
	require.NoError(t, err)

	// Should have all 10 entries in sorted order
	require.Len(t, results, 10)

	// Verify sorted order and correct values
	for i := 0; i < 5; i++ {
		assert.Equal(t, fmt.Sprintf("a%02d", i), string(results[i].key), "file key %d", i)
		assert.Equal(t, fmt.Sprintf("file-val-%02d", i), string(results[i].value), "file val %d", i)
	}
	for i := 0; i < 5; i++ {
		assert.Equal(t, fmt.Sprintf("b%02d", i), string(results[5+i].key), "mem key %d", i)
		assert.Equal(t, fmt.Sprintf("mem-val-%02d", i), string(results[5+i].value), "mem val %d", i)
	}

	// Cleanup
	for _, p := range providers {
		p.Dispose()
	}

	_ = logger
}

// TestMixedProvidersInterleavedKeys tests merge sort with interleaved keys
// from both memory and file providers, ensuring correct ordering.
func TestMixedProvidersInterleavedKeys(t *testing.T) {
	tmpdir := t.TempDir()

	// File provider: even keys
	fileBuf := NewSortableBuffer(BufferOptimalSize)
	for i := 0; i < 10; i += 2 {
		k := fmt.Appendf(nil, "key-%04d", i)
		v := fmt.Appendf(nil, "file-%04d", i)
		fileBuf.Put(k, v)
	}
	fileProvider, err := FlushToDisk("test", fileBuf, tmpdir, log.LvlInfo)
	require.NoError(t, err)

	// Memory provider: odd keys
	memBuf := NewSortableBuffer(BufferOptimalSize)
	for i := 1; i < 10; i += 2 {
		k := fmt.Appendf(nil, "key-%04d", i)
		v := fmt.Appendf(nil, "mem-%04d", i)
		memBuf.Put(k, v)
	}
	memBuf.Sort()
	memProvider := KeepInRAM(memBuf)

	providers := []dataProvider{fileProvider, memProvider}

	var keys, vals []string
	loadFunc := func(k, v []byte) error {
		keys = append(keys, string(k))
		vals = append(vals, string(v))
		return nil
	}

	err = mergeSortFiles("test", providers, loadFunc, TransformArgs{}, NewSortableBuffer(1))
	require.NoError(t, err)

	require.Len(t, keys, 10)
	// Verify interleaved order
	for i := 0; i < 10; i++ {
		assert.Equal(t, fmt.Sprintf("key-%04d", i), keys[i])
		if i%2 == 0 {
			assert.Equal(t, fmt.Sprintf("file-%04d", i), vals[i])
		} else {
			assert.Equal(t, fmt.Sprintf("mem-%04d", i), vals[i])
		}
	}

	for _, p := range providers {
		p.Dispose()
	}
}

// TestMixedProvidersZeroCopyIntegrity verifies that zero-copy slices from
// memoryDataProvider (GetRef) are not corrupted by subsequent Next() calls.
func TestMixedProvidersZeroCopyIntegrity(t *testing.T) {
	tmpdir := t.TempDir()

	// File provider with 1 key
	fileBuf := NewSortableBuffer(BufferOptimalSize)
	fileBuf.Put([]byte("aaa"), []byte("file-aaa"))
	fileProvider, err := FlushToDisk("test", fileBuf, tmpdir, log.LvlInfo)
	require.NoError(t, err)

	// Memory provider with multiple keys - GetRef returns slices into sortableBuffer.data
	memBuf := NewSortableBuffer(BufferOptimalSize)
	memBuf.Put([]byte("bbb"), []byte("mem-bbb"))
	memBuf.Put([]byte("ccc"), []byte("mem-ccc"))
	memBuf.Put([]byte("ddd"), []byte("mem-ddd"))
	memBuf.Sort()
	memProvider := KeepInRAM(memBuf)

	providers := []dataProvider{fileProvider, memProvider}

	// Capture zero-copy references and verify they remain valid
	type entry struct {
		key []byte
		val []byte
	}
	var entries []entry

	loadFunc := func(k, v []byte) error {
		// Intentionally NOT copying - to test that zero-copy refs stay valid
		entries = append(entries, entry{key: k, val: v})
		return nil
	}

	err = mergeSortFiles("test", providers, loadFunc, TransformArgs{}, NewSortableBuffer(1))
	require.NoError(t, err)

	require.Len(t, entries, 4)
	// Verify all entries still have correct data (not corrupted by subsequent reads)
	assert.Equal(t, "aaa", string(entries[0].key))
	assert.Equal(t, "file-aaa", string(entries[0].val))
	assert.Equal(t, "bbb", string(entries[1].key))
	assert.Equal(t, "mem-bbb", string(entries[1].val))
	assert.Equal(t, "ccc", string(entries[2].key))
	assert.Equal(t, "mem-ccc", string(entries[2].val))
	assert.Equal(t, "ddd", string(entries[3].key))
	assert.Equal(t, "mem-ddd", string(entries[3].val))

	for _, p := range providers {
		p.Dispose()
	}
}

func BenchmarkMemoryDataProviderNext(b *testing.B) {
	for _, keySize := range []int{20, 32, 64} {
		for _, valSize := range []int{32, 128, 256, 1024} {
			name := fmt.Sprintf("key%d_val%d", keySize, valSize)
			buf := makeSortedBuffer(keySize, valSize, 10_000)

			b.Run(name+"/GetRef", func(b *testing.B) {
				b.ReportAllocs()
				var keyBuf, valBuf []byte
				for i := 0; i < b.N; i++ {
					p := &memoryDataProvider{buffer: buf, currentIndex: 0}
					for {
						var err error
						keyBuf, valBuf, err = p.Next()
						if err == io.EOF {
							break
						}
						if err != nil {
							b.Fatal(err)
						}
					}
				}
				_ = keyBuf
				_ = valBuf
			})

			b.Run(name+"/Get", func(b *testing.B) {
				b.ReportAllocs()
				var keyBuf, valBuf []byte
				for i := 0; i < b.N; i++ {
					idx := 0
					for idx < buf.Len() {
						keyBuf, valBuf = buf.Get(idx)
						idx++
					}
				}
				_ = keyBuf
				_ = valBuf
			})
		}
	}
}

func makeSortedBuffer(keySize, valSize, n int) *sortableBuffer {
	buf := NewSortableBuffer(256 * datasize.MB)
	key := make([]byte, keySize)
	val := make([]byte, valSize)
	for i := 0; i < n; i++ {
		rand.Read(key)
		rand.Read(val)
		buf.Put(key, val)
	}
	buf.Sort()
	return buf
}

func BenchmarkCollect(b *testing.B) {
	logger := log.New()
	const keyLen = 32
	const valLen = 128

	for _, tc := range []struct {
		name    string
		count   int
		bufSize datasize.ByteSize
	}{
		{"10k_smallbuf", 10_000, 64 * datasize.KB},
		{"10k_largebuf", 10_000, 256 * datasize.MB},
		{"100k_smallbuf", 100_000, 256 * datasize.KB},
		{"100k_largebuf", 100_000, 256 * datasize.MB},
	} {
		// Pre-generate deterministic keys/values
		keys := make([][]byte, tc.count)
		vals := make([][]byte, tc.count)
		for i := range tc.count {
			k := make([]byte, keyLen)
			binary.BigEndian.PutUint64(k, uint64(i)*6364136223846793005)
			keys[i] = k
			v := make([]byte, valLen)
			binary.BigEndian.PutUint64(v, uint64(i))
			vals[i] = v
		}

		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			tmpdir := b.TempDir()
			for b.Loop() {
				c := NewCollector("bench", tmpdir, NewSortableBuffer(tc.bufSize), logger)
				for i := range tc.count {
					if err := c.Collect(keys[i], vals[i]); err != nil {
						b.Fatal(err)
					}
				}
				c.Close()
			}
		})
	}
}

func BenchmarkMergeSortFiles(b *testing.B) {
	logger := log.New()
	const keyLen = 32
	const valLen = 128

	for _, tc := range []struct {
		name         string
		count        int
		bufSize      datasize.ByteSize
		expectOnDisk bool // true when bufSize is small enough to force file providers
	}{
		{"mem_only_10k", 10_000, 256 * datasize.MB, false},
		{"file_only_10k", 10_000, 64 * datasize.KB, true},
		{"file_only_100k", 100_000, 256 * datasize.KB, true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			tmpdir := b.TempDir()

			// Pre-generate deterministic keys/values
			keys := make([][]byte, tc.count)
			vals := make([][]byte, tc.count)
			for i := range tc.count {
				k := make([]byte, keyLen)
				binary.BigEndian.PutUint64(k, uint64(i)*6364136223846793005)
				keys[i] = k
				v := make([]byte, valLen)
				binary.BigEndian.PutUint64(v, uint64(i))
				vals[i] = v
			}

			for b.Loop() {
				c := NewCollector("bench", tmpdir, NewSortableBuffer(tc.bufSize), logger)
				for i := range tc.count {
					if err := c.Collect(keys[i], vals[i]); err != nil {
						b.Fatal(err)
					}
				}
				if err := c.Load(nil, "", func(k, v []byte, _ CurrentTableReader, next LoadNextFunc) error {
					return nil
				}, TransformArgs{}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkSortableBufferSort(b *testing.B) {
	const keyLen = 32
	const valLen = 64

	makeBuffer := func(n int, sorted bool) *sortableBuffer {
		buf := NewSortableBuffer(256 * 1024 * 1024)
		buf.Prealloc(n, n*(keyLen+valLen))
		key := make([]byte, keyLen)
		val := make([]byte, valLen)
		for i := range n {
			if sorted {
				binary.BigEndian.PutUint64(key, uint64(i))
			} else {
				// deterministic pseudo-random: mix the index
				x := uint64(i) * 6364136223846793005
				binary.BigEndian.PutUint64(key, x)
				binary.BigEndian.PutUint64(key[8:], x^0xdeadbeef)
			}
			binary.BigEndian.PutUint64(val, uint64(i))
			buf.Put(key, val)
		}
		return buf
	}

	for _, tc := range []struct {
		name   string
		count  int
		sorted bool
	}{
		{"random_100k", 100_000, false},
		{"random_500k", 500_000, false},
		{"sorted_100k", 100_000, true},
		{"sorted_500k", 500_000, true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				b.StopTimer()
				ref := makeBuffer(tc.count, tc.sorted)
				b.StartTimer()
				ref.Sort()
			}
		})
	}
}
