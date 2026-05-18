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
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dir"
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
	m := &mmapBytesReader{data: bb, pos: 0}

	for i := range entries {
		k, err := readField(m)
		require.NoError(t, err)
		v, err := readField(m)
		require.NoError(t, err)
		assert.Equal(t, string(entries[i].key), string(k))
		assert.Equal(t, string(entries[i].value), string(v))
	}

	_, err := readField(m)
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
	c.Close()

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
	c.Close()

	// reuse
	see = 0
	err = c.Load(nil, "", func(k, v []byte, table CurrentTableReader, next LoadNextFunc) error {
		see++
		return nil
	}, TransformArgs{})
	require.NoError(t, err)
	require.Equal(t, 0, see)
	c.Close()

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

func TestSortableBufferStableSort(t *testing.T) {
	buf := NewSortableBuffer(256 * 1024 * 1024)

	// Need enough duplicates to trigger pdqsort's partitioning (not just insertion sort).
	// Insert 1000 entries under each of 4 duplicate keys, interleaved with unique keys.
	dupKey := []byte{0x05, 0x05, 0x05, 0x05, 0x05, 0x05, 0x05, 0x05} // same 8-byte prefix
	const dupsPerKey = 1000
	val := make([]byte, 8)

	for i := range dupsPerKey {
		binary.BigEndian.PutUint64(val, uint64(i))
		buf.Put(dupKey, val)
		// interleave with unique keys to force reordering
		uk := make([]byte, 8)
		binary.BigEndian.PutUint64(uk, uint64(i*3+1))
		buf.Put(uk, val)
	}

	buf.Sort()

	// Verify: all entries with dupKey must appear in insertion order
	seq := 0
	for i := range buf.Len() {
		k, v := buf.Get(i)
		if !bytes.Equal(k, dupKey) {
			continue
		}
		got := binary.BigEndian.Uint64(v)
		require.Equal(t, uint64(seq), got, "duplicate key at position %d: expected insertionOrder %d, got %d", i, seq, got)
		seq++
	}
	require.Equal(t, dupsPerKey, seq, "expected %d duplicate entries", dupsPerKey)
}

func TestSortableBufferNilAndEmptyKeys(t *testing.T) {
	buf := NewSortableBuffer(256 * 1024)

	buf.Put([]byte{0x01}, []byte("normal"))
	buf.Put(nil, []byte("nil-key"))
	buf.Put([]byte{}, []byte("empty-key-1"))
	buf.Put(nil, []byte("nil-key-2"))
	buf.Put([]byte{}, []byte("empty-key-2"))

	buf.Sort()

	// nil and empty keys both sort as zero-length, before non-empty.
	// Stable sort preserves insertion order among equal keys.
	_, v0 := buf.Get(0)
	_, v1 := buf.Get(1)
	_, v2 := buf.Get(2)
	_, v3 := buf.Get(3)
	_, v4 := buf.Get(4)
	assert.Equal(t, []byte("nil-key"), v0)
	assert.Equal(t, []byte("empty-key-1"), v1)
	assert.Equal(t, []byte("nil-key-2"), v2)
	assert.Equal(t, []byte("empty-key-2"), v3)
	assert.Equal(t, []byte("normal"), v4)
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

func BenchmarkFileDataProviderNext(b *testing.B) {
	const keySize = 32
	for _, valSize := range []int{32, 128, 1024} {
		name := fmt.Sprintf("key%d_val%d", keySize, valSize)
		buf := makeSortedBuffer(keySize, valSize, 10_000)

		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				b.StopTimer()
				tmpdir, _ := os.MkdirTemp("", "bench-fdp-")
				provider, err := FlushToDisk("bench", buf, tmpdir, log.LvlInfo)
				if err != nil {
					b.Fatal(err)
				}
				b.StartTimer()

				for {
					_, _, err := provider.Next()
					if err == io.EOF {
						break
					}
					if err != nil {
						b.Fatal(err)
					}
				}

				b.StopTimer()
				provider.Dispose()
				dir.RemoveAll(tmpdir)
				b.StartTimer()
			}
		})
	}
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

func BenchmarkSortableBufferPutSort(b *testing.B) {
	const keyLen = 32
	const valLen = 64

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
			key := make([]byte, keyLen)
			val := make([]byte, valLen)
			buf := NewSortableBuffer(256 * 1024 * 1024)
			buf.Prealloc(tc.count, tc.count*(keyLen+valLen))
			for b.Loop() {
				buf.Reset()
				for i := range tc.count {
					if tc.sorted {
						binary.BigEndian.PutUint64(key, uint64(i))
					} else {
						x := uint64(i) * 6364136223846793005
						binary.BigEndian.PutUint64(key, x)
						binary.BigEndian.PutUint64(key[8:], x^0xdeadbeef)
					}
					binary.BigEndian.PutUint64(val, uint64(i))
					buf.Put(key, val)
				}
				buf.Sort()
			}
		})
	}
}

func BenchmarkSortableBufferPutSortLoad(b *testing.B) {
	const keyLen = 32
	const valLen = 64

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
			key := make([]byte, keyLen)
			val := make([]byte, valLen)
			buf := NewSortableBuffer(256 * 1024 * 1024)
			buf.Prealloc(tc.count, tc.count*(keyLen+valLen))
			for b.Loop() {
				buf.Reset()
				for i := range tc.count {
					if tc.sorted {
						binary.BigEndian.PutUint64(key, uint64(i))
					} else {
						x := uint64(i) * 6364136223846793005
						binary.BigEndian.PutUint64(key, x)
						binary.BigEndian.PutUint64(key[8:], x^0xdeadbeef)
					}
					binary.BigEndian.PutUint64(val, uint64(i))
					buf.Put(key, val)
				}
				buf.Sort()
				// Load phase: iterate sorted buffer like ETL load does
				for i := range buf.Len() {
					_, _ = buf.Get(i)
				}
			}
		})
	}
}

func BenchmarkSortableBufferPutOnly(b *testing.B) {
	const keyLen = 32
	const valLen = 64

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
			key := make([]byte, keyLen)
			val := make([]byte, valLen)
			buf := NewSortableBuffer(256 * 1024 * 1024)
			buf.Prealloc(tc.count, tc.count*(keyLen+valLen))
			for b.Loop() {
				buf.Reset()
				for i := range tc.count {
					if tc.sorted {
						binary.BigEndian.PutUint64(key, uint64(i))
					} else {
						x := uint64(i) * 6364136223846793005
						binary.BigEndian.PutUint64(key, x)
						binary.BigEndian.PutUint64(key[8:], x^0xdeadbeef)
					}
					binary.BigEndian.PutUint64(val, uint64(i))
					buf.Put(key, val)
				}
			}
		})
	}
}

func BenchmarkSortableBufferInmemLoadOnly(b *testing.B) {
	const keyLen = 32
	const valLen = 64

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
			key := make([]byte, keyLen)
			val := make([]byte, valLen)
			buf := NewSortableBuffer(256 * 1024 * 1024)
			buf.Prealloc(tc.count, tc.count*(keyLen+valLen))
			for i := range tc.count {
				if tc.sorted {
					binary.BigEndian.PutUint64(key, uint64(i))
				} else {
					x := uint64(i) * 6364136223846793005
					binary.BigEndian.PutUint64(key, x)
					binary.BigEndian.PutUint64(key[8:], x^0xdeadbeef)
				}
				binary.BigEndian.PutUint64(val, uint64(i))
				buf.Put(key, val)
			}
			buf.Sort()
			for b.Loop() {
				for i := range buf.Len() {
					_, _ = buf.Get(i)
				}
			}
		})
	}
}

func BenchmarkSortableBufferLoadOnly(b *testing.B) {
	const keyLen = 32
	const valLen = 64
	logger := log.New()

	// bufSize is chosen to produce ~5 disk providers per run:
	// 100k entries × 96 bytes ≈ 9.6 MB → bufSize = 2 MB → ~5 flushes
	// 500k entries × 96 bytes ≈ 48 MB → bufSize = 10 MB → ~5 flushes
	for _, tc := range []struct {
		name    string
		count   int
		sorted  bool
		bufSize datasize.ByteSize
	}{
		{"random_100k", 100_000, false, 2 * datasize.MB},
		{"random_500k", 500_000, false, 10 * datasize.MB},
		{"sorted_100k", 100_000, true, 2 * datasize.MB},
		{"sorted_500k", 500_000, true, 10 * datasize.MB},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			key := make([]byte, keyLen)
			val := make([]byte, valLen)
			tmpdir := b.TempDir()
			for b.Loop() {
				b.StopTimer()
				c := NewCollector(b.Name(), tmpdir, NewSortableBuffer(tc.bufSize), logger)
				for i := range tc.count {
					if tc.sorted {
						binary.BigEndian.PutUint64(key, uint64(i))
					} else {
						x := uint64(i) * 6364136223846793005
						binary.BigEndian.PutUint64(key, x)
						binary.BigEndian.PutUint64(key[8:], x^0xdeadbeef)
					}
					binary.BigEndian.PutUint64(val, uint64(i))
					if err := c.Collect(key, val); err != nil {
						b.Fatal(err)
					}
				}
				c.buf.Sort()
				b.StartTimer()
				if err := c.Load(nil, "", func(k, v []byte, _ CurrentTableReader, next LoadNextFunc) error {
					return nil
					//return next(k, k, v)
				}, TransformArgs{}); err != nil {
					b.Fatal(err)
				}
				b.StopTimer()
				c.Close()
				b.StartTimer()
			}
		})
	}
}

var allBufferTypes = []struct {
	name string
	new  func() Buffer
}{
	{"sortable", func() Buffer { return NewSortableBuffer(1 * datasize.MB) }},
	{"append", func() Buffer { return NewAppendBuffer(1 * datasize.MB) }},
	{"oldest", func() Buffer { return NewOldestEntryBuffer(1 * datasize.MB) }},
}

func collectSorted(t *testing.T, buf Buffer, pairs [][2][]byte) [][]byte {
	t.Helper()
	c := NewCollector(t.Name(), "", buf, log.New())
	defer c.Close()
	for _, p := range pairs {
		require.NoError(t, c.Collect(p[0], p[1]))
	}
	var got [][]byte
	require.NoError(t, c.Load(nil, "", func(k, v []byte, _ CurrentTableReader, _ LoadNextFunc) error {
		got = append(got, bytes.Clone(k))
		return nil
	}, TransformArgs{}))
	return got
}

// TestBufferSortShortKey verifies that keys shorter than 8 bytes sort correctly.
// For sortableBuffer this guards against the prefix optimization reading past the
// key into value bytes: e.g. key=[0x01] with value=[0xFF×7] must still sort
// before key=[0x01,0x00].
func TestBufferSortShortKey(t *testing.T) {
	pairs := [][2][]byte{
		{{0x01}, bytes.Repeat([]byte{0xFF}, 7)},
		{{0x01, 0x00}, {0x00}},
	}
	for _, bt := range allBufferTypes {
		t.Run(bt.name, func(t *testing.T) {
			got := collectSorted(t, bt.new(), pairs)
			require.True(t, slices.IsSortedFunc(got, bytes.Compare), "keys not sorted: %x", got)
		})
	}
}

// TestBufferSortAfterReset verifies that reusing a buffer after Load (as sync.Pool
// does for sortableBuffer) produces correct sort order on the second use.
// For sortableBuffer this guards against stale prefix values when clear(prefixes) is missing:
// nil/empty-key slots inherit the old uint64 from the previous sort.
func TestBufferSortAfterReset(t *testing.T) {
	// First use: populate backing arrays with large prefix values (0xFFFF...).
	firstPairs := [][2][]byte{
		{{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}, {1}},
		{{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFE}, {2}},
	}
	// Second use: nil and empty keys must sort before any non-empty key,
	// not inherit the stale 0xFFFF... prefix from the first sort.
	secondPairs := [][2][]byte{
		{nil, {1}},
		{[]byte{}, {2}},
		{{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}, {3}},
	}
	for _, bt := range allBufferTypes {
		t.Run(bt.name, func(t *testing.T) {
			buf := bt.new()
			collectSorted(t, buf, firstPairs) // warms up backing arrays
			buf.Reset()                       // explicit reset, as sync.Pool reuse does
			got := collectSorted(t, buf, secondPairs)
			require.True(t, slices.IsSortedFunc(got, bytes.Compare), "keys not sorted after reset: %x", got)
		})
	}
}

func BenchmarkMemoryDataProviderNext(b *testing.B) {
	for _, keySize := range []int{20, 32, 64} {
		for _, valSize := range []int{32, 128, 256, 1024} {
			name := fmt.Sprintf("key%d_val%d", keySize, valSize)
			buf := makeSortedBuffer(keySize, valSize, 10_000)

			b.Run(name+"/Next", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					p := &memoryDataProvider{buffer: buf, currentIndex: 0}
					for {
						_, _, err := p.Next()
						if err == io.EOF {
							break
						}
						if err != nil {
							b.Fatal(err)
						}
					}
				}
			})

			b.Run(name+"/Get", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					for idx := 0; idx < buf.Len(); idx++ {
						_, _ = buf.Get(idx)
					}
				}
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

func TestVmtouchMmap(t *testing.T) {
	if _, err := exec.LookPath("vmtouch"); err != nil {
		t.Skip("vmtouch not installed")
	}

	tmpdir := t.TempDir()
	const n = 1_000_000
	buf := makeSortedBuffer(32, 1024, n) // ~1GB file

	provider, err := FlushToDisk("test", buf, tmpdir, log.LvlInfo)
	if err != nil {
		t.Fatal(err)
	}
	defer provider.Dispose()

	files, _ := filepath.Glob(filepath.Join(tmpdir, "*"))
	if len(files) == 0 {
		t.Fatal("no temp file found")
	}
	fname := files[0]

	vmtouch := func(label string) {
		fmt.Printf("\n=== %s ===\n", label)
		cmd := exec.Command("vmtouch", "-v", fname)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		cmd.Run()
	}

	vmtouch("BEFORE first Next()")

	// First Next() triggers initMmap + MadviseWillNeed + MadviseSequential
	_, _, err = provider.Next()
	if err != nil {
		t.Fatal(err)
	}
	vmtouch("AFTER first Next() (initMmap + madvise)")

	// Read 25%
	for i := 0; i < n/4-1; i++ {
		provider.Next()
	}
	vmtouch("AFTER 25%")

	// Read to 50%
	for i := 0; i < n/4; i++ {
		provider.Next()
	}
	vmtouch("AFTER 50%")

	// Read to 75%
	for i := 0; i < n/4; i++ {
		provider.Next()
	}
	vmtouch("AFTER 75%")

	// Read rest
	for {
		_, _, err := provider.Next()
		if err == io.EOF {
			break
		}
	}
	vmtouch("AFTER full scan")
}
