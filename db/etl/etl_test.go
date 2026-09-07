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
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"unsafe"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
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
	_, tx := mdbxtest.NewTestTx(t)
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
		k, err := readKeyField(m)
		require.NoError(t, err)
		v, err := readValField(m)
		require.NoError(t, err)
		assert.Equal(t, string(entries[i].key), string(k))
		assert.Equal(t, string(entries[i].value), string(v))
	}

	_, err := readKeyField(m)
	assert.Equal(t, io.EOF, err)
}

func TestNextKey(t *testing.T) {
	for _, tc := range []string{
		"00000001->00000002",
		"000000FF->00000100",
		"FEFFFFFF->FF000000",
	} {
		inputStr, expectedStr, ok := strings.Cut(tc, "->")
		require.True(t, ok)
		input := decodeHex(inputStr)
		expectedOutput := decodeHex(expectedStr)
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
	_, tx := mdbxtest.NewTestTx(t)
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
	_, tx := mdbxtest.NewTestTx(t)
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
	_, tx := mdbxtest.NewTestTx(t)

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
	_, tx := mdbxtest.NewTestTx(t)
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
	_, tx := mdbxtest.NewTestTx(t)
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
	_, tx := mdbxtest.NewTestTx(t)
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
	_, tx := mdbxtest.NewTestTx(t)
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
	_, tx := mdbxtest.NewTestTx(t)
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
	for i := range count {
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
	k1 := make([]byte, len(k)+1)
	copy(k1, k)
	k1[len(k)] = 0xAA
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
	k2 := make([]byte, len(k)+1)
	copy(k2, k)
	k2[len(k)] = 0xBB
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

	// buffer state resets for reuse: entries keep their cap, chunks are cleared
	require.Empty(t, buf.chunks)
	require.Empty(t, buf.entries)
	require.Zero(t, buf.Size())
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
	key1 := append(bytes.Clone(key), make([]byte, 16)...)

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
		switch k[0] {
		case 1:
			require.Equal([]byte{1, 2, 3, 4, 5, 6, 7}, v)
		case 2:
			require.Equal([]byte{10, 20, 30, 40, 50}, v)
		default:
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
// when multiple memoryDataProviders have the same key, across providers backed
// by different buffer types (file-flushed and in-memory).
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
		results = append(results, kv{bytes.Clone(k), bytes.Clone(v)})
		return nil
	}

	err = mergeSortFiles("test", providers, loadFunc,
		TransformArgs{BufferType: SortableAppendBuffer})
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

// drainBuffer reads what the buffer has left, in the order it hands it back.
func drainBuffer(b Buffer) []sortableBufferEntry {
	out := make([]sortableBufferEntry, 0, b.Len())
	for {
		k, v, ok := b.Next()
		if !ok {
			return out
		}
		out = append(out, sortableBufferEntry{key: k, value: v})
	}
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
	for i, e := range drainBuffer(buf) {
		k, v := e.key, e.value
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
	got := drainBuffer(buf)
	assert.Equal(t, []byte("nil-key"), got[0].value)
	assert.Equal(t, []byte("empty-key-1"), got[1].value)
	assert.Equal(t, []byte("nil-key-2"), got[2].value)
	assert.Equal(t, []byte("empty-key-2"), got[3].value)
	assert.Equal(t, []byte("normal"), got[4].value)
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
	for i := range 5 {
		k := fmt.Appendf(nil, "a%02d", i)
		v := fmt.Appendf(nil, "file-val-%02d", i)
		fileBuf.Put(k, v)
	}
	fileProvider, err := FlushToDisk("test", fileBuf, tmpdir, log.LvlInfo)
	require.NoError(t, err)
	require.NotNil(t, fileProvider)

	// Build memory provider
	memBuf := NewSortableBuffer(BufferOptimalSize)
	for i := range 5 {
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
			key:   bytes.Clone(k),
			value: bytes.Clone(v),
		})
		return nil
	}

	err = mergeSortFiles("test", providers, loadFunc, TransformArgs{})
	require.NoError(t, err)

	// Should have all 10 entries in sorted order
	require.Len(t, results, 10)

	// Verify sorted order and correct values
	for i := range 5 {
		assert.Equal(t, fmt.Sprintf("a%02d", i), string(results[i].key), "file key %d", i)
		assert.Equal(t, fmt.Sprintf("file-val-%02d", i), string(results[i].value), "file val %d", i)
	}
	for i := range 5 {
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

	err = mergeSortFiles("test", providers, loadFunc, TransformArgs{})
	require.NoError(t, err)

	require.Len(t, keys, 10)
	// Verify interleaved order
	for i := range 10 {
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
// memoryDataProvider (Next) are not corrupted by subsequent Next() calls.
func TestMixedProvidersZeroCopyIntegrity(t *testing.T) {
	tmpdir := t.TempDir()

	// File provider with 1 key
	fileBuf := NewSortableBuffer(BufferOptimalSize)
	fileBuf.Put([]byte("aaa"), []byte("file-aaa"))
	fileProvider, err := FlushToDisk("test", fileBuf, tmpdir, log.LvlInfo)
	require.NoError(t, err)

	// Memory provider with multiple keys - Next returns slices into sortableBuffer.chunks
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

	err = mergeSortFiles("test", providers, loadFunc, TransformArgs{})
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

func makeSortedBuffer(keySize, valSize, n int) *sortableBuffer {
	buf := NewSortableBuffer(256 * datasize.MB)
	key := make([]byte, keySize)
	val := make([]byte, valSize)
	for range n {
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
		cmd := exec.Command("vmtouch", "-v", fname) //nolint:noctx
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		_ = cmd.Run()
	}

	vmtouch("BEFORE first Next()")

	// First Next() triggers initMmap + MadviseWillNeed + MadviseSequential
	_, _, err = provider.Next()
	if err != nil {
		t.Fatal(err)
	}
	vmtouch("AFTER first Next() (initMmap + madvise)")

	// Read 25%
	for range n/4 - 1 {
		_, _, _ = provider.Next()
	}
	vmtouch("AFTER 25%")

	// Read to 50%
	for range n / 4 {
		_, _, _ = provider.Next()
	}
	vmtouch("AFTER 50%")

	// Read to 75%
	for range n / 4 {
		_, _, _ = provider.Next()
	}
	vmtouch("AFTER 75%")

	// Read rest
	for {
		_, _, err := provider.Next()
		if errors.Is(err, io.EOF) {
			break
		}
	}
	vmtouch("AFTER full scan")
}

// TestCollectorWithAllocatorDrawsBufferLazily pins the lazy-draw contract:
// an allocator-backed collector must not take a pooled buffer until the
// first Collect, so never-written collectors (common when a batch writer
// set is built upfront) cost no buffer at all.
func TestCollectorWithAllocatorDrawsBufferLazily(t *testing.T) {
	logger := log.New()
	_, tx := mdbxtest.NewTestTx(t)
	require := require.New(t)
	table := kv.ChaindataTables[0]

	var draws atomic.Int64
	pool := &sync.Pool{New: func() any {
		draws.Add(1)
		return NewSortableBuffer(BufferOptimalSize)
	}}
	allocator := NewAllocator(pool)

	empty := NewCollectorWithAllocator(t.Name()+"-empty", "", allocator, logger)
	defer empty.Close()
	require.NoError(empty.Flush())
	require.NoError(empty.Load(tx, table, IdentityLoadFunc, TransformArgs{}))
	empty.Close()
	require.Zero(draws.Load(), "empty collector must not draw from the pool")

	c := NewCollectorWithAllocator(t.Name(), "", allocator, logger)
	defer c.Close()
	require.NoError(c.Collect([]byte{1}, []byte{1}))
	require.EqualValues(1, draws.Load(), "first Collect draws exactly one pooled buffer")
	require.NoError(c.Load(tx, table, IdentityLoadFunc, TransformArgs{}))
	v, err := tx.GetOne(table, []byte{1})
	require.NoError(err)
	require.Equal([]byte{1}, v)
}

// TestSortableBufferChunks pins the chunked layout: key/value bytes live in
// fixed-size chunks, so a growing buffer never re-allocates and copies the
// bytes it already holds.
func TestSortableBufferChunks(t *testing.T) {
	buf := NewSortableBuffer(256 * datasize.MB)

	const entries = 512
	val := bytes.Repeat([]byte{0xAB}, 16*1024) // 512*16KB = 8MB of values
	key := make([]byte, 8)
	for i := range entries {
		binary.BigEndian.PutUint64(key, uint64(i))
		buf.Put(key, val)
	}

	require.Equal(t, entries, buf.Len())
	require.Greater(t, len(buf.chunks), 1, "data must be split into chunks")
	for i, c := range buf.chunks {
		require.Equal(t, dataChunkSize, cap(c), "chunk %d", i)
	}

	for i, e := range drainBuffer(buf) {
		binary.BigEndian.PutUint64(key, uint64(i))
		require.Equal(t, key, e.key, "entry %d", i)
		require.Equal(t, val, e.value, "entry %d", i)
	}
}

// TestSortableBufferSortAcrossChunks: the sort comparator has to split a
// packed offset back into a chunk index and an offset inside it, so entries
// must still order correctly once they live past chunk 0.
func TestSortableBufferSortAcrossChunks(t *testing.T) {
	buf := NewSortableBuffer(256 * datasize.MB)

	const entries = 512
	val := bytes.Repeat([]byte{0xCD}, 16*1024) // 512*16KB = 8MB of values
	key := make([]byte, 8)
	for i := range entries {
		// Scrambled, so IsSortedFunc cannot short-circuit and pdqsort really runs.
		// 313 is odd, so it permutes a power-of-two range.
		binary.BigEndian.PutUint64(key, uint64(i*313%entries))
		buf.Put(key, val)
	}
	require.Greater(t, len(buf.chunks), 1, "data must be split into chunks")

	buf.Sort()
	for i, e := range drainBuffer(buf) {
		binary.BigEndian.PutUint64(key, uint64(i))
		require.Equal(t, key, e.key, "entry %d", i)
		require.Equal(t, val, e.value, "entry %d", i)
	}
}

// TestSortableBufferOversizedEntry: an entry bigger than one chunk gets a chunk
// of its own - Next must still return one contiguous slice per key and value.
func TestSortableBufferOversizedEntry(t *testing.T) {
	buf := NewSortableBuffer(256 * datasize.MB)

	big := bytes.Repeat([]byte{0xCD}, dataChunkSize+7)
	buf.Put([]byte{0x01}, []byte("small"))
	buf.Put([]byte{0x02}, big)
	buf.Put([]byte{0x03}, []byte("after"))

	want := drainBuffer(buf)
	require.Equal(t, []byte{0x02}, want[1].key)
	require.Equal(t, big, want[1].value)
	require.Equal(t, []byte{0x03}, want[2].key)
	require.Equal(t, []byte("after"), want[2].value)

	w := bytes.NewBuffer(nil)
	require.NoError(t, buf.Write(w))
	m := &mmapBytesReader{data: w.Bytes()}
	for i := range want {
		wantK, wantV := want[i].key, want[i].value
		gotK, err := readKeyField(m)
		require.NoError(t, err)
		gotV, err := readValField(m)
		require.NoError(t, err)
		require.Equal(t, wantK, gotK)
		require.Equal(t, wantV, gotV)
	}
}

// TestSortableBufferResetReleasesChunks: Reset drops the buffer's own chunk
// slice and size bookkeeping so it can be reused immediately.
func TestSortableBufferResetReleasesChunks(t *testing.T) {
	buf := NewSortableBuffer(256 * datasize.MB)
	val := bytes.Repeat([]byte{0xEF}, 16*1024)
	for i := range 512 {
		buf.Put(binary.BigEndian.AppendUint64(nil, uint64(i)), val)
	}
	require.NotEmpty(t, buf.chunks)

	buf.Reset()
	require.Empty(t, buf.chunks)
	require.Zero(t, buf.Size())
	require.Zero(t, buf.Len())

	buf.Put([]byte{0x01}, []byte("reused"))
	got2 := drainBuffer(buf)
	require.Equal(t, []byte{0x01}, got2[0].key)
	require.Equal(t, []byte("reused"), got2[0].value)
}

// TestPutDataChunkRejectsOversized: an entry's private chunk (bigger than
// dataChunkSize) must never enter the shared pool — a later getDataChunk
// handing it out under a normal chunk index would corrupt an unrelated buffer.
func TestPutDataChunkRejectsOversized(t *testing.T) {
	for _, tc := range []struct {
		name   string
		length int
		pooled bool
	}{
		{"short", dataChunkSize - 1, false},
		{"exact", dataChunkSize, true},
		{"oversized", dataChunkSize + 7, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.pooled, isPooledChunk(make([]byte, tc.length)))
		})
	}
}

// disposeProbe records whether the collector still owned its data chunks when
// the provider was disposed.
type disposeProbe struct {
	buf          *sortableBuffer
	sawOwnChunks bool
}

func (p *disposeProbe) Next() ([]byte, []byte, error) { return nil, nil, io.EOF }
func (p *disposeProbe) Wait() error                   { return nil }
func (p *disposeProbe) String() string                { return "disposeProbe" }
func (p *disposeProbe) Dispose()                      { p.sawOwnChunks = len(p.buf.chunks) > 0 }

// TestCloseDisposesProvidersBeforeBuffer: KeepInRAM hands out a provider backed
// by the collector's own buffer, and Reset gives that buffer's chunks to a pool
// other collectors draw from. So Close must be done with every provider before
// it recycles the buffer.
func TestCloseDisposesProvidersBeforeBuffer(t *testing.T) {
	allocator := NewAllocator(&sync.Pool{New: func() any { return NewSortableBuffer(BufferOptimalSize) }})
	c := NewCollectorWithAllocator(t.Name(), t.TempDir(), allocator, log.New())
	require.NoError(t, c.Collect([]byte{1}, []byte{2}))

	probe := &disposeProbe{buf: c.buf.(*sortableBuffer)}
	c.dataProviders = append(c.dataProviders, probe)
	c.Close()

	require.True(t, probe.sawOwnChunks, "buffer was recycled before its providers were disposed")
}

// TestSortableBufferAllEmptyEntries: entries whose key and value are both
// zero-length keep insertion order too, which they only can if each still has
// an offset of its own. nil and empty stay distinguishable.
func TestSortableBufferAllEmptyEntries(t *testing.T) {
	buf := NewSortableBuffer(256 * 1024)

	// The last-sorting key goes in first, so Sort has to reorder.
	buf.Put([]byte{0xFF}, []byte("last"))
	buf.Put(nil, nil)
	buf.Put([]byte{}, []byte{})
	buf.Put(nil, []byte{})
	buf.Put([]byte{}, nil)

	seen := map[int32]bool{}
	for i := range buf.entries {
		off := buf.entries[i].offset
		require.False(t, seen[off], "entry %d reuses offset %d, so Sort cannot order it", i, off)
		seen[off] = true
	}

	buf.Sort()
	type nilness struct{ key, val bool }
	entries := drainBuffer(buf)
	got := make([]nilness, 4)
	for i := range got {
		got[i] = nilness{entries[i].key == nil, entries[i].value == nil}
	}
	assert.Equal(t, []nilness{{true, true}, {false, false}, {true, false}, {false, true}}, got)

	assert.Equal(t, []byte{0xFF}, entries[4].key)
	assert.Equal(t, []byte("last"), entries[4].value)
}

// TestSortableBufferStableSortAcrossChunks: duplicate keys spread over several
// data chunks are the case the offset tie-break has to get right, since the
// packed offset carries the chunk index in its high bits.
func TestSortableBufferStableSortAcrossChunks(t *testing.T) {
	buf := NewSortableBuffer(256 * datasize.MB)

	dupKey := []byte{0x05, 0x05, 0x05, 0x05, 0x05, 0x05, 0x05, 0x05}
	pad := make([]byte, 4096) // few entries per chunk, so the dups spread out
	val := make([]byte, 8)
	const dups = 1200
	for i := range dups {
		binary.BigEndian.PutUint64(val, uint64(i)) //nolint:gosec
		buf.Put(dupKey, val)
		uk := make([]byte, 8)
		binary.BigEndian.PutUint64(uk, uint64(i*3+1)) //nolint:gosec
		buf.Put(uk, pad)
	}
	require.Greater(t, len(buf.chunks), 4, "dups must spread over several chunks")

	buf.Sort()
	seq := 0
	for i, e := range drainBuffer(buf) {
		if !bytes.Equal(e.key, dupKey) {
			continue
		}
		require.Equal(t, uint64(seq), binary.BigEndian.Uint64(e.value), "dup at position %d", i) //nolint:gosec
		seq++
	}
	require.Equal(t, dups, seq)
}

// TestCollectRejectsOversizedKey: a key spells its length in keyLenSize bytes
// with nilKeyLen reserved, so it has a ceiling. Collect sits under Load and
// the stage loop, which return errors, so it fails the stage not the process.
func TestCollectRejectsOversizedKey(t *testing.T) {
	c := NewCollector(t.Name(), t.TempDir(), NewSortableBuffer(1*datasize.MB), log.New())
	defer c.Close()
	require.NoError(t, c.Collect(make([]byte, MaxKeyLen), []byte("v")))
	err := c.Collect(make([]byte, MaxKeyLen+1), []byte("v"))
	require.Error(t, err)
	require.Contains(t, err.Error(), "exceeds")
}

// TestSortableBufferReadIsAllocFree: reading a sorted buffer must not allocate
// per entry - the slices point into the chunks the buffer already holds.
func TestSortableBufferReadIsAllocFree(t *testing.T) {
	const count = 50_000
	buf := NewSortableBuffer(256 * datasize.MB)
	defer buf.Reset()

	key := make([]byte, 32)
	val := make([]byte, 64)
	for i := range count {
		x := uint64(i) * 6364136223846793005 //nolint:gosec
		binary.BigEndian.PutUint64(key, x)
		binary.BigEndian.PutUint64(key[8:], x^0xdeadbeef)
		buf.Put(key, val)
	}
	require.Greater(t, len(buf.chunks), 2, "the read must cross chunks")
	buf.Sort()

	got := 0
	n := testing.AllocsPerRun(3, func() {
		buf.Sort()
		got = 0
		for _, _, ok := buf.Next(); ok; _, _, ok = buf.Next() {
			got++
		}
	})
	require.Equal(t, count, got)
	require.Zero(t, n, "Sort and a full read must not allocate")
}

// TestEntryLocSize pins the constant to the struct. Size feeds CheckFlushSize,
// so a stale entryLocSize makes every collector spill early.
func TestEntryLocSize(t *testing.T) {
	require.Equal(t, uintptr(entryLocSize), unsafe.Sizeof(entryLoc{}))
}

// TestSpillNilKeyRoundTrip: a nil key crosses a spill file as nilKeyLen, the
// one length keyLenSize cannot otherwise spell. Read back as a real length it
// would desynchronise the rest of the file instead of failing.
func TestSpillNilKeyRoundTrip(t *testing.T) {
	c := NewCollector(t.Name(), t.TempDir(), NewSortableBuffer(1*datasize.MB), log.New())
	defer c.Close()
	require.NoError(t, c.Collect(nil, []byte("nil-key")))
	require.NoError(t, c.Collect([]byte{1}, []byte("one")))
	require.NoError(t, c.Flush()) // spill, so Load reads the file and not RAM

	var got [][2][]byte
	require.NoError(t, c.Load(nil, "", func(k, v []byte, _ CurrentTableReader, _ LoadNextFunc) error {
		got = append(got, [2][]byte{bytes.Clone(k), bytes.Clone(v)})
		return nil
	}, TransformArgs{}))

	require.Len(t, got, 2)
	require.Nil(t, got[0][0])
	require.Equal(t, []byte("nil-key"), got[0][1])
	require.Equal(t, []byte{1}, got[1][0])
	require.Equal(t, []byte("one"), got[1][1])
}

// TestSpillValueLengthCeiling pins why MaxValLen exists: one byte past it the
// length wraps negative, which readValField takes for nil without consuming the
// value's bytes, so every later record is parsed out of value payload.
func TestSpillValueLengthCeiling(t *testing.T) {
	var buf [valLenSize]byte

	putValLen(buf[:], int32(MaxValLen))
	require.EqualValues(t, MaxValLen, int32(binary.NativeEndian.Uint32(buf[:]))) //nolint:gosec

	over := int32(MaxValLen)
	over++ // wraps; a constant expression would not compile
	putValLen(buf[:], over)
	require.Negative(t, int32(binary.NativeEndian.Uint32(buf[:]))) //nolint:gosec
}

func discardLoad(_, _ []byte, _ CurrentTableReader, _ LoadNextFunc) error { return nil }

func emptyPool(bufferSize datasize.ByteSize) *Allocator {
	return NewAllocator(&sync.Pool{New: func() any { return NewSortableBuffer(bufferSize) }})
}

func collectN(t *testing.T, c *Collector, n int) {
	t.Helper()
	key := make([]byte, 8)
	for i := range n {
		binary.BigEndian.PutUint64(key, uint64(i))
		require.NoError(t, c.Collect(key, key))
	}
}

func runCycle(t *testing.T, c *Collector, n int) {
	t.Helper()
	collectN(t, c, n)
	require.NoError(t, c.Load(nil, "", discardLoad, TransformArgs{}))
	c.Close()
}

func purgePool() {
	runtime.GC()
	runtime.GC()
}

func TestCollectorSizesADrawnBufferToTheNamesLastFill(t *testing.T) {
	allocator := emptyPool(BufferOptimalSize)
	runCycle(t, NewCollectorWithAllocator("writer", t.TempDir(), allocator, log.New()), 1000)
	require.Equal(t, 1000, allocator.lastFill("writer"))

	purgePool()
	c := NewCollectorWithAllocator("writer", t.TempDir(), allocator, log.New())
	require.NoError(t, c.Collect([]byte{1}, []byte{1}))
	require.GreaterOrEqual(t, cap(c.buf.(*sortableBuffer).entries), 1000,
		"a buffer drawn from a purged pool must hold the previous cycle's entries without growing")
	c.Close()

	purgePool()
	other := NewCollectorWithAllocator("other", t.TempDir(), allocator, log.New())
	require.NoError(t, other.Collect([]byte{1}, []byte{1}))
	require.Less(t, cap(other.buf.(*sortableBuffer).entries), 1000, "a name that never filled anything gets no hint")
	other.Close()
}

func TestCollectorFillHintFollowsTheLastCycle(t *testing.T) {
	allocator := emptyPool(BufferOptimalSize)
	runCycle(t, NewCollectorWithAllocator("writer", t.TempDir(), allocator, log.New()), 1000)
	require.Equal(t, 1000, allocator.lastFill("writer"))

	c := NewCollectorWithAllocator("writer", t.TempDir(), allocator, log.New())
	runCycle(t, c, 10)
	require.Equal(t, 10, allocator.lastFill("writer"), "the hint must follow the last cycle down, not keep the high-water mark")

	c.Close()
	require.Equal(t, 10, allocator.lastFill("writer"), "a second Close must not erase the hint")

	idle := NewCollectorWithAllocator("writer", t.TempDir(), allocator, log.New())
	idle.Close()
	require.Equal(t, 10, allocator.lastFill("writer"), "a cycle that wrote nothing keeps the previous hint")
}

func TestCollectorFillHintCoversABackgroundSpill(t *testing.T) {
	c := NewCollectorWithAllocator(t.Name(), t.TempDir(), emptyPool(4*datasize.KB), log.New()).SortAndFlushInBackground(true)
	key := make([]byte, 8)
	collected := 0
	for len(c.dataProviders) == 0 {
		binary.BigEndian.PutUint64(key, uint64(collected))
		require.NoError(t, c.Collect(key, key))
		collected++
	}
	require.Nil(t, c.buf, "a background spill hands the buffer to the flusher")
	require.Equal(t, collected, c.fill, "the spilled buffer's fill is the hint for the next draw")

	require.NoError(t, c.Collect([]byte{1}, []byte{1}))
	require.GreaterOrEqual(t, cap(c.buf.(*sortableBuffer).entries), collected)
	require.NoError(t, c.Load(nil, "", discardLoad, TransformArgs{}))
	c.Close()
	require.Equal(t, collected, c.allocator.lastFill(t.Name()))
}

func TestAllocatorFillHintsAreBounded(t *testing.T) {
	allocator := emptyPool(BufferOptimalSize)
	for i := range 2 * maxFillHints {
		c := NewCollectorWithAllocator(fmt.Sprintf("index-%d", i), t.TempDir(), allocator, log.New())
		collectN(t, c, 1)
		c.Close()
		allocator.mu.Lock()
		require.LessOrEqual(t, len(allocator.fills), maxFillHints,
			"a collector named after a one-off file must not add to the hint table forever")
		allocator.mu.Unlock()
	}
	require.Equal(t, 1, allocator.lastFill("index-0"), "an established name survives one-shot name churn")
	require.Zero(t, allocator.lastFill(fmt.Sprintf("index-%d", 2*maxFillHints-1)), "a new name is refused once the table is full")
}
