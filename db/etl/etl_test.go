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
	"slices"
	"strings"
	"testing"

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

// ==================== Phase 1: DupSort Value Sorting Tests ====================

func TestSortByKeyAndValue(t *testing.T) {
	// Verify that SortByKeyAndValue sorts entries by (key, value) pairs
	buf := NewSortableBuffer(BufferOptimalSize)

	// Add entries with same key but different values in reverse order
	buf.Put([]byte{0x01}, []byte{0x03})
	buf.Put([]byte{0x01}, []byte{0x01})
	buf.Put([]byte{0x01}, []byte{0x02})
	// Add entries with different keys
	buf.Put([]byte{0x02}, []byte{0x02})
	buf.Put([]byte{0x02}, []byte{0x01})

	buf.SortByKeyAndValue()

	// Verify order: (0x01,0x01), (0x01,0x02), (0x01,0x03), (0x02,0x01), (0x02,0x02)
	expected := []struct{ k, v []byte }{
		{[]byte{0x01}, []byte{0x01}},
		{[]byte{0x01}, []byte{0x02}},
		{[]byte{0x01}, []byte{0x03}},
		{[]byte{0x02}, []byte{0x01}},
		{[]byte{0x02}, []byte{0x02}},
	}
	require.Equal(t, len(expected), buf.Len())
	for i, exp := range expected {
		k, v := buf.Get(i, nil, nil)
		require.Equal(t, exp.k, k, "key mismatch at index %d", i)
		require.Equal(t, exp.v, v, "value mismatch at index %d", i)
	}
}

func TestDupSortValueSorting(t *testing.T) {
	// Test that loading into a DupSort table with values in reverse order succeeds
	// (would fail with MDBX_EKEYMISMATCH without Phase 1 sorting)
	logger := log.New()
	_, tx := memdb.NewTestTx(t)

	table := kv.TblAccountIdx // DupSort table

	collector := NewCollector(t.Name(), "", NewSortableBuffer(BufferOptimalSize), logger)
	defer collector.Close()

	// Collect entries with same key but values in reverse order
	key := []byte{0x00, 0x00, 0x00, 0x01}
	require.NoError(t, collector.Collect(key, []byte{0x00, 0x00, 0x00, 0x03}))
	require.NoError(t, collector.Collect(key, []byte{0x00, 0x00, 0x00, 0x01}))
	require.NoError(t, collector.Collect(key, []byte{0x00, 0x00, 0x00, 0x02}))

	// Load should succeed — Phase 1 sorts by (key, value) for DupSort tables
	require.NoError(t, collector.Load(tx, table, IdentityLoadFunc, TransformArgs{}))

	// Verify entries are in correct order using DupSort cursor
	cur, err := tx.CursorDupSort(table)
	require.NoError(t, err)
	defer cur.Close()

	var keys, vals [][]byte
	for k, v, err := cur.First(); k != nil; k, v, err = cur.Next() {
		require.NoError(t, err)
		keys = append(keys, common.Copy(k))
		vals = append(vals, common.Copy(v))
	}
	require.Equal(t, 3, len(keys))
	require.Equal(t, []byte{0x00, 0x00, 0x00, 0x01}, vals[0])
	require.Equal(t, []byte{0x00, 0x00, 0x00, 0x02}, vals[1])
	require.Equal(t, []byte{0x00, 0x00, 0x00, 0x03}, vals[2])
}

func TestDupSortMixedKeys(t *testing.T) {
	// Test multiple keys each with multiple values in random/reverse order
	logger := log.New()
	_, tx := memdb.NewTestTx(t)

	table := kv.TblAccountIdx // DupSort table

	collector := NewCollector(t.Name(), "", NewSortableBuffer(BufferOptimalSize), logger)
	defer collector.Close()

	// Key 2 values in reverse
	require.NoError(t, collector.Collect([]byte{0x02}, []byte{0x03}))
	require.NoError(t, collector.Collect([]byte{0x02}, []byte{0x01}))
	// Key 1 values in reverse
	require.NoError(t, collector.Collect([]byte{0x01}, []byte{0x02}))
	require.NoError(t, collector.Collect([]byte{0x01}, []byte{0x01}))
	// Key 3 values in reverse
	require.NoError(t, collector.Collect([]byte{0x03}, []byte{0x02}))
	require.NoError(t, collector.Collect([]byte{0x03}, []byte{0x01}))

	require.NoError(t, collector.Load(tx, table, IdentityLoadFunc, TransformArgs{}))

	// Verify all entries present and correctly ordered
	cur, err := tx.CursorDupSort(table)
	require.NoError(t, err)
	defer cur.Close()

	type kv2 struct{ k, v byte }
	var results []kv2
	for k, v, err := cur.First(); k != nil; k, v, err = cur.Next() {
		require.NoError(t, err)
		results = append(results, kv2{k[0], v[0]})
	}
	expected := []kv2{
		{0x01, 0x01}, {0x01, 0x02},
		{0x02, 0x01}, {0x02, 0x03},
		{0x03, 0x01}, {0x03, 0x02},
	}
	require.Equal(t, expected, results)
}

func TestDupSortMultipleFlushes(t *testing.T) {
	// Test with very small buffer to force multiple disk flushes
	// This exercises the DupSort-aware heap merge
	logger := log.New()
	_, tx := memdb.NewTestTx(t)

	table := kv.TblAccountIdx // DupSort table

	// Very small buffer to force disk flushes
	collector := NewCollector(t.Name(), t.TempDir(), NewSortableBuffer(1), logger)
	defer collector.Close()

	// Same key, values spread across flushes
	key := []byte{0x00, 0x01}
	require.NoError(t, collector.Collect(key, []byte{0x05}))
	require.NoError(t, collector.Collect(key, []byte{0x03}))
	require.NoError(t, collector.Collect(key, []byte{0x01}))
	require.NoError(t, collector.Collect(key, []byte{0x04}))
	require.NoError(t, collector.Collect(key, []byte{0x02}))

	require.NoError(t, collector.Load(tx, table, IdentityLoadFunc, TransformArgs{}))

	// Verify correct ordering
	cur, err := tx.CursorDupSort(table)
	require.NoError(t, err)
	defer cur.Close()

	var vals []byte
	for k, v, err := cur.First(); k != nil; k, v, err = cur.Next() {
		require.NoError(t, err)
		vals = append(vals, v[0])
	}
	require.Equal(t, []byte{0x01, 0x02, 0x03, 0x04, 0x05}, vals)
}

func TestNonDupSortUnaffected(t *testing.T) {
	// Verify that non-DupSort tables are unaffected by the changes
	logger := log.New()
	_, tx := memdb.NewTestTx(t)

	table := kv.ChaindataTables[0] // Non-DupSort table

	collector := NewCollector(t.Name(), "", NewSortableBuffer(BufferOptimalSize), logger)
	defer collector.Close()

	// Collect entries in reverse key order
	require.NoError(t, collector.Collect([]byte{0x03}, []byte{0x30}))
	require.NoError(t, collector.Collect([]byte{0x01}, []byte{0x10}))
	require.NoError(t, collector.Collect([]byte{0x02}, []byte{0x20}))

	require.NoError(t, collector.Load(tx, table, IdentityLoadFunc, TransformArgs{}))

	// Verify entries sorted by key only
	v, err := tx.GetOne(table, []byte{0x01})
	require.NoError(t, err)
	require.Equal(t, []byte{0x10}, v)

	v, err = tx.GetOne(table, []byte{0x02})
	require.NoError(t, err)
	require.Equal(t, []byte{0x20}, v)

	v, err = tx.GetOne(table, []byte{0x03})
	require.NoError(t, err)
	require.Equal(t, []byte{0x30}, v)
}

// ==================== Phase 2: Dynamic canUseAppend Tests ====================

func TestDynamicCanUseAppend_OverlapThenAppend(t *testing.T) {
	// Test that dynamic canUseAppend transitions from Put to Append
	// when ETL keys pass the last key in the table
	t.Setenv("ETL_DYNAMIC_APPEND", "1")
	// Reset the package-level var for this test
	origVal := useDynamicAppend
	useDynamicAppend = true
	defer func() { useDynamicAppend = origVal }()

	logger := log.New()
	_, tx := memdb.NewTestTx(t)

	table := kv.ChaindataTables[0] // Non-DupSort table

	// Pre-populate table with keys 0x01..0x05
	for i := byte(1); i <= 5; i++ {
		require.NoError(t, tx.Put(table, []byte{i}, []byte{i * 10}))
	}

	collector := NewCollector(t.Name(), "", NewSortableBuffer(BufferOptimalSize), logger)
	defer collector.Close()

	// ETL keys: 0x03..0x08 (overlap: 0x03-0x05, append: 0x06-0x08)
	for i := byte(3); i <= 8; i++ {
		require.NoError(t, collector.Collect([]byte{i}, []byte{i * 20}))
	}

	require.NoError(t, collector.Load(tx, table, IdentityLoadFunc, TransformArgs{}))

	// Verify all keys present with correct values
	for i := byte(1); i <= 2; i++ {
		v, err := tx.GetOne(table, []byte{i})
		require.NoError(t, err)
		require.Equal(t, []byte{i * 10}, v, "key %d should have original value", i)
	}
	for i := byte(3); i <= 8; i++ {
		v, err := tx.GetOne(table, []byte{i})
		require.NoError(t, err)
		require.Equal(t, []byte{i * 20}, v, "key %d should have ETL value", i)
	}
}

func TestDynamicCanUseAppend_DupSort(t *testing.T) {
	// Test dynamic canUseAppend with DupSort tables
	t.Setenv("ETL_DYNAMIC_APPEND", "1")
	origVal := useDynamicAppend
	useDynamicAppend = true
	defer func() { useDynamicAppend = origVal }()

	logger := log.New()
	_, tx := memdb.NewTestTx(t)

	table := kv.TblAccountIdx // DupSort table

	// Pre-populate with key=0x01, values=0x01,0x02
	dupCur, err := tx.RwCursorDupSort(table)
	require.NoError(t, err)
	require.NoError(t, dupCur.Put([]byte{0x01}, []byte{0x01}))
	require.NoError(t, dupCur.Put([]byte{0x01}, []byte{0x02}))
	require.NoError(t, dupCur.Put([]byte{0x02}, []byte{0x01}))
	dupCur.Close()

	collector := NewCollector(t.Name(), "", NewSortableBuffer(BufferOptimalSize), logger)
	defer collector.Close()

	// ETL: add new values for existing keys and new keys
	require.NoError(t, collector.Collect([]byte{0x01}, []byte{0x03})) // extends existing key
	require.NoError(t, collector.Collect([]byte{0x02}, []byte{0x02})) // extends existing key
	require.NoError(t, collector.Collect([]byte{0x03}, []byte{0x01})) // new key

	require.NoError(t, collector.Load(tx, table, IdentityLoadFunc, TransformArgs{}))

	// Verify all entries
	cur, err := tx.CursorDupSort(table)
	require.NoError(t, err)
	defer cur.Close()

	type kv2 struct{ k, v byte }
	var results []kv2
	for k, v, err := cur.First(); k != nil; k, v, err = cur.Next() {
		require.NoError(t, err)
		results = append(results, kv2{k[0], v[0]})
	}
	expected := []kv2{
		{0x01, 0x01}, {0x01, 0x02}, {0x01, 0x03},
		{0x02, 0x01}, {0x02, 0x02},
		{0x03, 0x01},
	}
	require.Equal(t, expected, results)
}

func TestDynamicCanUseAppend_EmptyTable(t *testing.T) {
	// Test that empty table uses Append from the start
	t.Setenv("ETL_DYNAMIC_APPEND", "1")
	origVal := useDynamicAppend
	useDynamicAppend = true
	defer func() { useDynamicAppend = origVal }()

	logger := log.New()
	_, tx := memdb.NewTestTx(t)

	table := kv.ChaindataTables[0]

	collector := NewCollector(t.Name(), "", NewSortableBuffer(BufferOptimalSize), logger)
	defer collector.Close()

	for i := byte(1); i <= 10; i++ {
		require.NoError(t, collector.Collect([]byte{i}, []byte{i}))
	}

	require.NoError(t, collector.Load(tx, table, IdentityLoadFunc, TransformArgs{}))

	// Verify all entries present
	for i := byte(1); i <= 10; i++ {
		v, err := tx.GetOne(table, []byte{i})
		require.NoError(t, err)
		require.Equal(t, []byte{i}, v)
	}
}

func TestDynamicCanUseAppend_AllOverlap(t *testing.T) {
	// Test when all ETL keys overlap with existing data (all Put, no Append)
	t.Setenv("ETL_DYNAMIC_APPEND", "1")
	origVal := useDynamicAppend
	useDynamicAppend = true
	defer func() { useDynamicAppend = origVal }()

	logger := log.New()
	_, tx := memdb.NewTestTx(t)

	table := kv.ChaindataTables[0]

	// Pre-populate with keys 0x01..0x10
	for i := byte(1); i <= 0x10; i++ {
		require.NoError(t, tx.Put(table, []byte{i}, []byte{i}))
	}

	collector := NewCollector(t.Name(), "", NewSortableBuffer(BufferOptimalSize), logger)
	defer collector.Close()

	// ETL keys all within existing range
	for i := byte(1); i <= 5; i++ {
		require.NoError(t, collector.Collect([]byte{i}, []byte{i * 2}))
	}

	require.NoError(t, collector.Load(tx, table, IdentityLoadFunc, TransformArgs{}))

	// Verify updated values
	for i := byte(1); i <= 5; i++ {
		v, err := tx.GetOne(table, []byte{i})
		require.NoError(t, err)
		require.Equal(t, []byte{i * 2}, v)
	}
	// Verify untouched values
	for i := byte(6); i <= 0x10; i++ {
		v, err := tx.GetOne(table, []byte{i})
		require.NoError(t, err)
		require.Equal(t, []byte{i}, v)
	}
}

func TestDynamicCanUseAppend_NoSortingGuaranties(t *testing.T) {
	// Test that custom loadFunc (no sorting guarantees) still uses Put
	t.Setenv("ETL_DYNAMIC_APPEND", "1")
	origVal := useDynamicAppend
	useDynamicAppend = true
	defer func() { useDynamicAppend = origVal }()

	logger := log.New()
	_, tx := memdb.NewTestTx(t)

	table := kv.ChaindataTables[0]

	collector := NewCollector(t.Name(), "", NewSortableBuffer(BufferOptimalSize), logger)
	defer collector.Close()

	require.NoError(t, collector.Collect([]byte{0x01}, []byte{0x01}))
	require.NoError(t, collector.Collect([]byte{0x02}, []byte{0x02}))

	// Use custom loadFunc (not IdentityLoadFunc) — should force Put path
	customLoad := func(k, v []byte, table CurrentTableReader, next LoadNextFunc) error {
		return next(k, k, v)
	}
	require.NoError(t, collector.Load(tx, table, customLoad, TransformArgs{}))

	v, err := tx.GetOne(table, []byte{0x01})
	require.NoError(t, err)
	require.Equal(t, []byte{0x01}, v)
}

func TestDynamicCanUseAppend_FeatureFlag(t *testing.T) {
	// Test that without ETL_DYNAMIC_APPEND, behavior is unchanged (one-shot check)
	origVal := useDynamicAppend
	useDynamicAppend = false
	defer func() { useDynamicAppend = origVal }()

	logger := log.New()
	_, tx := memdb.NewTestTx(t)

	table := kv.ChaindataTables[0]

	// Pre-populate with key 0x05
	require.NoError(t, tx.Put(table, []byte{0x05}, []byte{0x50}))

	collector := NewCollector(t.Name(), "", NewSortableBuffer(BufferOptimalSize), logger)
	defer collector.Close()

	// ETL keys start before lastKey — one-shot check should set canUseAppend=false
	require.NoError(t, collector.Collect([]byte{0x03}, []byte{0x30}))
	require.NoError(t, collector.Collect([]byte{0x06}, []byte{0x60}))
	require.NoError(t, collector.Collect([]byte{0x07}, []byte{0x70}))

	// Should succeed (all via Put since canUseAppend=false from first key)
	require.NoError(t, collector.Load(tx, table, IdentityLoadFunc, TransformArgs{}))

	v, err := tx.GetOne(table, []byte{0x03})
	require.NoError(t, err)
	require.Equal(t, []byte{0x30}, v)
	v, err = tx.GetOne(table, []byte{0x06})
	require.NoError(t, err)
	require.Equal(t, []byte{0x60}, v)
}

func TestDynamicCanUseAppend_DupSort_Duplicates(t *testing.T) {
	// Test that duplicate (key, value) pairs are skipped for DupSort tables
	t.Setenv("ETL_DYNAMIC_APPEND", "1")
	origVal := useDynamicAppend
	useDynamicAppend = true
	defer func() { useDynamicAppend = origVal }()

	logger := log.New()
	_, tx := memdb.NewTestTx(t)

	table := kv.TblAccountIdx // DupSort table

	collector := NewCollector(t.Name(), "", NewSortableBuffer(BufferOptimalSize), logger)
	defer collector.Close()

	// Collect duplicate (key, value) pairs
	require.NoError(t, collector.Collect([]byte{0x01}, []byte{0x01}))
	require.NoError(t, collector.Collect([]byte{0x01}, []byte{0x01})) // duplicate
	require.NoError(t, collector.Collect([]byte{0x01}, []byte{0x02}))
	require.NoError(t, collector.Collect([]byte{0x01}, []byte{0x02})) // duplicate

	require.NoError(t, collector.Load(tx, table, IdentityLoadFunc, TransformArgs{}))

	// Verify only unique entries
	cur, err := tx.CursorDupSort(table)
	require.NoError(t, err)
	defer cur.Close()

	var vals []byte
	for k, v, err := cur.First(); k != nil; k, v, err = cur.Next() {
		require.NoError(t, err)
		vals = append(vals, v[0])
	}
	require.Equal(t, []byte{0x01, 0x02}, vals)
}

func TestDynamicCanUseAppend_DupSort_DuplicatesAcrossFlushes(t *testing.T) {
	// Test duplicate detection across multiple disk flushes
	t.Setenv("ETL_DYNAMIC_APPEND", "1")
	origVal := useDynamicAppend
	useDynamicAppend = true
	defer func() { useDynamicAppend = origVal }()

	logger := log.New()
	_, tx := memdb.NewTestTx(t)

	table := kv.TblAccountIdx // DupSort table

	// Very small buffer to force disk flushes
	collector := NewCollector(t.Name(), t.TempDir(), NewSortableBuffer(1), logger)
	defer collector.Close()

	// Entries that will end up as duplicates after merge-sort
	require.NoError(t, collector.Collect([]byte{0x01}, []byte{0x01}))
	require.NoError(t, collector.Collect([]byte{0x01}, []byte{0x01})) // duplicate in different flush
	require.NoError(t, collector.Collect([]byte{0x01}, []byte{0x02}))
	require.NoError(t, collector.Collect([]byte{0x02}, []byte{0x01}))

	require.NoError(t, collector.Load(tx, table, IdentityLoadFunc, TransformArgs{}))

	// Verify correct entries
	cur, err := tx.CursorDupSort(table)
	require.NoError(t, err)
	defer cur.Close()

	type kv2 struct{ k, v byte }
	var results []kv2
	for k, v, err := cur.First(); k != nil; k, v, err = cur.Next() {
		require.NoError(t, err)
		results = append(results, kv2{k[0], v[0]})
	}
	expected := []kv2{
		{0x01, 0x01}, {0x01, 0x02},
		{0x02, 0x01},
	}
	require.Equal(t, expected, results)
}

// ==================== Benchmarks ====================

func BenchmarkSortByKeyAndValue(b *testing.B) {
	const keyLen = 8
	const valLen = 8

	makeBuffer := func(n int, sameKey bool) *sortableBuffer {
		buf := NewSortableBuffer(256 * 1024 * 1024)
		buf.Prealloc(n, n*(keyLen+valLen))
		key := make([]byte, keyLen)
		val := make([]byte, valLen)
		for i := range n {
			if sameKey {
				binary.BigEndian.PutUint64(key, 1) // all same key
			} else {
				binary.BigEndian.PutUint64(key, uint64(i/3)) // ~3 values per key
			}
			// Values in reverse order to force sorting
			binary.BigEndian.PutUint64(val, uint64(n-i))
			buf.Put(key, val)
		}
		return buf
	}

	for _, tc := range []struct {
		name    string
		count   int
		sameKey bool
	}{
		{"mixed_keys_10k", 10_000, false},
		{"mixed_keys_100k", 100_000, false},
		{"same_key_10k", 10_000, true},
		{"same_key_100k", 100_000, true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				b.StopTimer()
				ref := makeBuffer(tc.count, tc.sameKey)
				b.StartTimer()
				ref.SortByKeyAndValue()
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
