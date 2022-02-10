/*
   Copyright 2021 Erigon contributors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/
package etl

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/stretchr/testify/assert"
	"github.com/ugorji/go/codec"
)

func decodeHex(in string) []byte {
	payload, err := hex.DecodeString(in)
	if err != nil {
		panic(err)
	}
	return payload
}

func TestWriteAndReadBufferEntry(t *testing.T) {
	buffer := bytes.NewBuffer(make([]byte, 0))
	encoder := codec.NewEncoder(buffer, &cbor)

	entries := make([]sortableBufferEntry, 100)
	for i := range entries {
		entries[i].key = []byte(fmt.Sprintf("key-%d", i))
		entries[i].value = []byte(fmt.Sprintf("value-%d", i))
	}

	if err := writeToDisk(encoder, entries); err != nil {
		t.Error(err)
	}

	bb := buffer.Bytes()

	readBuffer := bytes.NewReader(bb)

	decoder := codec.NewDecoder(readBuffer, &cbor)

	buf := make([][]byte, 2)
	for i := range entries {
		k, v, err := readElementFromDisk(buf, decoder)
		if err != nil {
			t.Error(err)
		}
		assert.Equal(t, string(entries[i].key), string(k))
		assert.Equal(t, string(entries[i].value), string(v))
	}

	_, _, err := readElementFromDisk(buf, decoder)
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
		assert.NoError(t, err)
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
	// test invariant when we go through files (> 1 buffer)
	_, tx := memdb.NewTestTx(t)
	sourceBucket := kv.ChaindataTables[0]

	generateTestData(t, tx, sourceBucket, 10)

	collector := NewCollector(t.Name(), "", NewSortableBuffer(1))

	err := extractBucketIntoFiles("logPrefix", tx, sourceBucket, nil, nil, collector, testExtractToMapFunc, nil, nil)
	assert.NoError(t, err)

	assert.Equal(t, 10, len(collector.dataProviders))

	for _, p := range collector.dataProviders {
		fp, ok := p.(*fileDataProvider)
		assert.True(t, ok)
		_, err = os.Stat(fp.file.Name())
		assert.NoError(t, err)
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
	// test invariant when we go through memory (1 buffer)
	_, tx := memdb.NewTestTx(t)
	sourceBucket := kv.ChaindataTables[0]
	generateTestData(t, tx, sourceBucket, 10)

	collector := NewCollector(t.Name(), "", NewSortableBuffer(BufferOptimalSize))
	err := extractBucketIntoFiles("logPrefix", tx, sourceBucket, nil, nil, collector, testExtractToMapFunc, nil, nil)
	assert.NoError(t, err)

	assert.Equal(t, 1, len(collector.dataProviders))

	for _, p := range collector.dataProviders {
		mp, ok := p.(*memoryDataProvider)
		assert.True(t, ok)
		assert.Equal(t, 10, mp.buffer.Len())
	}
}

func TestTransformRAMOnly(t *testing.T) {
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
	)
	assert.Nil(t, err)
	compareBuckets(t, tx, sourceBucket, destBucket, nil)
}

func TestEmptySourceBucket(t *testing.T) {
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
	)
	assert.Nil(t, err)
	compareBuckets(t, tx, sourceBucket, destBucket, nil)
}

func TestTransformExtractStartKey(t *testing.T) {
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
	)
	assert.Nil(t, err)
	compareBuckets(t, tx, sourceBucket, destBucket, []byte(fmt.Sprintf("%10d-key-%010d", 5, 5)))
}

func TestTransformThroughFiles(t *testing.T) {
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
	)
	assert.Nil(t, err)
	compareBuckets(t, tx, sourceBucket, destBucket, nil)
}

func TestTransformDoubleOnExtract(t *testing.T) {
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
	)
	assert.Nil(t, err)
	compareBucketsDouble(t, tx, sourceBucket, destBucket)
}

func TestTransformDoubleOnLoad(t *testing.T) {
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
	)
	assert.Nil(t, err)
	compareBucketsDouble(t, tx, sourceBucket, destBucket)
}

func generateTestData(t *testing.T, db kv.Putter, bucket string, count int) {
	for i := 0; i < count; i++ {
		k := []byte(fmt.Sprintf("%10d-key-%010d", i, i))
		v := []byte(fmt.Sprintf("val-%099d", i))
		err := db.Put(bucket, k, v)
		assert.NoError(t, err)
	}
}

func testExtractToMapFunc(k, v []byte, next ExtractNextFunc) error {
	buf := bytes.NewBuffer(nil)
	encoder := codec.NewEncoder(nil, &cbor)

	valueMap := make(map[string][]byte)
	valueMap["value"] = v
	encoder.Reset(buf)
	encoder.MustEncode(valueMap)
	return next(k, k, buf.Bytes())
}

func testExtractDoubleToMapFunc(k, v []byte, next ExtractNextFunc) error {
	buf := bytes.NewBuffer(nil)
	encoder := codec.NewEncoder(nil, &cbor)

	valueMap := make(map[string][]byte)
	valueMap["value"] = append(v, 0xAA)
	k1 := append(k, 0xAA)
	encoder.Reset(buf)
	encoder.MustEncode(valueMap)

	err := next(k, k1, buf.Bytes())
	if err != nil {
		return err
	}

	valueMap = make(map[string][]byte)
	valueMap["value"] = append(v, 0xBB)
	k2 := append(k, 0xBB)
	buf.Reset()
	encoder.Reset(buf)
	encoder.MustEncode(valueMap)
	return next(k, k2, buf.Bytes())
}

func testLoadFromMapFunc(k []byte, v []byte, _ CurrentTableReader, next LoadNextFunc) error {
	decoder := codec.NewDecoder(nil, &cbor)
	decoder.ResetBytes(v)
	valueMap := make(map[string][]byte)
	err := decoder.Decode(&valueMap)
	if err != nil {
		return err
	}
	realValue := valueMap["value"]
	return next(k, k, realValue)
}

func testLoadFromMapDoubleFunc(k []byte, v []byte, _ CurrentTableReader, next LoadNextFunc) error {
	decoder := codec.NewDecoder(nil, &cbor)
	decoder.ResetBytes(v)

	valueMap := make(map[string][]byte)
	err := decoder.Decode(valueMap)
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
	assert.NoError(t, err)
	b2Map := make(map[string]string)
	err = db.ForEach(b2, nil, func(k, v []byte) error {
		b2Map[fmt.Sprintf("%x", k)] = fmt.Sprintf("%x", v)
		return nil
	})
	assert.NoError(t, err)
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
	assert.NoError(t, err)
	b2Map := make(map[string]string)
	err = db.ForEach(b2, nil, func(k, v []byte) error {
		b2Map[fmt.Sprintf("%x", k)] = fmt.Sprintf("%x", v)
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, b1Map, b2Map)
}
