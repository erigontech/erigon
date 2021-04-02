package etl

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/stretchr/testify/assert"
	"github.com/ugorji/go/codec"
)

func TestWriteAndReadBufferEntry(t *testing.T) {
	buffer := bytes.NewBuffer(make([]byte, 0))
	encoder := codec.NewEncoder(buffer, &cbor)

	keys := make([]string, 100)
	vals := make([]string, 100)

	for i := range keys {
		keys[i] = fmt.Sprintf("key-%d", i)
		vals[i] = fmt.Sprintf("value-%d", i)
	}

	for i := range keys {
		if err := writeToDisk(encoder, []byte(keys[i]), []byte(vals[i])); err != nil {
			t.Error(err)
		}
	}

	bb := buffer.Bytes()

	readBuffer := bytes.NewReader(bb)

	decoder := codec.NewDecoder(readBuffer, &cbor)

	for i := range keys {
		k, v, err := readElementFromDisk(decoder)
		if err != nil {
			t.Error(err)
		}
		assert.Equal(t, keys[i], string(k))
		assert.Equal(t, vals[i], string(v))
	}

	_, _, err := readElementFromDisk(decoder)
	assert.Equal(t, io.EOF, err)
}

func TestNextKey(t *testing.T) {
	for _, tc := range []string{
		"00000001->00000002",
		"000000FF->00000100",
		"FEFFFFFF->FF000000",
	} {
		parts := strings.Split(tc, "->")
		input := common.Hex2Bytes(parts[0])
		expectedOutput := common.Hex2Bytes(parts[1])
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
		input := common.Hex2Bytes(tc)
		_, err := NextKey(input)
		assert.Error(t, err)
	}
}

func TestFileDataProviders(t *testing.T) {
	// test invariant when we go through files (> 1 buffer)
	db := ethdb.NewMemDatabase()
	defer db.Close()
	tx, err := db.Begin(context.Background(), ethdb.RW)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	sourceBucket := dbutils.Buckets[0]

	generateTestData(t, tx, sourceBucket, 10)

	collector := NewCollector("", NewSortableBuffer(1))

	err = extractBucketIntoFiles("logPrefix", tx.(ethdb.HasTx).Tx().(ethdb.RwTx), sourceBucket, nil, nil, 0, collector, testExtractToMapFunc, nil, nil)
	assert.NoError(t, err)

	assert.Equal(t, 10, len(collector.dataProviders))

	for _, p := range collector.dataProviders {
		fp, ok := p.(*fileDataProvider)
		assert.True(t, ok)
		_, err = os.Stat(fp.file.Name())
		assert.NoError(t, err)
	}

	disposeProviders("logPrefix", collector.dataProviders)

	for _, p := range collector.dataProviders {
		fp, ok := p.(*fileDataProvider)
		assert.True(t, ok)
		_, err = os.Stat(fp.file.Name())
		assert.True(t, os.IsNotExist(err))
	}
}

func TestRAMDataProviders(t *testing.T) {
	// test invariant when we go through memory (1 buffer)
	db := ethdb.NewMemDatabase()
	defer db.Close()
	tx, err := db.Begin(context.Background(), ethdb.RW)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	sourceBucket := dbutils.Buckets[0]
	generateTestData(t, tx, sourceBucket, 10)

	collector := NewCollector("", NewSortableBuffer(BufferOptimalSize))
	err = extractBucketIntoFiles("logPrefix", tx.(ethdb.HasTx).Tx().(ethdb.RwTx), sourceBucket, nil, nil, 0, collector, testExtractToMapFunc, nil, nil)
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
	db := ethdb.NewMemDatabase()
	defer db.Close()
	tx, err := db.Begin(context.Background(), ethdb.RW)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	sourceBucket := dbutils.Buckets[0]
	destBucket := dbutils.Buckets[1]
	generateTestData(t, tx, sourceBucket, 20)
	err = Transform(
		"logPrefix",
		tx.(ethdb.HasTx).Tx().(ethdb.RwTx),
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

func TestTransformOnLoadCommitCustomBatchSize(t *testing.T) {
	// test invariant when we only have one buffer and it fits into RAM (exactly 1 buffer)
	db := ethdb.NewMemDatabase()
	defer db.Close()
	tx, err := db.Begin(context.Background(), ethdb.RW)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	sourceBucket := dbutils.Buckets[0]
	destBucket := dbutils.Buckets[1]
	generateTestData(t, tx, sourceBucket, 20)

	numberOfCalls := 0
	finalized := false

	err = Transform(
		"logPrefix",
		tx.(ethdb.HasTx).Tx().(ethdb.RwTx),
		sourceBucket,
		destBucket,
		"", // temp dir
		testExtractToMapFunc,
		testLoadFromMapFunc,
		TransformArgs{
			OnLoadCommit: func(_ ethdb.Putter, _ []byte, isDone bool) error {
				numberOfCalls++
				if isDone {
					finalized = true
				}
				return nil
			},
			loadBatchSize: 1,
		},
	)
	assert.Nil(t, err)
	compareBuckets(t, tx, sourceBucket, destBucket, nil)

	assert.Equal(t, 1, numberOfCalls)
	assert.True(t, finalized)
}

func TestTransformOnLoadCommitDefaultBatchSize(t *testing.T) {
	// test invariant when we only have one buffer and it fits into RAM (exactly 1 buffer)
	db := ethdb.NewMemDatabase()
	defer db.Close()
	tx, err := db.Begin(context.Background(), ethdb.RW)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	sourceBucket := dbutils.Buckets[0]
	destBucket := dbutils.Buckets[1]
	generateTestData(t, tx, sourceBucket, 20)

	numberOfCalls := 0
	finalized := false

	err = Transform(
		"logPrefix",
		tx.(ethdb.HasTx).Tx().(ethdb.RwTx),
		sourceBucket,
		destBucket,
		"", // temp dir
		testExtractToMapFunc,
		testLoadFromMapFunc,
		TransformArgs{
			OnLoadCommit: func(_ ethdb.Putter, _ []byte, isDone bool) error {
				numberOfCalls++
				if isDone {
					finalized = true
				}
				return nil
			},
		},
	)
	assert.Nil(t, err)
	compareBuckets(t, tx, sourceBucket, destBucket, nil)

	assert.Equal(t, 1, numberOfCalls)
	assert.True(t, finalized)
}

func TestEmptySourceBucket(t *testing.T) {
	db := ethdb.NewMemDatabase()
	defer db.Close()
	tx, err := db.Begin(context.Background(), ethdb.RW)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	sourceBucket := dbutils.Buckets[0]
	destBucket := dbutils.Buckets[1]
	err = Transform(
		"logPrefix",
		tx.(ethdb.HasTx).Tx().(ethdb.RwTx),
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
	db := ethdb.NewMemDatabase()
	defer db.Close()
	tx, err := db.Begin(context.Background(), ethdb.RW)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	sourceBucket := dbutils.Buckets[0]
	destBucket := dbutils.Buckets[1]
	generateTestData(t, tx, sourceBucket, 10)
	err = Transform(
		"logPrefix",
		tx.(ethdb.HasTx).Tx().(ethdb.RwTx),
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
	db := ethdb.NewMemDatabase()
	defer db.Close()
	tx, err := db.Begin(context.Background(), ethdb.RW)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	sourceBucket := dbutils.Buckets[0]
	destBucket := dbutils.Buckets[1]
	generateTestData(t, tx, sourceBucket, 10)
	err = Transform(
		"logPrefix",
		tx.(ethdb.HasTx).Tx().(ethdb.RwTx),
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
	db := ethdb.NewMemDatabase()
	defer db.Close()
	tx, err := db.Begin(context.Background(), ethdb.RW)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	sourceBucket := dbutils.Buckets[0]
	destBucket := dbutils.Buckets[1]
	generateTestData(t, tx, sourceBucket, 10)
	err = Transform(
		"logPrefix",
		tx.(ethdb.HasTx).Tx().(ethdb.RwTx),
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
	db := ethdb.NewMemDatabase()
	defer db.Close()
	tx, err := db.Begin(context.Background(), ethdb.RW)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	sourceBucket := dbutils.Buckets[0]
	destBucket := dbutils.Buckets[1]
	generateTestData(t, tx, sourceBucket, 10)
	err = Transform(
		"logPrefix",
		tx.(ethdb.HasTx).Tx().(ethdb.RwTx),
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

func generateTestData(t *testing.T, db ethdb.Putter, bucket string, count int) {
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

func compareBuckets(t *testing.T, db ethdb.Database, b1, b2 string, startKey []byte) {
	t.Helper()
	b1Map := make(map[string]string)
	err := db.Walk(b1, startKey, len(startKey), func(k, v []byte) (bool, error) {
		b1Map[fmt.Sprintf("%x", k)] = fmt.Sprintf("%x", v)
		return true, nil
	})
	assert.NoError(t, err)
	b2Map := make(map[string]string)
	err = db.Walk(b2, nil, 0, func(k, v []byte) (bool, error) {
		b2Map[fmt.Sprintf("%x", k)] = fmt.Sprintf("%x", v)
		return true, nil
	})
	assert.NoError(t, err)
	assert.Equal(t, b1Map, b2Map)
}

func compareBucketsDouble(t *testing.T, db ethdb.Database, b1, b2 string) {
	t.Helper()
	b1Map := make(map[string]string)
	err := db.Walk(b1, nil, 0, func(k, v []byte) (bool, error) {
		b1Map[fmt.Sprintf("%x", append(k, 0xAA))] = fmt.Sprintf("%x", append(v, 0xAA))
		b1Map[fmt.Sprintf("%x", append(k, 0xBB))] = fmt.Sprintf("%x", append(v, 0xBB))
		return true, nil
	})
	assert.NoError(t, err)
	b2Map := make(map[string]string)
	err = db.Walk(b2, nil, 0, func(k, v []byte) (bool, error) {
		b2Map[fmt.Sprintf("%x", k)] = fmt.Sprintf("%x", v)
		return true, nil
	})
	assert.NoError(t, err)
	assert.Equal(t, b1Map, b2Map)
}
