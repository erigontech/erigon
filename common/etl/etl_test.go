package etl

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"testing"

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

func TestFileDataProviders(t *testing.T) {
	oldSize := bufferOptimalSize
	bufferOptimalSize = 1 // guarantee file per entry
	defer func() {
		bufferOptimalSize = oldSize
	}()

	// test invariant when we go through files (> 1 buffer)
	db := ethdb.NewMemDatabase()
	sourceBucket := []byte("source")
	generateTestData(t, db, sourceBucket, 10)

	collector := NewCollector("")

	err := extractBucketIntoFiles(db, sourceBucket, nil, collector, testExtractToMapFunc, nil)
	assert.NoError(t, err)

	assert.Equal(t, 10, len(collector.dataProviders))

	for _, p := range collector.dataProviders {
		fp, ok := p.(*fileDataProvider)
		assert.True(t, ok)
		_, err = os.Stat(fp.file.Name())
		assert.NoError(t, err)
	}

	disposeProviders(collector.dataProviders)

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
	sourceBucket := []byte("source")
	generateTestData(t, db, sourceBucket, 10)

	collector := NewCollector("")
	err := extractBucketIntoFiles(db, sourceBucket, nil, collector, testExtractToMapFunc, nil)
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
	sourceBucket := []byte("source")
	destBucket := []byte("dest")
	generateTestData(t, db, sourceBucket, 20)
	err := Transform(
		db,
		sourceBucket,
		destBucket,
		"", // temp dir
		nil,
		testExtractToMapFunc,
		testLoadFromMapFunc,
		nil,
	)
	assert.Nil(t, err)
	compareBuckets(t, db, sourceBucket, destBucket, nil)
}

func TestEmptySourceBucket(t *testing.T) {
	db := ethdb.NewMemDatabase()
	sourceBucket := []byte("source")
	destBucket := []byte("dest")
	err := Transform(
		db,
		sourceBucket,
		destBucket,
		"", // temp dir
		nil,
		testExtractToMapFunc,
		testLoadFromMapFunc,
		nil,
	)
	assert.Nil(t, err)
	compareBuckets(t, db, sourceBucket, destBucket, nil)
}

func TestTransformStartkey(t *testing.T) {
	// test invariant when we only have one buffer and it fits into RAM (exactly 1 buffer)
	db := ethdb.NewMemDatabase()
	sourceBucket := []byte("source")
	destBucket := []byte("dest")
	generateTestData(t, db, sourceBucket, 10)
	err := Transform(
		db,
		sourceBucket,
		destBucket,
		"", // temp dir
		[]byte(fmt.Sprintf("%10d-key-%010d", 5, 5)),
		testExtractToMapFunc,
		testLoadFromMapFunc,
		nil,
	)
	assert.Nil(t, err)
	compareBuckets(t, db, sourceBucket, destBucket, []byte(fmt.Sprintf("%10d-key-%010d", 5, 5)))
}

func TestTransformThroughFiles(t *testing.T) {
	oldSize := bufferOptimalSize
	bufferOptimalSize = 1 // guarantee file per entry
	defer func() {
		bufferOptimalSize = oldSize
	}()
	// test invariant when we go through files (> 1 buffer)
	db := ethdb.NewMemDatabase()
	sourceBucket := []byte("source")
	destBucket := []byte("dest")
	generateTestData(t, db, sourceBucket, 10)
	err := Transform(
		db,
		sourceBucket,
		destBucket,
		"", // temp dir
		nil,
		testExtractToMapFunc,
		testLoadFromMapFunc,
		nil,
	)
	assert.Nil(t, err)
	compareBuckets(t, db, sourceBucket, destBucket, nil)
}

func TestTransformDoubleOnExtract(t *testing.T) {
	// test invariant when extractFunc multiplies the data 2x
	db := ethdb.NewMemDatabase()
	sourceBucket := []byte("source")
	destBucket := []byte("dest")
	generateTestData(t, db, sourceBucket, 10)
	err := Transform(
		db,
		sourceBucket,
		destBucket,
		"", // temp dir
		nil,
		testExtractDoubleToMapFunc,
		testLoadFromMapFunc,
		nil,
	)
	assert.Nil(t, err)
	compareBucketsDouble(t, db, sourceBucket, destBucket)
}

func TestTransformDoubleOnLoad(t *testing.T) {
	// test invariant when loadFunc multiplies the data 2x
	db := ethdb.NewMemDatabase()
	sourceBucket := []byte("source")
	destBucket := []byte("dest")
	generateTestData(t, db, sourceBucket, 10)
	err := Transform(
		db,
		sourceBucket,
		destBucket,
		"", // temp dir
		nil,
		testExtractToMapFunc,
		testLoadFromMapDoubleFunc,
		nil,
	)
	assert.Nil(t, err)
	compareBucketsDouble(t, db, sourceBucket, destBucket)
}

func generateTestData(t *testing.T, db ethdb.Putter, bucket []byte, count int) {
	for i := 0; i < count; i++ {
		k := []byte(fmt.Sprintf("%10d-key-%010d", i, i))
		v := []byte(fmt.Sprintf("val-%099d", i))
		err := db.Put(bucket, k, v)
		assert.NoError(t, err)
	}
}

func testExtractToMapFunc(k, v []byte, next ExtractNextFunc) error {
	valueMap := make(map[string][]byte)
	valueMap["value"] = v
	return next(k, valueMap)
}

func testExtractDoubleToMapFunc(k, v []byte, next ExtractNextFunc) error {
	valueMap := make(map[string][]byte)
	valueMap["value"] = append(v, 0xAA)
	k1 := append(k, 0xAA)
	err := next(k1, valueMap)
	if err != nil {
		return err
	}

	valueMap = make(map[string][]byte)
	valueMap["value"] = append(v, 0xBB)
	k2 := append(k, 0xBB)
	return next(k2, valueMap)
}

func testLoadFromMapFunc(k []byte, decoder Decoder, _ State, next LoadNextFunc) error {
	valueMap := make(map[string][]byte)
	err := decoder.Decode(&valueMap)
	if err != nil {
		return err
	}
	realValue := valueMap["value"]
	return next(k, realValue)
}

func testLoadFromMapDoubleFunc(k []byte, decoder Decoder, _ State, next LoadNextFunc) error {
	valueMap := make(map[string][]byte)
	err := decoder.Decode(&valueMap)
	if err != nil {
		return err
	}
	realValue := valueMap["value"]

	err = next(append(k, 0xAA), append(realValue, 0xAA))
	if err != nil {
		return err
	}
	return next(append(k, 0xBB), append(realValue, 0xBB))
}

func compareBuckets(t *testing.T, db ethdb.Database, b1, b2 []byte, startKey []byte) {
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

func compareBucketsDouble(t *testing.T, db ethdb.Database, b1, b2 []byte) {
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
