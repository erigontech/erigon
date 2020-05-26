package downloader

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"testing"

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
		if err := writeToDisk(buffer, []byte(keys[i]), []byte(vals[i])); err != nil {
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

func getDataDir() string {
	name, err := ioutil.TempDir("", "geth-tests-staged-sync")
	if err != nil {
		panic(err)
	}
	return name
}

func TestPromoteHashedStateClearState(t *testing.T) {
	db1 := ethdb.NewMemDatabase()
	db2 := ethdb.NewMemDatabase()

	generateBlocks(t, 1, 50, hashedWriterGen(db1), changeCodeWithIncarnations)

	generateBlocks(t, 1, 50, plainWriterGen(db2), changeCodeWithIncarnations)

	m2 := db2.NewBatch()
	err := promoteHashedState(m2, 0, getDataDir())
	if err != nil {
		t.Errorf("error while promoting state: %v", err)
	}
	_, err = m2.Commit()
	if err != nil {
		t.Errorf("error while commiting state: %v", err)
	}

	compareCurrentState(t, db1, db2, dbutils.CurrentStateBucket, dbutils.ContractCodeBucket)
}

func TestPromoteHashedStateIncremental(t *testing.T) {
	t.Skip("not implemented yet")
	db1 := ethdb.NewMemDatabase()
	db2 := ethdb.NewMemDatabase()

	generateBlocks(t, 1, 50, hashedWriterGen(db1), changeCodeWithIncarnations)
	generateBlocks(t, 1, 50, plainWriterGen(db2), changeCodeWithIncarnations)

	m2 := db2.NewBatch()
	err := promoteHashedState(m2, 0, getDataDir())
	if err != nil {
		t.Errorf("error while promoting state: %v", err)
	}
	_, err = m2.Commit()
	if err != nil {
		t.Errorf("error while commiting state: %v", err)
	}

	generateBlocks(t, 51, 50, hashedWriterGen(db1), changeCodeWithIncarnations)
	generateBlocks(t, 51, 50, plainWriterGen(db2), changeCodeWithIncarnations)

	m2 = db2.NewBatch()
	err = promoteHashedState(m2, 50, getDataDir())
	if err != nil {
		t.Errorf("error while promoting state: %v", err)
	}
	_, err = m2.Commit()
	if err != nil {
		t.Errorf("error while commiting state: %v", err)
	}

	compareCurrentState(t, db1, db2, dbutils.CurrentStateBucket, dbutils.ContractCodeBucket)
}

func TestPromoteHashedStateIncrementalMixed(t *testing.T) {
	t.Skip("not implemented yet")
	db1 := ethdb.NewMemDatabase()
	db2 := ethdb.NewMemDatabase()

	generateBlocks(t, 1, 100, hashedWriterGen(db1), changeCodeWithIncarnations)
	generateBlocks(t, 1, 50, hashedWriterGen(db1), changeCodeWithIncarnations)
	generateBlocks(t, 51, 50, plainWriterGen(db2), changeCodeWithIncarnations)

	m2 := db2.NewBatch()
	err := promoteHashedState(m2, 50, getDataDir())
	if err != nil {
		t.Errorf("error while promoting state: %v", err)
	}

	_, err = m2.Commit()
	if err != nil {
		t.Errorf("error while commiting state: %v", err)
	}

	compareCurrentState(t, db1, db2, dbutils.CurrentStateBucket, dbutils.ContractCodeBucket)
}
