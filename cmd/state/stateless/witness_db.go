package stateless

import (
	"bytes"
	"encoding/binary"
	"encoding/csv"
	"fmt"

	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/trie"
)

var (
	witnessesBucket = []byte("witnesses")
)

type WitnessDBWriter struct {
	storage     ethdb.Database
	statsWriter *csv.Writer
	batch       ethdb.DbWithPendingMutations
}

func NewWitnessDBWriter(storage ethdb.Database, statsWriter *csv.Writer) (*WitnessDBWriter, error) {
	err := statsWriter.Write([]string{
		"blockNum", "maxTrieSize", "witnessesSize",
	})
	if err != nil {
		return nil, err
	}
	return &WitnessDBWriter{storage, statsWriter, nil}, nil
}

func (db *WitnessDBWriter) MustUpsert(blockNumber uint64, maxTrieSize uint32, resolveWitnesses []*trie.Witness) {
	key := deriveDbKey(blockNumber, maxTrieSize)

	var buf bytes.Buffer

	for i, witness := range resolveWitnesses {
		if _, err := witness.WriteTo(&buf); err != nil {
			panic(fmt.Errorf("error while writing witness to a buffer: %w", err))
		}
		if i < len(resolveWitnesses)-1 {
			buf.WriteByte(byte(trie.OpNewTrie))
		}
	}

	bytes := buf.Bytes()

	if db.batch == nil {
		db.batch = db.storage.NewBatch()
	}

	err := db.batch.Put(witnessesBucket, key, bytes)

	if err != nil {
		panic(fmt.Errorf("error while upserting witness: %w", err))
	}

	_, err = db.batch.Commit()
	if err != nil {
		panic(err)
	}
	db.batch = nil

	err = db.statsWriter.Write([]string{
		fmt.Sprintf("%v", blockNumber),
		fmt.Sprintf("%v", maxTrieSize),
		fmt.Sprintf("%v", len(bytes)),
	})

	if err != nil {
		panic(fmt.Errorf("error while writing stats: %w", err))
	}

	db.statsWriter.Flush()
}

func (db *WitnessDBWriter) Flush() error {
	if db.batch == nil {
		return nil
	}

	_, err := db.batch.Commit()
	db.batch = nil
	return err
}

type WitnessDBReader struct {
	getter ethdb.Getter
}

func NewWitnessDBReader(getter ethdb.Getter) *WitnessDBReader {
	return &WitnessDBReader{getter}
}

func (db *WitnessDBReader) GetWitnessesForBlock(blockNumber uint64, maxTrieSize uint32) ([]byte, error) {
	key := deriveDbKey(blockNumber, maxTrieSize)
	return db.getter.Get(witnessesBucket, key)
}

func deriveDbKey(blockNumber uint64, maxTrieSize uint32) []byte {
	buffer := make([]byte, 8+4)

	binary.LittleEndian.PutUint64(buffer[:], blockNumber)
	binary.LittleEndian.PutUint32(buffer[8:], maxTrieSize)

	return buffer
}
