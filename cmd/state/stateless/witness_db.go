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
	putter      ethdb.Putter
	statsWriter *csv.Writer
}

func NewWitnessDBWriter(putter ethdb.Putter, statsWriter *csv.Writer) (*WitnessDBWriter, error) {
	err := statsWriter.Write([]string{
		"blockNum", "maxTrieSize", "witnessesSize",
	})
	if err != nil {
		return nil, err
	}
	return &WitnessDBWriter{putter, statsWriter}, nil
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
	err := db.putter.Put(witnessesBucket, key, bytes)

	if err != nil {
		panic(fmt.Errorf("error while upserting witness: %w", err))
	}

	err = db.statsWriter.Write([]string{
		fmt.Sprintf("%v", blockNumber),
		fmt.Sprintf("%v", maxTrieSize),
		fmt.Sprintf("%v", len(bytes)),
	})

	if err != nil {
		panic(fmt.Errorf("error while writing stats: %w", err))
	}
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
