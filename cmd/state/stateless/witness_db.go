package stateless

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/trie"
)

var (
	witnessesBucket = []byte("witnesses")
	newTrieOp       = byte(0xBB)
)

type WitnessDB struct {
	putter ethdb.Putter
}

func NewWitnessDB(putter ethdb.Putter) (*WitnessDB, error) {
	return &WitnessDB{putter}, nil
}

func (db *WitnessDB) MustUpsert(blockNumber uint64, maxTrieSize uint32, resolveWitnesses []*trie.Witness) {
	key := deriveDbKey(blockNumber, maxTrieSize)

	var buf bytes.Buffer

	for i, witness := range resolveWitnesses {
		var stats *trie.BlockWitnessStats
		var err error
		if stats, err = witness.WriteTo(&buf); err != nil {
			panic(fmt.Errorf("error while writing witness to a buffer: %w", err))
		}
		fmt.Printf("\nwritten witness for block %v cacheSize %v -> %v bytes\n", blockNumber, maxTrieSize, stats.BlockWitnessSize())
		if i < len(resolveWitnesses)-1 {
			buf.WriteByte(newTrieOp)
		}
	}

	err := db.putter.Put(witnessesBucket, key, buf.Bytes())
	if err != nil {
		panic(fmt.Errorf("error while upserting witness: %w", err))
	}
}

func deriveDbKey(blockNumber uint64, maxTrieSize uint32) []byte {
	buffer := make([]byte, 8+4)

	binary.LittleEndian.PutUint64(buffer[:], blockNumber)
	binary.LittleEndian.PutUint32(buffer[8:], maxTrieSize)

	return buffer
}
