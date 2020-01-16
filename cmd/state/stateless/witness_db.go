package stateless

import (
	"encoding/binary"
	"fmt"

	"github.com/ledgerwatch/turbo-geth/ethdb"
)

var witnessesBucket []byte = []byte("witnesses")

type WitnessDB struct {
	putter ethdb.Putter
}

func NewWitnessDB(putter ethdb.Putter) (*WitnessDB, error) {
	return &WitnessDB{putter}, nil
}

func (db *WitnessDB) MustUpsert(blockNumber uint64, maxTrieSize uint32, marshalledWitness []byte) {
	key := deriveDbKey(blockNumber, maxTrieSize)

	err := db.putter.Put(witnessesBucket, key, marshalledWitness)
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
