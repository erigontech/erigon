// Copyright 2020 The turbo-geth Authors
// This file is part of the turbo-geth library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package stages

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

// SyncStage represents the stages of syncronisation in the SyncMode.StagedSync mode
type SyncStage byte

const (
	Headers             SyncStage = iota // Headers are downloaded, their Proof-Of-Work validity and chaining is verified
	Bodies                               // Block bodies are downloaded, TxHash and UncleHash are getting verified
	Senders                              // "From" recovered from signatures, bodies re-written
	Execution                            // Executing each block w/o buildinf a trie
	IntermediateHashes                   // Generate intermediate hashes, calculate the state root hash
	HashState                            // Apply Keccak256 to all the keys in the state
	AccountHistoryIndex                  // Generating history index for accounts
	StorageHistoryIndex                  // Generating history index for storage
	TxLookup                             // Generating transactions lookup index
	TxPool                               // Starts TxPool
	Finish                               // Nominal stage after all other stages
)

var DBKeys = map[SyncStage][]byte{
	Headers:             []byte("Headers"),
	Bodies:              []byte("Bodies"),
	Senders:             []byte("Senders"),
	Execution:           []byte("Execution"),
	IntermediateHashes:  []byte("IntermediateHashes"),
	HashState:           []byte("HashState"),
	AccountHistoryIndex: []byte("AccountHistoryIndex"),
	StorageHistoryIndex: []byte("StorageHistoryIndex"),
	TxLookup:            []byte("TxLookup"),
	TxPool:              []byte("TxPool"),
	Finish:              []byte("Finish"),
}

// GetStageProgress retrieves saved progress of given sync stage from the database
func GetStageProgress(db ethdb.Getter, stage SyncStage) (uint64, []byte, error) {
	v, err := db.Get(dbutils.SyncStageProgress, DBKeys[stage])
	if err != nil && !errors.Is(err, ethdb.ErrKeyNotFound) {
		return 0, nil, err
	}
	return unmarshalData(v)
}

// SaveStageProgress saves the progress of the given stage in the database
func SaveStageProgress(db ethdb.Putter, stage SyncStage, progress uint64, stageData []byte) error {
	return db.Put(dbutils.SyncStageProgress, DBKeys[stage], marshalData(progress, stageData))
}

// GetStageUnwind retrieves the invalidation for the given stage
// Invalidation means that that stage needs to rollback to the invalidation
// point and be redone
func GetStageUnwind(db ethdb.Getter, stage SyncStage) (uint64, []byte, error) {
	v, err := db.Get(dbutils.SyncStageUnwind, DBKeys[stage])
	if err != nil && !errors.Is(err, ethdb.ErrKeyNotFound) {
		return 0, nil, err
	}
	return unmarshalData(v)
}

// SaveStageUnwind saves the progress of the given stage in the database
func SaveStageUnwind(db ethdb.Putter, stage SyncStage, invalidation uint64, stageData []byte) error {
	return db.Put(dbutils.SyncStageUnwind, DBKeys[stage], marshalData(invalidation, stageData))
}

func marshalData(blockNumber uint64, stageData []byte) []byte {
	return append(encodeBigEndian(blockNumber), stageData...)
}

func unmarshalData(data []byte) (uint64, []byte, error) {
	if len(data) == 0 {
		return 0, nil, nil
	}
	if len(data) < 8 {
		return 0, nil, fmt.Errorf("value must be at least 8 bytes, got %d", len(data))
	}
	return binary.BigEndian.Uint64(data[:8]), common.CopyBytes(data[8:]), nil
}

func encodeBigEndian(n uint64) []byte {
	var v [8]byte
	binary.BigEndian.PutUint64(v[:], n)
	return v[:]
}
