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
	"fmt"

	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

// SyncStage represents the stages of syncronisation in the SyncMode.StagedSync mode
// It is used to persist the information about the stage state into the database.
// It should not be empty and should be unique.
type SyncStage []byte

var (
	Headers             SyncStage = []byte("Headers")             // Headers are downloaded, their Proof-Of-Work validity and chaining is verified
	BlockHashes         SyncStage = []byte("BlockHashes")         // Headers Number are written, fills blockHash => number bucket
	Bodies              SyncStage = []byte("Bodies")              // Block bodies are downloaded, TxHash and UncleHash are getting verified
	Senders             SyncStage = []byte("Senders")             // "From" recovered from signatures, bodies re-written
	Execution           SyncStage = []byte("Execution")           // Executing each block w/o buildinf a trie
	IntermediateHashes  SyncStage = []byte("IntermediateHashes")  // Generate intermediate hashes, calculate the state root hash
	HashState           SyncStage = []byte("HashState")           // Apply Keccak256 to all the keys in the state
	AccountHistoryIndex SyncStage = []byte("AccountHistoryIndex") // Generating history index for accounts
	StorageHistoryIndex SyncStage = []byte("StorageHistoryIndex") // Generating history index for storage
	LogIndex            SyncStage = []byte("LogIndex")            // Generating logs index (from receipts)
	CallTraces          SyncStage = []byte("CallTraces")          // Generating call traces index
	TxLookup            SyncStage = []byte("TxLookup")            // Generating transactions lookup index
	TxPool              SyncStage = []byte("TxPool")              // Starts Backend
	Finish              SyncStage = []byte("Finish")              // Nominal stage after all other stages

	MiningCreateBlock SyncStage = []byte("MiningCreateBlock")
	MiningExecution   SyncStage = []byte("MiningExecution")
	MiningFinish      SyncStage = []byte("MiningFinish")
)

var AllStages = []SyncStage{
	Headers,
	BlockHashes,
	Bodies,
	Senders,
	Execution,
	IntermediateHashes,
	HashState,
	AccountHistoryIndex,
	StorageHistoryIndex,
	LogIndex,
	CallTraces,
	TxLookup,
	TxPool,
	Finish,
}

// GetStageProgress retrieves saved progress of given sync stage from the database
func GetStageProgress(db ethdb.KVGetter, stage SyncStage) (uint64, error) {
	v, err := db.GetOne(dbutils.SyncStageProgress, stage)
	if err != nil {
		return 0, err
	}
	return unmarshalData(v)
}

func SaveStageProgress(db ethdb.Putter, stage SyncStage, progress uint64) error {
	return db.Put(dbutils.SyncStageProgress, stage, marshalData(progress))
}

// GetStageUnwind retrieves the invalidation for the given stage
// Invalidation means that that stage needs to rollback to the invalidation
// point and be redone
func GetStageUnwind(db ethdb.KVGetter, stage SyncStage) (uint64, error) {
	v, err := db.GetOne(dbutils.SyncStageUnwind, stage)
	if err != nil {
		return 0, err
	}
	return unmarshalData(v)
}

// SaveStageUnwind saves the progress of the given stage in the database
func SaveStageUnwind(db ethdb.Putter, stage SyncStage, invalidation uint64) error {
	return db.Put(dbutils.SyncStageUnwind, []byte(stage), marshalData(invalidation))
}

func marshalData(blockNumber uint64) []byte {
	return encodeBigEndian(blockNumber)
}

func unmarshalData(data []byte) (uint64, error) {
	if len(data) == 0 {
		return 0, nil
	}
	if len(data) < 8 {
		return 0, fmt.Errorf("value must be at least 8 bytes, got %d", len(data))
	}
	return binary.BigEndian.Uint64(data[:8]), nil
}

func encodeBigEndian(n uint64) []byte {
	var v [8]byte
	binary.BigEndian.PutUint64(v[:], n)
	return v[:]
}
