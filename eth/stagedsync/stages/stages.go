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
type SyncStage byte

const (
	Headers             SyncStage = iota // Headers are downloaded, their Proof-Of-Work validity and chaining is verified
	Bodies                               // Block bodies are downloaded, TxHash and UncleHash are getting verified
	Senders                              // "From" recovered from signatures, bodies re-written
	Execution                            // Executing each block w/o buildinf a trie
	HashState                            // Apply Keccak256 to all the keys in the state
	IntermediateHashes                   // Generate intermediate hashes
	AccountHistoryIndex                  // Generating history index for accounts
	StorageHistoryIndex                  // Generating history index for storage
	Finish                               // Nominal stage after all other stages
)

// GetStageProgressretrieves saved progress of given sync stage from the database
func GetStageProgress(db ethdb.Getter, stage SyncStage) (uint64, error) {
	v, err := db.Get(dbutils.SyncStageProgress, []byte{byte(stage)})
	if err != nil && err != ethdb.ErrKeyNotFound {
		return 0, err
	}
	if len(v) == 0 {
		return 0, nil
	}
	if len(v) != 8 {
		return 0, fmt.Errorf("stage progress value must be of length 8, got %d", len(v))
	}
	return binary.BigEndian.Uint64(v), nil
}

// SaveStageProgress saves the progress of the given stage in the database
func SaveStageProgress(db ethdb.Putter, stage SyncStage, progress uint64) error {
	return db.Put(dbutils.SyncStageProgress, []byte{byte(stage)}, encodeBigEndian(progress))
}

// GetStageUnwind retrieves the invalidation for the given stage
// Invalidation means that that stage needs to rollback to the invalidation
// point and be redone
func GetStageUnwind(db ethdb.Getter, stage SyncStage) (uint64, error) {
	v, err := db.Get(dbutils.SyncStageUnwind, []byte{byte(stage)})
	if err != nil && err != ethdb.ErrKeyNotFound {
		return 0, err
	}
	if len(v) == 0 {
		return 0, nil
	}
	if len(v) != 8 {
		return 0, fmt.Errorf("stage invalidation value must be of length 8, got %d", len(v))
	}
	return binary.BigEndian.Uint64(v), nil
}

// SaveStageUnwind saves the progress of the given stage in the database
func SaveStageUnwind(db ethdb.Putter, stage SyncStage, invalidation uint64) error {
	return db.Put(dbutils.SyncStageUnwind, []byte{byte(stage)}, encodeBigEndian(invalidation))
}

func encodeBigEndian(n uint64) []byte {
	var v [8]byte
	binary.BigEndian.PutUint64(v[:], n)
	return v[:]
}
