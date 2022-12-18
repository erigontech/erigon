// Copyright 2020 The Erigon Authors
// This file is part of the Erigon library.
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

	"github.com/ledgerwatch/erigon-lib/kv"
)

// SyncStage represents the stages of syncronisation in the Mode.StagedSync mode
// It is used to persist the information about the stage state into the database.
// It should not be empty and should be unique.
type SyncStage string

var (
	Snapshots           SyncStage = "Snapshots"       // Snapshots
	Headers             SyncStage = "Headers"         // Headers are downloaded, their Proof-Of-Work validity and chaining is verified
	CumulativeIndex     SyncStage = "CumulativeIndex" // Calculate how much gas has been used up to each block.
	BlockHashes         SyncStage = "BlockHashes"     // Headers Number are written, fills blockHash => number bucket
	Bodies              SyncStage = "Bodies"          // Block bodies are downloaded, TxHash and UncleHash are getting verified
	Senders             SyncStage = "Senders"         // "From" recovered from signatures, bodies re-written
	Execution           SyncStage = "Execution"       // Executing each block w/o buildinf a trie
	Translation         SyncStage = "Translation"     // Translation each marked for translation contract (from EVM to TEVM)
	VerkleTrie          SyncStage = "VerkleTrie"
	IntermediateHashes  SyncStage = "IntermediateHashes"  // Generate intermediate hashes, calculate the state root hash
	HashState           SyncStage = "HashState"           // Apply Keccak256 to all the keys in the state
	AccountHistoryIndex SyncStage = "AccountHistoryIndex" // Generating history index for accounts
	StorageHistoryIndex SyncStage = "StorageHistoryIndex" // Generating history index for storage
	LogIndex            SyncStage = "LogIndex"            // Generating logs index (from receipts)
	CallTraces          SyncStage = "CallTraces"          // Generating call traces index
	TxLookup            SyncStage = "TxLookup"            // Generating transactions lookup index
	Issuance            SyncStage = "WatchTheBurn"        // Compute ether issuance for each block
	Finish              SyncStage = "Finish"              // Nominal stage after all other stages

	MiningCreateBlock SyncStage = "MiningCreateBlock"
	MiningExecution   SyncStage = "MiningExecution"
	MiningFinish      SyncStage = "MiningFinish"
	// Beacon chain stages
	BeaconHistoryReconstruction SyncStage = "BeaconHistoryReconstruction" // BeaconHistoryReconstruction reconstruct missing history.
	BeaconBlocks                SyncStage = "BeaconBlocks"                // BeaconBlocks are downloaded, no verification
	BeaconState                 SyncStage = "BeaconState"                 // Beacon blocks are sent to the state transition function

)

var AllStages = []SyncStage{
	Snapshots,
	Headers,
	BlockHashes,
	Bodies,
	Senders,
	Execution,
	Translation,
	HashState,
	IntermediateHashes,
	AccountHistoryIndex,
	StorageHistoryIndex,
	LogIndex,
	CallTraces,
	TxLookup,
	Finish,
}

// GetStageProgress retrieves saved progress of given sync stage from the database
func GetStageProgress(db kv.Getter, stage SyncStage) (uint64, error) {
	v, err := db.GetOne(kv.SyncStageProgress, []byte(stage))
	if err != nil {
		return 0, err
	}
	return unmarshalData(v)
}

func SaveStageProgress(db kv.Putter, stage SyncStage, progress uint64) error {
	return db.Put(kv.SyncStageProgress, []byte(stage), marshalData(progress))
}

// GetStagePruneProgress retrieves saved progress of given sync stage from the database
func GetStagePruneProgress(db kv.Getter, stage SyncStage) (uint64, error) {
	v, err := db.GetOne(kv.SyncStageProgress, []byte("prune_"+stage))
	if err != nil {
		return 0, err
	}
	return unmarshalData(v)
}

func SaveStagePruneProgress(db kv.Putter, stage SyncStage, progress uint64) error {
	return db.Put(kv.SyncStageProgress, []byte("prune_"+stage), marshalData(progress))
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
