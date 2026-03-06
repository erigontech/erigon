// Copyright 2017 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package stages

import (
	"encoding/binary"
	"fmt"

	"github.com/erigontech/erigon/db/kv"
)

// SyncStage represents the stages of synchronization in the Mode.StagedSync mode
// It is used to persist the information about the stage state into the database.
// It should not be empty and should be unique.
type SyncStage string

var (
	Snapshots SyncStage = "OtterSync" // Snapshots
	Headers   SyncStage = "Headers"   // Headers are downloaded, their Proof-Of-Work validity and chaining is verified

	BlockHashes       SyncStage = "BlockHashes"       // Headers Number are written, fills blockHash => number bucket
	Bodies            SyncStage = "Bodies"            // Block bodies are downloaded, TxHash and UncleHash are getting verified
	Senders           SyncStage = "Senders"           // "From" recovered from signatures, bodies re-written
	Execution         SyncStage = "Execution"         // Executing each block w/o building a trie
	CustomTrace       SyncStage = "CustomTrace"       // Executing each block w/o building a trie
	WitnessProcessing SyncStage = "WitnessProcessing" // Process buffered witness data for Polygon chains
	TxLookup          SyncStage = "TxLookup"          // Generating transactions lookup index
	Finish            SyncStage = "Finish"            // Nominal stage after all other stages
)

var AllStages = []SyncStage{
	Snapshots,
	Headers,
	BlockHashes,
	Bodies,
	Senders,
	Execution,
	CustomTrace,
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

// GetStageProgress retrieves saved progress of given sync stage from the database
func GetStageProgressIfAllEqual(db kv.Getter, stage ...SyncStage) (progress uint64, equal bool, err error) {
	if len(stage) == 0 {
		panic("GetStageProgressIfAllEqual should be called with at least one stage")
	}
	currentProgress := int64(-1)
	for _, s := range stage {
		v, err := db.GetOne(kv.SyncStageProgress, []byte(s))
		if err != nil {
			return 0, false, err
		}
		progressU64, err := unmarshalData(v)
		if err != nil {
			return 0, false, err
		}
		if currentProgress == -1 {
			currentProgress = int64(progressU64)
			continue
		}
		if currentProgress != int64(progressU64) {
			return 0, false, nil
		}
	}

	return uint64(currentProgress), true, nil
}

func SaveStageProgress(db kv.Putter, stage SyncStage, progress uint64) error {
	if m, ok := SyncMetrics[stage]; ok {
		m.SetUint64(progress)
	}
	return db.Put(kv.SyncStageProgress, []byte(stage), encodeBigEndian(progress))
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
	return db.Put(kv.SyncStageProgress, []byte("prune_"+stage), encodeBigEndian(progress))
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
