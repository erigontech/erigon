// Copyright 2018 The go-ethereum Authors
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

package rawdb

import (
	"context"
	"encoding/json"

	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/types"
	"github.com/erigontech/erigon/eth/stagedsync/stages"
)

func WriteGenesisIfNotExist(db kv.RwTx, g *types.Genesis) error {
	has, err := db.Has(kv.ConfigTable, kv.GenesisKey)
	if err != nil {
		return err
	}
	if has {
		return nil
	}

	// Marshal json g
	val, err := json.Marshal(g)
	if err != nil {
		return err
	}
	return db.Put(kv.ConfigTable, kv.GenesisKey, val)
}

func ReadGenesis(db kv.Getter) (*types.Genesis, error) {
	val, err := db.GetOne(kv.ConfigTable, kv.GenesisKey)
	if err != nil {
		return nil, err
	}
	if len(val) == 0 || string(val) == "null" {
		return nil, nil
	}
	var g types.Genesis
	if err := json.Unmarshal(val, &g); err != nil {
		return nil, err
	}
	return &g, nil
}

func AllSegmentsDownloadComplete(tx kv.Getter) (allSegmentsDownloadComplete bool, err error) {
	snapshotsStageProgress, err := stages.GetStageProgress(tx, stages.Snapshots)
	return snapshotsStageProgress > 0, err
}
func AllSegmentsDownloadCompleteFromDB(db kv.RoDB) (allSegmentsDownloadComplete bool, err error) {
	err = db.View(context.Background(), func(tx kv.Tx) error {
		allSegmentsDownloadComplete, err = AllSegmentsDownloadComplete(tx)
		return err
	})
	return allSegmentsDownloadComplete, err
}
