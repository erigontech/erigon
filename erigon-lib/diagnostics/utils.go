// Copyright 2024 The Erigon Authors
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

package diagnostics

import (
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
)

func ReadDataFromTable(tx kv.Tx, table string, key []byte) ([]byte, error) {
	bytes, err := tx.GetOne(table, key)

	if err != nil {
		return nil, err
	}

	return bytes, nil
}

func PutDataToTable(table string, key []byte, info any) func(tx kv.RwTx) error {
	return func(tx kv.RwTx) error {
		infoBytes, err := json.Marshal(info)

		if err != nil {
			return err
		}

		return tx.Put(table, key, infoBytes)
	}
}

func InitStagesFromList(list []string) []SyncStage {
	stages := make([]SyncStage, 0, len(list))

	for _, stage := range list {
		stages = append(stages, SyncStage{
			ID:        stage,
			State:     Queued,
			SubStages: []SyncSubStage{},
			Stats:     SyncStageStats{},
		})
	}

	return stages
}

func InitSubStagesFromList(list []string) []SyncSubStage {
	subStages := make([]SyncSubStage, 0, len(list))

	for _, subStage := range list {
		subStages = append(subStages,
			SyncSubStage{
				ID:    subStage,
				State: Queued,
			},
		)
	}

	return subStages
}

func CalculateTime(amountLeft, rate uint64) string {
	if rate == 0 {
		return "999hrs:99m"
	}
	timeLeftInSeconds := amountLeft / rate

	hours := timeLeftInSeconds / 3600
	minutes := (timeLeftInSeconds / 60) % 60

	return fmt.Sprintf("%dhrs:%dm", hours, minutes)
}

func SecondsToHHMMString(seconds uint64) string {
	if seconds == 0 {
		return "0hrs:0m"
	}

	hours := seconds / 3600
	minutes := (seconds / 60) % 60

	return fmt.Sprintf("%dhrs:%dm", hours, minutes)
}

func ParseData(data []byte, v interface{}) {
	if len(data) == 0 {
		return
	}

	err := json.Unmarshal(data, &v)
	if err != nil {
		log.Warn("[Diagnostics] Failed to parse data", "data", string(data), "type", reflect.TypeOf(v))
	}
}
