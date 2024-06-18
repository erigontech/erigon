package diagnostics

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/ledgerwatch/erigon-lib/kv"
)

func ReadDataFromTable(db kv.RoDB, table string, key []byte) (data []byte) {
	if err := db.View(context.Background(), func(tx kv.Tx) error {
		bytes, err := tx.GetOne(table, key)

		if err != nil {
			return err
		}

		data = bytes

		return nil
	}); err != nil {
		return []byte{}
	}
	return data
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
