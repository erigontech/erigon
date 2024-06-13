package diagnostics

import (
	"context"
	"encoding/json"

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
