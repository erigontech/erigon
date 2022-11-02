package main

import (
	"context"

	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon/cl/clparams"
	clcore "github.com/ledgerwatch/erigon/cmd/erigon-cl/cl-core"
	cldb "github.com/ledgerwatch/erigon/cmd/erigon-cl/cl-core/cl-db"
	"github.com/ledgerwatch/log/v3"
)

func main() {
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StderrHandler))

	ctx := context.Background()
	db, err := mdbx.NewTemporaryMdbx()
	if err != nil {
		log.Error("Error opening database", "err", err)
	}
	defer db.Close()
	uri := clparams.GetCheckpointSyncEndpoint(clparams.MainnetNetwork)

	state, err := clcore.RetrieveBeaconState(ctx, uri)

	if err != nil {
		log.Error("[Checkpoint Sync] Failed", "reason", err)
		return
	}
	tx, err := db.BeginRw(ctx)
	if err != nil {
		log.Error("[DB] Failed", "reason", err)
		return
	}
	defer tx.Rollback()

	if err := cldb.WriteBeaconState(tx, state); err != nil {
		log.Error("[DB] Failed", "reason", err)
		return
	}
	if _, err = cldb.ReadBeaconState(tx, state.Slot); err != nil {
		log.Error("[DB] Failed", "reason", err)
		return
	}
	log.Info("Everything Successfull Hurray!")
}
