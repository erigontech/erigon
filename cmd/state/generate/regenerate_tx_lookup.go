package generate

import (
	"context"
	"os"
	"os/signal"
	"time"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/ethdb/prune"
	"github.com/ledgerwatch/log/v3"
)

func RegenerateTxLookup(chaindata string) error {
	db := mdbx.MustOpen(chaindata)
	defer db.Close()
	tx, err := db.BeginRw(context.Background())
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := tx.ClearBucket(kv.TxLookup); err != nil {
		return err
	}

	startTime := time.Now()
	ch := make(chan os.Signal, 1)
	quitCh := make(chan struct{})
	signal.Notify(ch, os.Interrupt)
	go func() {
		<-ch
		close(quitCh)
	}()

	pm, err := prune.Get(tx)
	if err != nil {
		return err
	}
	lastExecutedBlock, err := stages.GetStageProgress(tx, stages.Execution)
	if err != nil {
		//There could be headers without block in the end
		log.Error("Cant get last executed block", "err", err)
	}
	log.Info("TxLookup generation started", "start time", startTime)
	err = stagedsync.TxLookupTransform("txlookup", tx,
		dbutils.EncodeBlockNumber(pm.TxIndex.PruneTo(lastExecutedBlock)),
		dbutils.EncodeBlockNumber(lastExecutedBlock+1),
		quitCh,
		stagedsync.StageTxLookupCfg(db, pm, os.TempDir()))
	if err != nil {
		return err
	}
	log.Info("TxLookup index is successfully regenerated", "it took", time.Since(startTime))
	return nil
}
