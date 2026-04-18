package integrity

import (
	"context"
	"fmt"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/services"
)

func CheckStateProgress(ctx context.Context, db kv.TemporalRoDB, blockReader services.FullBlockReader, failFast bool) (err error) {
	// state files should not be ahead of blocks files
	tx, err := db.BeginTemporalRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stateFileProgress := tx.Debug().DomainFiles(kv.CommitmentDomain).EndRootNum()

	txnumReader := blockReader.TxnumReader()
	blockFileProgress, err := txnumReader.Max(ctx, tx, blockReader.FrozenBlocks())
	if err != nil {
		return err
	}

	if stateFileProgress > blockFileProgress {
		return fmt.Errorf("state files progress (%d) is ahead of blocks files progress (%d). To recover: erigon seg rm-state --latest --datadir=<datadir>, then run integration stage_exec --reset --datadir=<datadir>", stateFileProgress, blockFileProgress)
	}

	return nil
}
