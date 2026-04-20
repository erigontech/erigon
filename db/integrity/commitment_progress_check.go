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

	blockFileProgress, err := MaxCollatableTxNum(ctx, tx, blockReader)
	if err != nil {
		return err
	}

	if stateFileProgress > blockFileProgress {
		return fmt.Errorf("state files progress (%d) is ahead of blocks files progress (%d). To recover: erigon seg rm-state --latest --datadir=<datadir>, then run integration stage_exec --reset --datadir=<datadir>", stateFileProgress, blockFileProgress)
	}

	return nil
}

// MaxCollatableTxNum returns the upper bound txNum that state collation may
// target without running ahead of block snapshot files. Callers of
// Aggregator.BuildFiles / BuildFilesInBackground must cap their target txNum
// by this value — otherwise state files may advance past block files, an
// unrecoverable state that requires manual `erigon seg rm-state --latest` to
// release. See CheckStateProgress for the detection counterpart.
func MaxCollatableTxNum(ctx context.Context, tx kv.Tx, blockReader services.FullBlockReader) (uint64, error) {
	return blockReader.TxnumReader().Max(ctx, tx, blockReader.FrozenBlocks())
}
