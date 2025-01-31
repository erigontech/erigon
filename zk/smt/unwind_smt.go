package smt

import (
	"context"
	"fmt"
	"math"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	db2 "github.com/ledgerwatch/erigon/smt/pkg/db"

	"github.com/ledgerwatch/erigon-lib/kv/membatchwithdb"

	"github.com/ledgerwatch/erigon/smt/pkg/smt"
	"github.com/ledgerwatch/erigon/turbo/trie"
	"github.com/ledgerwatch/erigon/zk"
	"github.com/ledgerwatch/erigon/zkevm/log"
)

func UnwindZkSMT(ctx context.Context, logPrefix string, from, to uint64, tx kv.RwTx, checkRoot bool, expectedRootHash *common.Hash, quiet bool) (common.Hash, error) {
	if !quiet {
		log.Info(fmt.Sprintf("[%s] Unwind trie hashes started", logPrefix))
		defer log.Info(fmt.Sprintf("[%s] Unwind ended", logPrefix))
	}

	eridb := db2.NewEriDb(tx)
	eridb.RollbackBatch()

	dbSmt := smt.NewSMT(eridb, false)

	if !quiet {
		log.Info(fmt.Sprintf("[%s]", logPrefix), "last root", common.BigToHash(dbSmt.LastRoot()))
	}

	// only open the batch if tx is not already one
	if _, ok := tx.(*membatchwithdb.MemoryMutation); !ok {
		quit := make(chan struct{})
		eridb.OpenBatch(quit)
	}

	cg := NewChangesGetter(tx)
	if err := cg.openChangesGetter(from); err != nil {
		return trie.EmptyRoot, fmt.Errorf("OpenChangesGetter: %w", err)
	}
	defer cg.closeChangesGetter()

	total := uint64(math.Abs(float64(from) - float64(to) + 1))
	progressChan, stopPrinter := zk.ProgressPrinter(fmt.Sprintf("[%s] Progress unwinding", logPrefix), total, quiet)
	defer stopPrinter()

	// walk backwards through the blocks, applying state changes, and deletes
	// PlainState contains data AT the block
	// History tables contain data BEFORE the block - so need a +1 offset
	for i := from; i >= to+1; i-- {
		select {
		case <-ctx.Done():
			return trie.EmptyRoot, fmt.Errorf("context done")
		default:
		}

		if err := cg.getChangesForBlock(i); err != nil {
			return trie.EmptyRoot, fmt.Errorf("getChangesForBlock: %w", err)
		}

		progressChan <- 1
	}

	stopPrinter()

	if _, _, err := dbSmt.SetStorage(ctx, logPrefix, cg.accChanges, cg.codeChanges, cg.storageChanges); err != nil {
		return trie.EmptyRoot, err
	}

	lr := dbSmt.LastRoot()

	hash := common.BigToHash(lr)
	if checkRoot && hash != *expectedRootHash {
		log.Error("failed to verify hash")
		return trie.EmptyRoot, fmt.Errorf("wrong trie root: %x, expected (from header): %x", hash, expectedRootHash)
	}

	if !quiet {
		log.Info(fmt.Sprintf("[%s] Trie root matches", logPrefix), "hash", hash.Hex())
	}

	if err := eridb.CommitBatch(); err != nil {
		return trie.EmptyRoot, err
	}

	return hash, nil
}
