package stagedsync

import (
	"bytes"
	"context"
	"fmt"
	"sync/atomic"

	"github.com/ledgerwatch/erigon/core/state/temporal"
	"github.com/ledgerwatch/log/v3"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/turbo/trie"
)

func collectAndComputeCommitment(s *StageState, ctx context.Context, tx kv.RwTx, cfg TrieCfg) ([]byte, error) {
	agg, ac := tx.(*temporal.Tx).Agg(), tx.(*temporal.Tx).AggCtx()

	domains := agg.SharedDomains(ac)
	defer domains.Close()

	acc := domains.Account.MakeContext()
	stc := domains.Storage.MakeContext()

	defer acc.Close()
	defer stc.Close()

	logger := log.New("stage", "patricia_trie", "block", s.BlockNumber)
	logger.Info("Collecting account keys")
	collector := etl.NewCollector("collect_keys", cfg.tmpDir, etl.NewSortableBuffer(etl.BufferOptimalSize/2), logger)
	defer collector.Close()

	var totalKeys atomic.Uint64
	for _, dc := range []*state.DomainContext{acc, stc} {
		logger.Info("Collecting keys")
		err := dc.IteratePrefix(tx, nil, func(k []byte, _ []byte) {
			if err := collector.Collect(k, nil); err != nil {
				panic(err)
			}
			totalKeys.Add(1)

			if ctx.Err() != nil {
				panic(ctx.Err())
			}
		})
		if err != nil {
			return nil, err
		}
	}

	var (
		batchSize = 10_000_000
		batch     = make([][]byte, 0, batchSize)
		processed atomic.Uint64
	)

	loadKeys := func(k, v []byte, table etl.CurrentTableReader, next etl.LoadNextFunc) error {
		if len(batch) >= batchSize {
			rh, err := domains.Commit(true, false)
			if err != nil {
				return err
			}
			logger.Info("Committing batch",
				"processed", fmt.Sprintf("%d/%d (%.2f%%)",
					processed.Load(), totalKeys.Load(), 100*(float64(totalKeys.Load())/float64(processed.Load()))),
				"intermediate root", rh)
		}
		processed.Add(1)
		domains.Commitment.TouchPlainKey(k, nil, nil) // will panic if CommitmentModeUpdates is used. which is OK.

		return nil
	}
	err := collector.Load(nil, "", loadKeys, etl.TransformArgs{Quit: ctx.Done()})
	if err != nil {
		return nil, err
	}
	collector.Close()

	rh, err := domains.Commit(true, false)
	if err != nil {
		return nil, err
	}
	logger.Info("Commitment has been reevaluated", "block", s.BlockNumber, "root", rh, "processed", processed.Load(), "total", totalKeys.Load())

	if err := cfg.agg.Flush(ctx, tx); err != nil {
		return nil, err
	}

	//acc.DomainRangeLatest()
	return rh, nil
}

func SpawnPatriciaTrieStage(s *StageState, u Unwinder, tx kv.RwTx, cfg TrieCfg, ctx context.Context, logger log.Logger) (libcommon.Hash, error) {
	useExternalTx := tx != nil
	if !useExternalTx {
		var err error
		tx, err = cfg.db.BeginRw(context.Background())
		if err != nil {
			return trie.EmptyRoot, err
		}
		defer tx.Rollback()
	}

	to, err := s.ExecutionAt(tx)
	if err != nil {
		return trie.EmptyRoot, err
	}
	if s.BlockNumber > to { // Erigon will self-heal (download missed blocks) eventually
		return trie.EmptyRoot, nil
	}

	if s.BlockNumber == to {
		// we already did hash check for this block
		// we don't do the obvious `if s.BlockNumber > to` to support reorgs more naturally
		return trie.EmptyRoot, nil
	}

	var expectedRootHash libcommon.Hash
	var headerHash libcommon.Hash
	var syncHeadHeader *types.Header
	if cfg.checkRoot {
		syncHeadHeader, err = cfg.blockReader.HeaderByNumber(ctx, tx, to)
		if err != nil {
			return trie.EmptyRoot, err
		}
		if syncHeadHeader == nil {
			return trie.EmptyRoot, fmt.Errorf("no header found with number %d", to)
		}
		expectedRootHash = syncHeadHeader.Root
		headerHash = syncHeadHeader.Hash()
	}
	logPrefix := s.LogPrefix()
	rh, err := collectAndComputeCommitment(s, ctx, tx, cfg)
	if err != nil {
		return trie.EmptyRoot, err
	}
	if cfg.checkRoot && bytes.Equal(rh, expectedRootHash[:]) {
		logger.Error(fmt.Sprintf("[%s] Wrong trie root of block %d: %x, expected (from header): %x. Block hash: %x", logPrefix, to, rh, expectedRootHash, headerHash))
		if cfg.badBlockHalt {
			return trie.EmptyRoot, fmt.Errorf("wrong trie root")
		}
		//if cfg.hd != nil {
		//	cfg.hd.ReportBadHeaderPoS(headerHash, syncHeadHeader.ParentHash)
		//}
		if to > s.BlockNumber {
			unwindTo := (to + s.BlockNumber) / 2 // Binary search for the correct block, biased to the lower numbers
			logger.Warn("Unwinding (should to) due to incorrect root hash", "to", unwindTo)
			//u.UnwindTo(unwindTo, headerHash)
		}
	} else if err = s.Update(tx, to); err != nil {
		return trie.EmptyRoot, err
	}

	if !useExternalTx {
		if err := tx.Commit(); err != nil {
			return trie.EmptyRoot, err
		}
	}
	return libcommon.BytesToHash(rh[:]), err
}
