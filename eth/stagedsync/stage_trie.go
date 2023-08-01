package stagedsync

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/log/v3"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/rawdbv3"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/turbo/trie"
)

func CollectPatriciaKeys(s *StageState, ctx context.Context, tx kv.RwTx, cfg TrieCfg) error {
	ac := cfg.agg.MakeContext()
	defer ac.Close()

	domains := cfg.agg.SharedDomains(ac)
	defer domains.Close()

	acc := domains.Account.MakeContext()
	stc := domains.Storage.MakeContext()
	ctc := domains.Code.MakeContext()

	defer acc.Close()
	defer stc.Close()
	defer ctc.Close()

	//acc.DomainRangeLatest()

	return nil
}

func SpawnPatriciaTrieStage(s *StageState, u Unwinder, tx kv.RwTx, cfg TrieCfg, ctx context.Context, logger log.Logger) (libcommon.Hash, error) {
	quit := ctx.Done()
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
	if to > s.BlockNumber+16 {
		logger.Info(fmt.Sprintf("[%s] Generating intermediate hashes", logPrefix), "from", s.BlockNumber, "to", to)
	}

	var root libcommon.Hash
	tooBigJump := to > s.BlockNumber && to-s.BlockNumber > 100_000 // RetainList is in-memory structure and it will OOM if jump is too big, such big jump anyway invalidate most of existing Intermediate hashes
	if !tooBigJump && cfg.historyV3 && to-s.BlockNumber > 10 {
		//incremental can work only on DB data, not on snapshots
		_, n, err := rawdbv3.TxNums.FindBlockNum(tx, cfg.agg.EndTxNumMinimax())
		if err != nil {
			return trie.EmptyRoot, err
		}
		tooBigJump = s.BlockNumber < n
	}
	if s.BlockNumber == 0 || tooBigJump {
		if root, err = RegenerateIntermediateHashes(logPrefix, tx, cfg, expectedRootHash, ctx, logger); err != nil {
			return trie.EmptyRoot, err
		}
	} else {
		if root, err = IncrementIntermediateHashes(logPrefix, s, tx, to, cfg, expectedRootHash, quit, logger); err != nil {
			return trie.EmptyRoot, err
		}
	}

	if cfg.checkRoot && root != expectedRootHash {
		logger.Error(fmt.Sprintf("[%s] Wrong trie root of block %d: %x, expected (from header): %x. Block hash: %x", logPrefix, to, root, expectedRootHash, headerHash))
		if cfg.badBlockHalt {
			return trie.EmptyRoot, fmt.Errorf("wrong trie root")
		}
		if cfg.hd != nil {
			cfg.hd.ReportBadHeaderPoS(headerHash, syncHeadHeader.ParentHash)
		}
		if to > s.BlockNumber {
			unwindTo := (to + s.BlockNumber) / 2 // Binary search for the correct block, biased to the lower numbers
			logger.Warn("Unwinding due to incorrect root hash", "to", unwindTo)
			u.UnwindTo(unwindTo, headerHash)
		}
	} else if err = s.Update(tx, to); err != nil {
		return trie.EmptyRoot, err
	}

	if !useExternalTx {
		if err := tx.Commit(); err != nil {
			return trie.EmptyRoot, err
		}
	}

	return root, err
}
