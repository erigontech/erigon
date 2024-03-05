package stages

import (
	"context"
	"fmt"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	db2 "github.com/ledgerwatch/erigon/smt/pkg/db"
	"github.com/ledgerwatch/erigon/smt/pkg/smt"
	"github.com/ledgerwatch/erigon/turbo/shards"
	"github.com/ledgerwatch/erigon/zk/erigon_db"
	rawdbZk "github.com/ledgerwatch/erigon/zk/rawdb"
)

type SequencerInterhashesCfg struct {
	db          kv.RwDB
	accumulator *shards.Accumulator
}

func StageSequencerInterhashesCfg(
	db kv.RwDB,
	accumulator *shards.Accumulator,
) SequencerInterhashesCfg {
	return SequencerInterhashesCfg{
		db:          db,
		accumulator: accumulator,
	}
}

func SpawnSequencerInterhashesStage(
	s *stagedsync.StageState,
	u stagedsync.Unwinder,
	tx kv.RwTx,
	ctx context.Context,
	cfg SequencerInterhashesCfg,
	initialCycle bool,
	quiet bool,
) error {
	var err error
	freshTx := tx == nil
	if freshTx {
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	to, err := s.ExecutionAt(tx)
	if err != nil {
		return err
	}

	erigonDb := erigon_db.NewErigonDb(tx)
	eridb := db2.NewEriDb(tx)
	smt := smt.NewSMT(eridb)

	// if we are at block 1 then just regenerate the whole thing otherwise take an incremental approach
	var newRoot libcommon.Hash
	if to == 1 {
		newRoot, err = regenerateIntermediateHashes(s.LogPrefix(), tx, eridb, smt)
		if err != nil {
			return err
		}
	} else {
		// todo [zkevm] we need to be prepared for multi-block batches at some point so this should really be a loop with a from/to
		// of the previous stage state and the latest block from execution stage
		newRoot, err = zkIncrementIntermediateHashes(s.LogPrefix(), s, tx, eridb, smt, to, false, nil, ctx.Done())
		if err != nil {
			return err
		}
	}

	latest, err := rawdb.ReadBlockByNumber(tx, to)
	if err != nil {
		return err
	}
	header := latest.Header()

	blockNum := header.Number.Uint64()
	preExecuteHeaderHash := header.Hash()
	receipts, err := rawdb.ReadReceiptsByHash(tx, preExecuteHeaderHash)
	if err != nil {
		return err
	}
	senders, err := rawdb.ReadSenders(tx, preExecuteHeaderHash, header.Number.Uint64())
	if err != nil {
		return err
	}

	// update the details related to anything that may have changed after figuring out the root
	header.Root = newRoot
	header.ReceiptHash = types.DeriveSha(receipts)
	header.Bloom = types.CreateBloom(receipts)
	newHash := header.Hash()

	rawdb.WriteHeader(tx, header)
	if err := rawdb.WriteHeadHeaderHash(tx, newHash); err != nil {
		return err
	}
	if err := rawdb.WriteCanonicalHash(tx, newHash, blockNum); err != nil {
		return fmt.Errorf("failed to write header: %v", err)
	}

	err = rawdb.WriteReceipts(tx, blockNum, receipts)
	if err != nil {
		return fmt.Errorf("failed to write receipts: %v", err)
	}

	err = erigonDb.WriteBody(header.Number, newHash, latest.Transactions())
	if err != nil {
		return fmt.Errorf("failed to write body: %v", err)
	}

	if err := rawdb.WriteSenders(tx, newHash, blockNum, senders); err != nil {
		return fmt.Errorf("failed to write senders: %v", err)
	}

	if err := rawdbZk.DeleteSenders(tx, preExecuteHeaderHash, blockNum); err != nil {
		return fmt.Errorf("failed to delete senders: %v", err)
	}

	if err := rawdbZk.DeleteHeader(tx, preExecuteHeaderHash, blockNum); err != nil {
		return fmt.Errorf("failed to delete header: %v", err)
	}

	//TODO: Consider deleting something else rather than only senders and headers
	//TODO: Consider offloading deleting to a thread that runs in a parallel to stages

	// write the new block lookup entries
	rawdb.WriteTxLookupEntries(tx, latest)

	if err := s.Update(tx, to); err != nil {
		return err
	}

	// inform the accumulator of this new block to update the txpool and anything else that needs to know
	// we need to do this here instead of execution as the interhashes stage will have updated the block
	// hashes
	if cfg.accumulator != nil {
		txs, err := rawdb.RawTransactionsRange(tx, header.Number.Uint64(), header.Number.Uint64())
		if err != nil {
			return err
		}
		cfg.accumulator.StartChange(header.Number.Uint64(), header.Hash(), txs, false)
	}

	if freshTx {
		if err = tx.Commit(); err != nil {
			return err
		}
	}

	return nil
}

func UnwindSequencerInterhashsStage(
	u *stagedsync.UnwindState,
	s *stagedsync.StageState,
	tx kv.RwTx,
	ctx context.Context,
	cfg SequencerInterhashesCfg,
	initialCycle bool,
) error {
	return nil
}

func PruneSequencerInterhashesStage(
	s *stagedsync.PruneState,
	tx kv.RwTx,
	cfg SequencerInterhashesCfg,
	ctx context.Context,
	initialCycle bool,
) error {
	return nil
}
