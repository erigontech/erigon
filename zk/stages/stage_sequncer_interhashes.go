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
)

type SequencerInterhashesCfg struct {
	db          kv.RwDB
	accumulator *shards.Accumulator
}

func StageSequencerInterhashesCfg(db kv.RwDB, accumulator *shards.Accumulator) SequencerInterhashesCfg {
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

	receipts, err := rawdb.ReadReceiptsByHash(tx, header.Hash())
	if err != nil {
		return err
	}

	// update the details related to anything that may have changed after figuring out the root
	header.Root = newRoot
	for _, r := range receipts {
		r.PostState = newRoot.Bytes()
	}
	header.ReceiptHash = types.DeriveSha(receipts)
	header.Bloom = types.CreateBloom(receipts)
	newHash := header.Hash()

	rawdb.WriteHeader(tx, header)
	if err := rawdb.WriteHeadHeaderHash(tx, newHash); err != nil {
		return err
	}
	if err := rawdb.WriteCanonicalHash(tx, newHash, header.Number.Uint64()); err != nil {
		return fmt.Errorf("failed to write header: %v", err)
	}

	err = rawdb.WriteReceipts(tx, header.Number.Uint64(), receipts)
	if err != nil {
		return fmt.Errorf("failed to write receipts: %v", err)
	}

	err = erigonDb.WriteBody(header.Number, newHash, latest.Transactions())
	if err != nil {
		return fmt.Errorf("failed to write body: %v", err)
	}

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
