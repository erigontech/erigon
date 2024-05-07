package stagedsync

import (
	"context"
	"fmt"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/cmd/verkle/verkletrie"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
)

func SpawnVerkleTrie(s *StageState, u Unwinder, tx kv.RwTx, cfg TrieCfg, ctx context.Context, logger log.Logger) (libcommon.Hash, error) {
	var err error
	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return libcommon.Hash{}, err
		}
		defer tx.Rollback()
	}
	from := uint64(0)
	if s.BlockNumber > 0 {
		from = s.BlockNumber + 1
	}
	to, err := s.ExecutionAt(tx)
	if err != nil {
		return libcommon.Hash{}, err
	}
	verkleWriter := verkletrie.NewVerkleTreeWriter(tx, cfg.tmpDir, logger)
	if err := verkletrie.IncrementAccount(tx, tx, 10, verkleWriter, from, to, cfg.tmpDir); err != nil {
		return libcommon.Hash{}, err
	}
	var newRoot libcommon.Hash
	if newRoot, err = verkletrie.IncrementStorage(tx, tx, 10, verkleWriter, from, to, cfg.tmpDir); err != nil {
		return libcommon.Hash{}, err
	}
	if cfg.checkRoot {
		header := rawdb.ReadHeaderByNumber(tx, to)
		if header.Root != newRoot {
			return libcommon.Hash{}, fmt.Errorf("invalid verkle root, header has %x, computed: %x", header.Root, newRoot)
		}
	}
	if err := s.Update(tx, to); err != nil {
		return libcommon.Hash{}, err
	}
	if err := stages.SaveStageProgress(tx, stages.VerkleTrie, to); err != nil {
		return libcommon.Hash{}, err
	}
	if !useExternalTx {
		return newRoot, tx.Commit()
	}
	return newRoot, nil
}

func UnwindVerkleTrie(u *UnwindState, s *StageState, tx kv.RwTx, cfg TrieCfg, ctx context.Context, logger log.Logger) (err error) {
	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}
	from := u.UnwindPoint + 1
	to, err := s.ExecutionAt(tx)
	if err != nil {
		return err
	}
	verkleWriter := verkletrie.NewVerkleTreeWriter(tx, cfg.tmpDir, logger)
	if err := verkletrie.IncrementAccount(tx, tx, 10, verkleWriter, from, to, cfg.tmpDir); err != nil {
		return err
	}
	if _, err = verkletrie.IncrementStorage(tx, tx, 10, verkleWriter, from, to, cfg.tmpDir); err != nil {
		return err
	}
	if err := s.Update(tx, from); err != nil {
		return err
	}
	if err := stages.SaveStageProgress(tx, stages.VerkleTrie, from); err != nil {
		return err
	}
	if !useExternalTx {
		return tx.Commit()
	}
	return nil
}

func PruneVerkleTries(s *PruneState, tx kv.RwTx, cfg TrieCfg, ctx context.Context) (err error) {
	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}
	s.Done(tx)

	if !useExternalTx {
		if err = tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}
