package stagedsync

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math/big"

	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/rlp"
)

type DifficultyCfg struct {
	tmpDir                  string
	terminalTotalDifficulty *big.Int
	db                      kv.RwDB
}

func StageDifficultyCfg(db kv.RwDB, tmpDir string, terminalTotalDifficulty *big.Int) DifficultyCfg {
	return DifficultyCfg{
		db:                      db,
		tmpDir:                  tmpDir,
		terminalTotalDifficulty: terminalTotalDifficulty,
	}
}

func SpawnDifficultyStage(s *StageState, tx kv.RwTx, cfg DifficultyCfg, ctx context.Context) (err error) {
	useExternalTx := tx != nil

	if !useExternalTx {
		var err error
		tx, err = cfg.db.BeginRw(context.Background())
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	quit := ctx.Done()
	headNumber, err := stages.GetStageProgress(tx, stages.Headers)
	if err != nil {
		return fmt.Errorf("getting headers progress: %w", err)
	}

	td := big.NewInt(0)
	if s.BlockNumber > 0 {
		td, err = rawdb.ReadTd(tx, rawdb.ReadHeaderByNumber(tx, s.BlockNumber).Hash(), s.BlockNumber)
		if err != nil {
			return err
		}
	}
	// If the chain does not have a proof of stake config or has reached terminalTotalDifficulty then we can skip this stage
	if cfg.terminalTotalDifficulty == nil || td.Cmp(cfg.terminalTotalDifficulty) >= 0 {
		if err = s.Update(tx, headNumber); err != nil {
			return err
		}
		if !useExternalTx {
			if err = tx.Commit(); err != nil {
				return err
			}
		}
		return nil
	}

	startKey := make([]byte, 8)
	binary.BigEndian.PutUint64(startKey, s.BlockNumber)

	header := new(types.Header)
	if err := etl.Transform(
		s.LogPrefix(),
		tx,
		kv.Headers,
		kv.HeaderTD,
		cfg.tmpDir,
		func(k []byte, v []byte, next etl.ExtractNextFunc) error {
			if len(k) != 40 {
				return nil
			}

			canonical, err := rawdb.ReadCanonicalHash(tx, binary.BigEndian.Uint64(k))
			if err != nil {
				return err
			}

			if !bytes.Equal(k[8:], canonical[:]) {
				return nil
			}
			if err := rlp.Decode(bytes.NewReader(v), header); err != nil {
				return err
			}
			td.Add(td, header.Difficulty)
			if td.Cmp(cfg.terminalTotalDifficulty) > 0 {
				return nil
			}
			data, err := rlp.EncodeToBytes(td)
			if err != nil {
				return fmt.Errorf("failed to RLP encode block total difficulty: %w", err)
			}
			return next(k, k, data)
		},
		etl.IdentityLoadFunc,
		etl.TransformArgs{
			ExtractStartKey: startKey,
			Quit:            quit,
		},
	); err != nil {
		return err
	}
	if err = s.Update(tx, headNumber); err != nil {
		return err
	}
	if !useExternalTx {
		if err = tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func UnwindDifficultyStage(u *UnwindState, tx kv.RwTx, ctx context.Context) (err error) {
	useExternalTx := tx != nil

	if err = u.Done(tx); err != nil {
		return fmt.Errorf(" reset: %w", err)
	}
	if !useExternalTx {
		if err = tx.Commit(); err != nil {
			return fmt.Errorf("failed to write db commit: %w", err)
		}
	}
	return nil
}

func PruneDifficultyStage(p *PruneState, tx kv.RwTx, ctx context.Context) (err error) {
	useExternalTx := tx != nil

	if !useExternalTx {
		if err = tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}
