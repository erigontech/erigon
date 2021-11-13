package stagedsync

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math/big"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
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
		fmt.Println(s.BlockNumber)
		td, err = rawdb.ReadTd(tx, rawdb.ReadHeaderByNumber(tx, s.BlockNumber).Hash(), s.BlockNumber)
		if err != nil {
			return err
		}
	}
	// fmt.Println(td.Uint64())
	// If the chain does not have a proof of stake config or has reached terminalTotalDifficulty then we can skip this stage
	if cfg.terminalTotalDifficulty == nil || td.Cmp(cfg.terminalTotalDifficulty) > 0 {
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

	logPrefix := s.LogPrefix()
	// Canonical hashes (for checking)
	canonicalC, err := tx.Cursor(kv.HeaderCanonical)
	if err != nil {
		return err
	}
	canonical := make([]common.Hash, headNumber-s.BlockNumber)
	currentHeaderIdx := uint64(0)

	for k, v, err := canonicalC.Seek(dbutils.EncodeBlockNumber(s.BlockNumber + 1)); k != nil; k, v, err = canonicalC.Next() {
		if err != nil {
			return err
		}
		if err := libcommon.Stopped(quit); err != nil {
			return err
		}

		if currentHeaderIdx >= headNumber-s.BlockNumber { // if header stage is ehead of body stage
			break
		}

		copy(canonical[currentHeaderIdx][:], v)
		currentHeaderIdx++
	}

	header := new(types.Header)
	currentHeaderIdx = 0
	if err := etl.Transform(
		logPrefix,
		tx,
		kv.Headers,
		kv.HeaderTD,
		cfg.tmpDir,
		func(k []byte, v []byte, next etl.ExtractNextFunc) error {
			if len(k) != 40 {
				return nil
			}

			// If not canonical, then we skip it
			if !bytes.Equal(k[8:], canonical[currentHeaderIdx][:]) {
				return nil
			}
			currentHeaderIdx++

			if err := rlp.Decode(bytes.NewReader(v), header); err != nil {
				return err
			}
			td.Add(td, header.Difficulty)
			if td.Cmp(cfg.terminalTotalDifficulty) < 0 {
				return nil
			}
			fmt.Println(td.Uint64())
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
