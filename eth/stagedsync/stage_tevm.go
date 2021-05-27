package stagedsync

import (
	"context"
	"fmt"
	"runtime"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/log"
	"github.com/ledgerwatch/erigon/metrics"
	"github.com/ledgerwatch/erigon/params"
)

var stageTranspileGauge = metrics.NewRegisteredGauge("stage/tevm", nil)

type TranspileCfg struct {
	db            ethdb.RwKV
	batchSize     datasize.ByteSize
	readerBuilder StateReaderBuilder
	writerBuilder StateWriterBuilder
	chainConfig   *params.ChainConfig
}

func StageTranspileCfg(
	kv ethdb.RwKV,
	batchSize datasize.ByteSize,
	readerBuilder StateReaderBuilder,
	writerBuilder StateWriterBuilder,
	chainConfig *params.ChainConfig,
) TranspileCfg {
	return TranspileCfg{
		db:            kv,
		batchSize:     batchSize,
		readerBuilder: readerBuilder,
		writerBuilder: writerBuilder,
		chainConfig:   chainConfig,
	}
}

func transpileBatch(logPrefix string, s *StageState, fromBlock uint64, toBlock uint64, tx ethdb.RwTx, batch ethdb.DbWithPendingMutations, cfg TranspileCfg, useExternalTx bool, quitCh <-chan struct{}) error {
	logEvery := time.NewTicker(logInterval)
	defer logEvery.Stop()

	stageProgress := uint64(0)
	logBlock := stageProgress
	logTime := time.Now()

	// read contracts pending for translation
	keyStart := dbutils.EncodeBlockNumber(fromBlock + 1)
	c, err := tx.CursorDupSort(dbutils.ContractTEVMCodeStatusBucket)
	if err != nil {
		return err
	}
	defer c.Close()

	for k, hash, err := c.Seek(keyStart); k != nil; k, hash, err = c.Next() {
		if err != nil {
			return fmt.Errorf("can't read pending code translations: %w", err)
		}
		if err = common.Stopped(quitCh); err != nil {
			return fmt.Errorf("can't read pending code translations: %w", err)
		}

		select {
		case <-logEvery.C:
			logBlock, logTime = logTEVMProgress(logPrefix, logBlock, logTime, stageProgress)
			if hasTx, ok := tx.(ethdb.HasTx); ok {
				hasTx.Tx().CollectMetrics()
			}
		default:
		}

		block, err := dbutils.DecodeBlockNumber(k)
		if err != nil {
			return fmt.Errorf("can't read pending code translations: %w", err)
		}

		if block > toBlock {
			return nil
		}

		// load the contract code. don't use batch to prevent a data race on creating a new batch variable.
		evmContract, err := batch.GetOne(dbutils.CodeBucket, hash)
		if err != nil {
			return fmt.Errorf("can't read pending code translations: %w", err)
		}

		// call a transpiler
		transpiledCode, err := transpileCode(evmContract)
		if err != nil {
			return fmt.Errorf("contract %q cannot be translated: %w",
				common.BytesToHash(hash).String(), err)
		}

		// store TEVM contract code
		err = batch.Put(dbutils.ContractTEVMCodeBucket, hash, transpiledCode)
		if err != nil {
			return fmt.Errorf("cannot store TEVM code %q: %w", common.BytesToHash(hash), err)
		}

		stageProgress++

		currentSize := batch.BatchSize()
		updateProgress := currentSize >= int(cfg.batchSize)

		if updateProgress {
			if err = batch.Commit(); err != nil {
				return fmt.Errorf("cannot commit the batch of translations on %q: %w",
					common.BytesToHash(hash), err)
			}

			if !useExternalTx {
				if err = s.Update(tx, stageProgress); err != nil {
					return fmt.Errorf("cannot update the stage status on %q: %w",
						common.BytesToHash(hash), err)
				}
				if err = tx.Commit(); err != nil {
					return fmt.Errorf("cannot commit the external transation on %q: %w",
						common.BytesToHash(hash), err)
				}

				tx, err = cfg.db.BeginRw(context.Background())
				if err != nil {
					return fmt.Errorf("cannot begin the batch transaction on %q: %w",
						common.BytesToHash(hash), err)
				}

				// TODO: This creates stacked up deferrals
				defer tx.Rollback()
			}

			batch = ethdb.NewBatch(tx)
			// TODO: This creates stacked up deferrals
			defer batch.Rollback()

			stageTranspileGauge.Inc(int64(currentSize))
		}
	}

	log.Info(fmt.Sprintf("[%s] Completed on", logPrefix), "contracts", stageProgress)

	return nil
}

func logTEVMProgress(logPrefix string, prevContract uint64, prevTime time.Time, currentContract uint64) (uint64, time.Time) {
	currentTime := time.Now()
	interval := currentTime.Sub(prevTime)
	speed := float64(currentContract-prevContract) / float64(interval/time.Second)
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	var logpairs = []interface{}{
		"number", currentContract,
		"contracts/second", speed,
	}
	logpairs = append(logpairs, "alloc", common.StorageSize(m.Alloc), "sys", common.StorageSize(m.Sys), "numGC", int(m.NumGC))
	log.Info(fmt.Sprintf("[%s] Translated contracts", logPrefix), logpairs...)

	return currentContract, currentTime
}

func SpawnTranspileStage(s *StageState, tx ethdb.RwTx, toBlock uint64, quit <-chan struct{}, cfg TranspileCfg) error {
	useExternalTx := tx != nil
	if !useExternalTx {
		var err error
		tx, err = cfg.db.BeginRw(context.Background())
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	prevStageProgress, errStart := stages.GetStageProgress(tx, stages.Execution)
	if errStart != nil {
		return errStart
	}

	var to = prevStageProgress
	if toBlock > 0 {
		to = min(prevStageProgress, toBlock)
	}

	if to <= s.BlockNumber {
		s.Done()
		return nil
	}

	logPrefix := s.state.LogPrefix()
	log.Info(fmt.Sprintf("[%s] Contract translation", logPrefix), "from", s.BlockNumber, "to", to)

	batch := ethdb.NewBatch(tx)
	defer batch.Rollback()

	err := common.Stopped(quit)
	if err != nil {
		return err
	}

	if err = transpileBatch(logPrefix, s, s.BlockNumber, to, tx, batch, cfg, useExternalTx, quit); err != nil {
		return err
	}

	// commit the same number as execution
	if err := s.Update(batch, prevStageProgress); err != nil {
		return err
	}
	if err := batch.Commit(); err != nil {
		return fmt.Errorf("%s: failed to write batch commit: %v", logPrefix, err)
	}

	if !useExternalTx {
		if err := tx.Commit(); err != nil {
			return err
		}
	}

	log.Info(fmt.Sprintf("[%s] Completed on", logPrefix), "block", prevStageProgress)
	s.Done()
	return nil
}

func UnwindTranspileStage(u *UnwindState, s *StageState, tx ethdb.RwTx, _ <-chan struct{}, cfg TranspileCfg) error {
	useExternalTx := tx != nil
	if !useExternalTx {
		var err error
		tx, err = cfg.db.BeginRw(context.Background())
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	keyStart := dbutils.EncodeBlockNumber(u.UnwindPoint + 1)
	c, err := tx.CursorDupSort(dbutils.ContractTEVMCodeStatusBucket)
	if err != nil {
		return err
	}
	defer c.Close()

	for k, hash, err := c.Seek(keyStart); k != nil; k, hash, err = c.Next() {
		if err != nil {
			return err
		}

		if err = tx.Delete(dbutils.ContractTEVMCodeBucket, hash, nil); err != nil {
			return err
		}
	}

	err = u.Done(tx)
	logPrefix := s.state.LogPrefix()
	if err != nil {
		return fmt.Errorf("%s: reset: %v", logPrefix, err)
	}
	if !useExternalTx {
		err = tx.Commit()
		if err != nil {
			return fmt.Errorf("%s: failed to write db commit: %v", logPrefix, err)
		}
	}
	return nil
}

// todo: TBD actual TEVM translator
func transpileCode(code []byte) ([]byte, error) {
	return append(make([]byte, 0, len(code)), code...), nil
}
