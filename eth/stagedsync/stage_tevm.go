package stagedsync

import (
	"context"
	"fmt"
	"runtime"
	"time"

	"github.com/c2h5oh/datasize"
	"golang.org/x/sync/errgroup"

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

type contract struct {
	hash []byte
	code []byte
}

func transpileBatch(logPrefix string, s *StageState, tx ethdb.RwTx, batch ethdb.DbWithPendingMutations, cfg TranspileCfg, useExternalTx bool, quitCh <-chan struct{}) error {
	done := make(chan struct{})
	inContracts := make(chan contract, 32768)
	outContracts := make(chan contract, 32768)

	readWG := new(errgroup.Group)
	numWorkers := runtime.NumCPU()
	for i := 0; i < numWorkers; i++ {
		readWG.Go(func() error {
			for {
				select {
				case evmContract, closed := <-inContracts:
					if !closed {
						return nil
					}

					transpiledCode, err := transpileCode(evmContract.code)
					if err != nil {
						return tryError(fmt.Errorf("contract %q cannot be transalated: %w",
							common.BytesToHash(evmContract.hash).String(), err), done)
					}

					outContracts <- contract{evmContract.hash, transpiledCode}
				case <-done:
					return nil
				case <-quitCh:
					return common.ErrStopped
				}
			}
		})
	}

	logEvery := time.NewTicker(logInterval)
	defer logEvery.Stop()

	stageProgress := uint64(0)
	logBlock := stageProgress
	logTime := time.Now()

	// store translated results
	writeWG := new(errgroup.Group)
	writeWG.Go(func() error {
		var err error
		for {
			select {
			case tevmContract, closed := <-outContracts:
				if !closed {
					return nil
				}

				err = batch.Put(dbutils.ContractTEVMCodeBucket, tevmContract.hash, tevmContract.code)
				if err != nil {
					return tryError(fmt.Errorf("cannot store %q: %w", common.BytesToHash(tevmContract.hash), err), done)
				}

				err = batch.Delete(dbutils.ContractTEVMCodeStatusBucket, tevmContract.hash, nil)
				if err != nil {
					return tryError(fmt.Errorf("cannot reset translation status %q: %w",
						common.BytesToHash(tevmContract.hash), err), done)
				}

				stageProgress++

				currentSize := batch.BatchSize()
				updateProgress := currentSize >= int(cfg.batchSize)

				if updateProgress {
					if err = batch.Commit(); err != nil {
						return tryError(fmt.Errorf("cannot commit the batch of translations on %q: %w",
							common.BytesToHash(tevmContract.hash), err), done)
					}

					if !useExternalTx {
						if err = s.Update(tx, stageProgress); err != nil {
							return tryError(fmt.Errorf("cannot update the stage status on %q: %w",
								common.BytesToHash(tevmContract.hash), err), done)
						}
						if err = tx.Commit(); err != nil {
							return tryError(fmt.Errorf("cannot commit the external transation on %q: %w",
								common.BytesToHash(tevmContract.hash), err), done)
						}

						tx, err = cfg.db.BeginRw(context.Background())
						if err != nil {
							return tryError(fmt.Errorf("cannot begin the batch transaction on %q: %w",
								common.BytesToHash(tevmContract.hash), err), done)
						}

						// TODO: This creates stacked up deferrals
						defer tx.Rollback()
					}

					batch = ethdb.NewBatch(tx)
					// TODO: This creates stacked up deferrals
					defer batch.Rollback()

					stageTranspileGauge.Inc(int64(currentSize))
				}

			case <-logEvery.C:
				logBlock, logTime = logTEVMProgress(logPrefix, logBlock, logTime, stageProgress)
				if hasTx, ok := tx.(ethdb.HasTx); ok {
					hasTx.Tx().CollectMetrics()
				}
			case <-done:
				return nil
			case <-quitCh:
				return common.ErrStopped
			}
		}
	})

	// read contracts pending for translation
	c, err := tx.Cursor(dbutils.ContractTEVMCodeStatusBucket)
	if err != nil {
		return err
	}
	defer c.Close()

	if err := ethdb.ForEach(c, func(codeHash, _ []byte) (bool, error) {
		if err := common.Stopped(quitCh); err != nil {
			return false, err
		}
		if err := common.Stopped(done); err != nil {
			// return nil error to not overwrite error from errs channel
			return false, nil
		}

		// load the contract code. don't use batch to prevent a data race on creating a new batch variable.
		contractCode, err := tx.GetOne(dbutils.CodeBucket, codeHash)
		if err != nil {
			return false, err
		}

		inContracts <- contract{codeHash, contractCode}
		return true, nil
	}); err != nil {
		return tryError(fmt.Errorf("can't read pending code translations: %w", err), done)
	}

	close(inContracts)
	err = readWG.Wait()
	if err != nil {
		return err
	}

	close(outContracts)
	err = writeWG.Wait()
	if err != nil {
		return err
	}

	log.Info(fmt.Sprintf("[%s] Completed on", logPrefix), "contracts", stageProgress)

	return err
}

func tryError(err error, done chan struct{}) error {
	if err == nil {
		return nil
	}

	common.SafeClose(done)

	return err
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
	log.Info(fmt.Sprintf("[%s] Blocks translation", logPrefix), "from", s.BlockNumber, "to", to)

	batch := ethdb.NewBatch(tx)
	defer batch.Rollback()

	err := common.Stopped(quit)
	if err != nil {
		return err
	}

	if err = transpileBatch(logPrefix, s, tx, batch, cfg, useExternalTx, quit); err != nil {
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

	err := u.Done(tx)
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
