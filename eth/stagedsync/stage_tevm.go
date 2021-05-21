package stagedsync

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/c2h5oh/datasize"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/core/types"
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

func transpileWithGo(tx ethdb.RwTx, batch ethdb.Database, quitCh <-chan struct{}) error {
	c, err := tx.Cursor(dbutils.ContractTEVMCodeStatusBucket)
	if err != nil {
		return err
	}
	defer c.Close()

	numWorkers := runtime.NumCPU()
	wg := sync.WaitGroup{}

	done := make(chan struct{})
	inContracts := make(chan contract, 32768)
	outContracts := make(chan contract, 32768)
	errs := make(chan error, numWorkers+1)

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for {
				select {
				case c := <-inContracts:
					transpiledCode, err := transpile(c.code)
					if err != nil {
						errs <- fmt.Errorf("contract %q cannot be transalated: %w",
							common.BytesToHash(c.hash).String(), err)

						common.SafeClose(done)
					}

					outContracts <- contract{c.hash, transpiledCode}
				case <-done:
					return
				}
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()

		for {
			select {
			case c := <-outContracts:
				err = batch.Put(dbutils.ContractTEVMCodeBucket, c.hash, c.code)
				if err != nil {
					errs <- fmt.Errorf("cannot store %q: %w", common.BytesToHash(c.hash), err)

					common.SafeClose(done)

					return
				}

				err = batch.Delete(dbutils.ContractTEVMCodeStatusBucket, c.hash, nil)
				if err != nil {
					errs <- fmt.Errorf("cannot reset translation status %q: %w",
						common.BytesToHash(c.hash), err)

					common.SafeClose(done)

					return
				}

			case <-done:
				return
			}
		}
	}()

	if err := ethdb.ForEach(c, func(codeHash, v []byte) (bool, error) {
		if err := common.Stopped(quitCh); err != nil {
			return false, err
		}
		if err := common.Stopped(done); err != nil {
			// return nil error to not overwrite error from errs channel
			return false, nil
		}

		// load the contract code
		contractCode, err := batch.GetOne(dbutils.CodeBucket, codeHash)
		if err != nil {
			return false, err
		}

		inContracts <- contract{codeHash, contractCode}

		return true, nil
	}); err != nil {
		return err
	}

	select {
	case <-done:
	// Channel was already closed
	case err := <-errs:
		common.SafeClose(done)
		wg.Wait()

		return err
	default:
		common.SafeClose(done)
	}

	wg.Wait()

	return nil
}

func transpile(code []byte) ([]byte, error) {
	return append(make([]byte, 0, len(code)), code...), nil
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

	logEvery := time.NewTicker(logInterval)
	defer logEvery.Stop()
	stageProgress := s.BlockNumber
	logBlock := stageProgress
	logTime := time.Now()

	for blockNum := stageProgress + 1; blockNum <= to; blockNum++ {
		err := common.Stopped(quit)
		if err != nil {
			return err
		}
		var block *types.Block
		if block, err = readBlock(blockNum, tx); err != nil {
			return err
		}
		if block == nil {
			log.Error(fmt.Sprintf("[%s] Empty block", logPrefix), "blocknum", blockNum)
			break
		}

		if err = transpileWithGo(tx, batch, quit); err != nil {
			return err
		}

		stageProgress = blockNum

		updateProgress := batch.BatchSize() >= int(cfg.batchSize)
		if updateProgress {
			if err = batch.Commit(); err != nil {
				return err
			}

			if !useExternalTx {
				if err = s.Update(tx, stageProgress); err != nil {
					return err
				}
				if err = tx.Commit(); err != nil {
					return err
				}

				tx, err = cfg.db.BeginRw(context.Background())
				if err != nil {
					return err
				}
			}
			batch = ethdb.NewBatch(tx)
		}

		select {
		default:
		case <-logEvery.C:
			logBlock, logTime = logProgress(logPrefix, logBlock, logTime, blockNum, batch)
			if hasTx, ok := tx.(ethdb.HasTx); ok {
				hasTx.Tx().CollectMetrics()
			}
		}
		stageTranspileGauge.Update(int64(blockNum))
	}

	if err := s.Update(batch, stageProgress); err != nil {
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

	log.Info(fmt.Sprintf("[%s] Completed on", logPrefix), "block", stageProgress)
	s.Done()
	return nil
}

func UnwindTranspileStage(u *UnwindState, s *StageState, tx ethdb.RwTx, quit <-chan struct{}, cfg TranspileCfg) error {
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
