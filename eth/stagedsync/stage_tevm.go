package stagedsync

import (
	"context"
	"fmt"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/metrics"
	"github.com/ledgerwatch/turbo-geth/params"
)

var stageTranspileGauge = metrics.NewRegisteredGauge("stage/tevm", nil)

type TranspileCfg struct {
	db            ethdb.RwKV
	batchSize     datasize.ByteSize
	readerBuilder StateReaderBuilder
	writerBuilder StateWriterBuilder
	chainConfig   *params.ChainConfig
	on            bool
}

func StageTranspileCfg(
	kv ethdb.RwKV,
	BatchSize datasize.ByteSize,
	ReaderBuilder StateReaderBuilder,
	WriterBuilder StateWriterBuilder,
	chainConfig *params.ChainConfig,
) TranspileCfg {
	return TranspileCfg{
		db:            kv,
		batchSize:     BatchSize,
		readerBuilder: ReaderBuilder,
		writerBuilder: WriterBuilder,
		chainConfig:   chainConfig,
		on:            false,
	}
}

func transpileWithGo(tx ethdb.RwTx, batch ethdb.Database, quitCh <-chan struct{}) error {
	c, err := tx.Cursor(dbutils.ContractTEVMCodeStatusBucket)
	if err != nil {
		return err
	}
	defer c.Close()

	ethdb.ForEach(c, func(codeHash, v []byte) (bool, error) {
		if err := common.Stopped(quitCh); err != nil {
			return false, err
		}

		// load the contract code
		contractCode, err := batch.GetOne(dbutils.CodeBucket, codeHash)
		if err != nil {
			return false, err
		}

		// transpile
		transpiledCode := transpile(contractCode)

		err = batch.Put(dbutils.ContractTEVMCodeBucket, codeHash, transpiledCode)
		if err != nil {
			return false, err
		}

		err = batch.Delete(dbutils.ContractTEVMCodeStatusBucket, codeHash, nil)
		if err != nil {
			return false, err
		}

		return true, nil
	})

	return nil
}

func transpile(code []byte) []byte {
	return append(make([]byte, 0, len(code)), code...)
}

func SpawnTranspileStage(s *StageState, tx ethdb.RwTx, toBlock uint64, quit <-chan struct{}, cfg TranspileCfg) error {
	if !cfg.on {
		return nil
	}

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
	s.Done()
	return nil
}
