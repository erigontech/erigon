package stagedsync

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/common/etl"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/crypto/secp256k1"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/log"
	"github.com/ledgerwatch/erigon/params"
)

type SendersCfg struct {
	db              ethdb.RwKV
	batchSize       int
	blockSize       int
	bufferSize      int
	numOfGoroutines int
	readChLen       int
	tmpdir          string

	chainConfig *params.ChainConfig
}

func StageSendersCfg(db ethdb.RwKV, chainCfg *params.ChainConfig, tmpdir string) SendersCfg {
	const sendersBatchSize = 10000
	const sendersBlockSize = 4096

	return SendersCfg{
		db:              db,
		batchSize:       sendersBatchSize,
		blockSize:       sendersBlockSize,
		bufferSize:      (sendersBlockSize * 10 / 20) * 10000, // 20*4096
		numOfGoroutines: secp256k1.NumOfContexts(),            // we can only be as parallels as our crypto library supports,
		readChLen:       4,
		tmpdir:          tmpdir,
		chainConfig:     chainCfg,
	}
}

func SpawnRecoverSendersStage(cfg SendersCfg, s *StageState, u Unwinder, tx ethdb.RwTx, toBlock uint64, quitCh <-chan struct{}) error {
	useExternalTx := tx != nil
	if !useExternalTx {
		var err error
		tx, err = cfg.db.BeginRw(context.Background())
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	prevStageProgress, errStart := stages.GetStageProgress(tx, stages.Bodies)
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
	if to > s.BlockNumber+16 {
		log.Info(fmt.Sprintf("[%s] Started", logPrefix), "from", s.BlockNumber, "to", to)
	}

	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()

	canonical := make([]common.Hash, to-s.BlockNumber)
	currentHeaderIdx := uint64(0)

	canonicalC, err := tx.Cursor(dbutils.HeaderCanonicalBucket)
	if err != nil {
		return err
	}
	defer canonicalC.Close()

	for k, v, err := canonicalC.Seek(dbutils.EncodeBlockNumber(s.BlockNumber + 1)); k != nil; k, v, err = canonicalC.Next() {
		if err != nil {
			return err
		}
		if err := common.Stopped(quitCh); err != nil {
			return err
		}

		if currentHeaderIdx >= to-s.BlockNumber { // if header stage is ehead of body stage
			break
		}

		copy(canonical[currentHeaderIdx][:], v)
		currentHeaderIdx++

		select {
		default:
		case <-logEvery.C:
			log.Info(fmt.Sprintf("[%s] Preload headedrs", logPrefix), "block_number", binary.BigEndian.Uint64(k))
		}
	}
	log.Debug(fmt.Sprintf("[%s] Read canonical hashes", logPrefix), "amount", len(canonical))

	jobs := make(chan *senderRecoveryJob, cfg.batchSize)
	out := make(chan *senderRecoveryJob, cfg.batchSize)
	wg := new(sync.WaitGroup)
	wg.Add(cfg.numOfGoroutines)
	for i := 0; i < cfg.numOfGoroutines; i++ {
		go func(threadNo int) {
			defer wg.Done()
			// each goroutine gets it's own crypto context to make sure they are really parallel
			recoverSenders(logPrefix, secp256k1.ContextForThread(threadNo), cfg.chainConfig, jobs, out, quitCh)
		}(i)
	}

	collectorSenders := etl.NewCollector(cfg.tmpdir, etl.NewSortableBuffer(etl.BufferOptimalSize))

	errCh := make(chan senderRecoveryError)
	go func() {
		defer close(errCh)
		for j := range out {
			if j.err != nil {
				errCh <- senderRecoveryError{err: j.err, blockNumber: j.blockNumber, blockHash: j.blockHash}
				return
			}
			if err := common.Stopped(quitCh); err != nil {
				errCh <- senderRecoveryError{err: j.err}
				return
			}
			select {
			default:
			case <-logEvery.C:
				log.Info(fmt.Sprintf("[%s] Recovery", logPrefix), "block_number", s.BlockNumber+uint64(j.index))
			}

			k := make([]byte, 4)
			binary.BigEndian.PutUint32(k, uint32(j.index))
			index := int(binary.BigEndian.Uint32(k))
			if err := collectorSenders.Collect(dbutils.BlockBodyKey(s.BlockNumber+uint64(index)+1, canonical[index]), j.senders); err != nil {
				errCh <- senderRecoveryError{err: j.err}
				return
			}
		}
	}()

	bodiesC, err := tx.Cursor(dbutils.BlockBodyPrefix)
	if err != nil {
		return err
	}
	defer bodiesC.Close()

	for k, _, err := bodiesC.Seek(dbutils.EncodeBlockNumber(s.BlockNumber + 1)); k != nil; k, _, err = bodiesC.Next() {
		if err != nil {
			return err
		}
		if err := common.Stopped(quitCh); err != nil {
			return err
		}

		blockNumber := binary.BigEndian.Uint64(k[:8])
		blockHash := common.BytesToHash(k[8:])
		if blockNumber > to {
			break
		}

		if canonical[blockNumber-s.BlockNumber-1] != blockHash {
			// non-canonical case
			continue
		}
		body := rawdb.ReadBody(tx, blockHash, blockNumber)

		select {
		case recoveryErr := <-errCh:
			if recoveryErr.err != nil {
				if recoveryErr.blockHash == (common.Hash{}) {
					return recoveryErr.err
				}
			}
		case jobs <- &senderRecoveryJob{body: body, key: k, blockNumber: blockNumber, blockHash: blockHash, index: int(blockNumber - s.BlockNumber - 1)}:
		}
	}

	close(jobs)
	wg.Wait()
	close(out)
	var minBlockNum uint64 = math.MaxUint64
	var minBlockHash common.Hash
	var minBlockErr error
	for recoveryErr := range errCh {
		if recoveryErr.err != nil {
			if recoveryErr.blockHash == (common.Hash{}) {
				return recoveryErr.err
			}
			if recoveryErr.blockNumber < minBlockNum {
				minBlockNum = recoveryErr.blockNumber
				minBlockHash = recoveryErr.blockHash
				minBlockErr = recoveryErr.err
			}
		}
	}
	if minBlockErr != nil {
		log.Error(fmt.Sprintf("[%s] Error recovering senders for block %d %x): %v", logPrefix, minBlockNum, minBlockHash, minBlockErr))
		if to > s.BlockNumber {
			if err = u.UnwindTo(minBlockNum-1, tx, minBlockHash); err != nil {
				return err
			}
		}
		s.Done()
	} else {
		if err := collectorSenders.Load(logPrefix, tx,
			dbutils.Senders,
			etl.IdentityLoadFunc,
			etl.TransformArgs{
				Quit: quitCh,
				LogDetailsLoad: func(k, v []byte) (additionalLogArguments []interface{}) {
					return []interface{}{"block", binary.BigEndian.Uint64(k)}
				},
			},
		); err != nil {
			return err
		}
		if err = s.DoneAndUpdate(tx, to); err != nil {
			return err
		}
	}

	if !useExternalTx {
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

type senderRecoveryError struct {
	err         error
	blockNumber uint64
	blockHash   common.Hash
}

type senderRecoveryJob struct {
	body        *types.Body
	key         []byte
	blockNumber uint64
	blockHash   common.Hash
	index       int
	senders     []byte
	err         error
}

func recoverSenders(logPrefix string, cryptoContext *secp256k1.Context, config *params.ChainConfig, in, out chan *senderRecoveryJob, quit <-chan struct{}) {
	for job := range in {
		if job == nil {
			return
		}
		body := job.body
		signer := types.MakeSigner(config, job.blockNumber)
		job.senders = make([]byte, len(body.Transactions)*common.AddressLength)
		for i, tx := range body.Transactions {
			from, err := signer.SenderWithContext(cryptoContext, tx)
			if err != nil {
				job.err = fmt.Errorf("%s: error recovering sender for tx=%x, %w", logPrefix, tx.Hash(), err)
				break
			}
			copy(job.senders[i*common.AddressLength:], from[:])
		}

		// prevent sending to close channel
		if err := common.Stopped(quit); err != nil {
			job.err = err
		}
		out <- job

		if errors.Is(job.err, common.ErrStopped) {
			return
		}
	}
}

func UnwindSendersStage(u *UnwindState, s *StageState, tx ethdb.RwTx, cfg SendersCfg) error {
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
