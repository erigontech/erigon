package stagedsync

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/etl"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/crypto/secp256k1"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/params"
)

type SendersCfg struct {
	batchSize       int
	blockSize       int
	bufferSize      int
	numOfGoroutines int
	readChLen       int

	chainConfig *params.ChainConfig
}

func StageSendersCfg(chainCfg *params.ChainConfig) SendersCfg {
	const sendersBatchSize = 10000
	const sendersBlockSize = 4096

	return SendersCfg{
		batchSize:       sendersBatchSize,
		blockSize:       sendersBlockSize,
		bufferSize:      (sendersBlockSize * 10 / 20) * 10000, // 20*4096
		numOfGoroutines: secp256k1.NumOfContexts(),            // we can only be as parallels as our crypto library supports,
		readChLen:       4,

		chainConfig: chainCfg,
	}
}

func SpawnRecoverSendersStage(cfg SendersCfg, s *StageState, db ethdb.Database, toBlock uint64, tmpdir string, quitCh <-chan struct{}) error {
	var tx ethdb.RwTx
	var useExternalTx bool
	if hasTx, ok := db.(ethdb.HasTx); ok && hasTx.Tx() != nil {
		tx = hasTx.Tx().(ethdb.RwTx)
		useExternalTx = true
	} else {
		var err error
		tx, err = db.(ethdb.HasRwKV).RwKV().BeginRw(context.Background())
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
	log.Info(fmt.Sprintf("[%s] Started", logPrefix), "from", s.BlockNumber, "to", to)

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
	log.Info(fmt.Sprintf("[%s] Read canonical hashes", logPrefix), "amount", len(canonical))

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

	collectorSenders := etl.NewCollector(tmpdir, etl.NewSortableBuffer(etl.BufferOptimalSize))

	errCh := make(chan error)
	go func() {
		defer close(errCh)
		for j := range out {
			if j.err != nil {
				errCh <- j.err
				return
			}
			if err := common.Stopped(quitCh); err != nil {
				errCh <- j.err
				return
			}
			select {
			default:
			case <-logEvery.C:
				log.Info(fmt.Sprintf("[%s] Recovery", logPrefix), "block_number", j.index)
			}

			k := make([]byte, 4)
			binary.BigEndian.PutUint32(k, uint32(j.index))
			index := int(binary.BigEndian.Uint32(k))
			if err := collectorSenders.Collect(dbutils.BlockBodyKey(s.BlockNumber+uint64(index)+1, canonical[index]), j.senders); err != nil {
				errCh <- j.err
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
		body := rawdb.ReadBody(ethdb.NewRoTxDb(tx), blockHash, blockNumber)

		select {
		case err := <-errCh:
			if err != nil {
				return err
			}
		case jobs <- &senderRecoveryJob{body: body, key: k, blockNumber: blockNumber, index: int(blockNumber - s.BlockNumber - 1)}:
		}
	}

	close(jobs)
	wg.Wait()
	close(out)
	for err := range errCh {
		if err != nil {
			return err
		}
	}

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

	if err := s.DoneAndUpdate(tx, to); err != nil {
		return err
	}

	if !useExternalTx {
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

type senderRecoveryJob struct {
	body        *types.Body
	key         []byte
	blockNumber uint64
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

func UnwindSendersStage(u *UnwindState, s *StageState, db ethdb.Database) error {
	// Does not require any special processing
	mutation := db.NewBatch()
	err := u.Done(mutation)
	logPrefix := s.state.LogPrefix()
	if err != nil {
		return fmt.Errorf("%s: reset: %v", logPrefix, err)
	}
	err = mutation.Commit()
	if err != nil {
		return fmt.Errorf("%s: failed to write db commit: %v", logPrefix, err)
	}
	return nil
}
