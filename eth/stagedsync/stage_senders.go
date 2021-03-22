package stagedsync

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math/big"
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

type Stage3Config struct {
	BatchSize       int
	BlockSize       int
	BufferSize      int
	ToProcess       int
	NumOfGoroutines int
	ReadChLen       int
	Now             time.Time
}

func SpawnRecoverSendersStage(cfg Stage3Config, s *StageState, db ethdb.Database, config *params.ChainConfig, toBlock uint64, tmpdir string, quitCh <-chan struct{}) error {
	prevStageProgress, errStart := stages.GetStageProgress(db, stages.Bodies)
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

	if err := db.Walk(dbutils.HeaderCanonicalBucket, dbutils.EncodeBlockNumber(s.BlockNumber+1), 0, func(k, v []byte) (bool, error) {
		if err := common.Stopped(quitCh); err != nil {
			return false, err
		}

		if currentHeaderIdx >= to-s.BlockNumber { // if header stage is ehead of body stage
			return false, nil
		}

		copy(canonical[currentHeaderIdx][:], v)
		currentHeaderIdx++

		select {
		default:
		case <-logEvery.C:
			log.Info(fmt.Sprintf("[%s] Preload headedrs", logPrefix), "block_number", binary.BigEndian.Uint64(k))
		}

		return true, nil
	}); err != nil {
		return err
	}

	log.Info(fmt.Sprintf("[%s] Read canonical hashes", logPrefix), "amount", len(canonical))

	jobs := make(chan *senderRecoveryJob, cfg.BatchSize)
	out := make(chan *senderRecoveryJob, cfg.BatchSize)
	wg := new(sync.WaitGroup)
	wg.Add(cfg.NumOfGoroutines)
	for i := 0; i < cfg.NumOfGoroutines; i++ {
		go func(threadNo int) {
			defer wg.Done()
			// each goroutine gets it's own crypto context to make sure they are really parallel
			recoverSenders(logPrefix, secp256k1.ContextForThread(threadNo), config, jobs, out, quitCh)
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

	if err := db.Walk(dbutils.BlockBodyPrefix, dbutils.EncodeBlockNumber(s.BlockNumber+1), 0, func(k, v []byte) (bool, error) {
		if err := common.Stopped(quitCh); err != nil {
			return false, err
		}

		blockNumber := binary.BigEndian.Uint64(k[:8])
		blockHash := common.BytesToHash(k[8:])
		if blockNumber > to {
			return false, nil
		}

		if canonical[blockNumber-s.BlockNumber-1] != blockHash {
			// non-canonical case
			return true, nil
		}
		body := rawdb.ReadBody(db, blockHash, blockNumber)

		select {
		case err := <-errCh:
			if err != nil {
				return false, err
			}
		case jobs <- &senderRecoveryJob{body: body, key: k, blockNumber: blockNumber, index: int(blockNumber - s.BlockNumber - 1)}:
		}

		return true, nil
	}); err != nil {
		return fmt.Errorf("walking over the block bodies: %w", err)
	}

	close(jobs)
	wg.Wait()
	close(out)
	for err := range errCh {
		if err != nil {
			return err
		}
	}

	if err := collectorSenders.Load(logPrefix, db,
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

	return s.DoneAndUpdate(db, to)
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
		signer := types.MakeSigner(config, big.NewInt(int64(job.blockNumber)))
		job.senders = make([]byte, len(body.Transactions)*common.AddressLength)
		for i, tx := range body.Transactions {
			from, err := signer.SenderWithContext(cryptoContext, tx)
			if err != nil {
				job.err = fmt.Errorf("%s: error recovering sender for tx=%x, %w", logPrefix, tx.Hash(), err)
				break
			}
			if tx.Protected() && tx.ChainId().Cmp(signer.ChainID()) != 0 {
				job.err = fmt.Errorf("%s: invalid chainId, tx.Chain()=%d, igner.ChainID()=%d", logPrefix, tx.ChainId(), signer.ChainID())
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

func UnwindSendersStage(u *UnwindState, s *StageState, stateDB ethdb.Database) error {
	// Does not require any special processing
	mutation := stateDB.NewBatch()
	err := u.Done(mutation)
	logPrefix := s.state.LogPrefix()
	if err != nil {
		return fmt.Errorf("%s: reset: %v", logPrefix, err)
	}
	_, err = mutation.Commit()
	if err != nil {
		return fmt.Errorf("%s: failed to write db commit: %v", logPrefix, err)
	}
	return nil
}
