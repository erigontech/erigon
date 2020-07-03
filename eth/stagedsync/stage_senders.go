package stagedsync

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math/big"
	"os"
	"runtime"
	"runtime/pprof"
	"runtime/trace"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/crypto/secp256k1"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/params"
	"github.com/ledgerwatch/turbo-geth/rlp"
)

type Stage3Config struct {
	BatchSize       int
	BlockSize       int
	BufferSize      int
	StartTrace      bool
	Prof            bool
	ToProcess       int
	NumOfGoroutines int
	ReadChLen       int
	Now             time.Time
}

func SpawnRecoverSendersStage(cfg Stage3Config, s *StageState, stateDB ethdb.Database, config *params.ChainConfig, quitCh chan struct{}) error {
	if cfg.StartTrace {
		filePath := fmt.Sprintf("trace_%d_%d_%d.out", cfg.Now.Day(), cfg.Now.Hour(), cfg.Now.Minute())
		f1, err := os.Create(filePath)
		if err != nil {
			return err
		}
		err = trace.Start(f1)
		if err != nil {
			return err
		}
		defer func() {
			trace.Stop()
			f1.Close()
		}()
	}
	if cfg.Prof {
		f2, err := os.Create(fmt.Sprintf("cpu_%d_%d_%d.prof", cfg.Now.Day(), cfg.Now.Hour(), cfg.Now.Minute()))
		if err != nil {
			log.Error("could not create CPU profile", "error", err)
			return err
		}
		defer f2.Close()
		if err = pprof.StartCPUProfile(f2); err != nil {
			log.Error("could not start CPU profile", "error", err)
			return err
		}
	}
	if err := common.Stopped(quitCh); err != nil {
		return err
	}
	nextBlockNumber := s.BlockNumber
	toBlockNumber, _, err := stages.GetStageProgress(stateDB, stages.Bodies)
	if err != nil {
		return err
	}
	canonical := make([]common.Hash, toBlockNumber-s.BlockNumber)
	currentHeaderIdx := 0

	err = stateDB.Walk(dbutils.HeaderPrefix, dbutils.EncodeBlockNumber(s.BlockNumber+1), 0, func(k, v []byte) (bool, error) {
		if err = common.Stopped(quitCh); err != nil {
			return false, err
		}

		// Skip non relevant records
		if !dbutils.CheckCanonicalKey(k) {
			return true, nil
		}

		copy(canonical[currentHeaderIdx][:], v)
		currentHeaderIdx++
		return true, nil
	})

	jobs := make(chan *senderRecoveryJob, cfg.BatchSize)
	go func() {
		defer close(jobs)
		err = stateDB.Walk(dbutils.BlockBodyPrefix, dbutils.EncodeBlockNumber(s.BlockNumber+1), 0, func(k, v []byte) (bool, error) {
			if err = common.Stopped(quitCh); err != nil {
				return false, err
			}

			blockNumber := binary.BigEndian.Uint64(k[:8])
			blockHash := common.BytesToHash(k[8:])
			if canonical[blockNumber-s.BlockNumber-1] != blockHash {
				// non-canonical case
				return true, nil
			}

			data := make([]byte, len(v))
			copy(data, v)

			if cfg.Prof || cfg.StartTrace {
				if blockNumber == uint64(cfg.ToProcess) {
					// Flush the profiler
					pprof.StopCPUProfile()
					common.SafeClose(quitCh)
					return false, nil
				}
			}

			jobs <- &senderRecoveryJob{bodyRlp: common.CopyBytes(v), blockHash: blockHash, blockNumber: blockNumber}

			return true, nil
		})
		if err != nil {
			log.Error("walking over the block bodies", "error", err)
		}
	}()

	out := make(chan *senderRecoveryJob, cfg.BatchSize)
	wg := new(sync.WaitGroup)
	wg.Add(cfg.NumOfGoroutines)
	for i := 0; i < cfg.NumOfGoroutines; i++ {
		go func() {
			runtime.LockOSThread()
			defer func() {
				wg.Done()
				runtime.UnlockOSThread()
			}()

			// each goroutine gets it's own crypto context to make sure they are really parallel
			recoverSenders(secp256k1.NewContext(), config, jobs, out, quitCh)
		}()
	}
	log.Info("Sync (Senders): Started recoverer goroutines", "numOfGoroutines", cfg.NumOfGoroutines)
	go func() {
		wg.Wait()
		close(out)
	}()

	mutation := stateDB.NewBatch()
	defer func() {
		_, dbErr := mutation.Commit()
		if dbErr != nil {
			log.Error("Sync (Senders): failed to write db commit", "err", dbErr)
		}
	}()

	for j := range out {
		if j.err != nil {
			return err
		}
		if err := common.Stopped(quitCh); err != nil {
			return err
		}
		rawdb.WriteSenders(context.Background(), mutation, j.blockHash, j.blockNumber, j.froms)
		if err := s.Update(mutation, nextBlockNumber); err != nil {
			return err
		}
		log.Info("Recovered for blocks:", "blockNumber", nextBlockNumber)

		if mutation.BatchSize() >= mutation.IdealBatchSize() {
			if _, err := mutation.Commit(); err != nil {
				return err
			}
			mutation = stateDB.NewBatch()
		}
	}

	s.Done()
	return nil
}

type senderRecoveryJob struct {
	bodyRlp         rlp.RawValue
	blockHash       common.Hash
	blockNumber     uint64
	froms           []common.Address
	err             error
}

func recoverSenders(cryptoContext *secp256k1.Context, config *params.ChainConfig, in, out chan *senderRecoveryJob, quit chan struct{}) {
	for job := range in {
		if job == nil {
			return
		}
		body := new(types.Body)
		if err := rlp.Decode(bytes.NewReader(job.bodyRlp), body); err != nil {
			job.err = fmt.Errorf("invalid block body RLP: %w", err)
			out <- job
			return
		}
		signer := types.MakeSigner(config, big.NewInt(int64(job.blockNumber)))
		job.froms = make([]common.Address, len(body.Transactions))
		for i, tx := range body.Transactions {
			from, err := signer.SenderWithContext(cryptoContext, tx)
			if err != nil {
				job.err = errors.Wrap(err, fmt.Sprintf("error recovering sender for tx=%x\n", tx.Hash()))
				break
			}
			if tx.Protected() && tx.ChainID().Cmp(signer.ChainID()) != 0 {
				job.err = errors.New("invalid chainId")
				break
			}
			job.froms[i] = from
		}

		// prevent sending to close channel
		if err := common.Stopped(quit); err != nil {
			job.err = err
		}
		out <- job

		if job.err == common.ErrStopped {
			return
		}
	}
}

func unwindSendersStage(u *UnwindState, stateDB ethdb.Database) error {
	// Does not require any special processing
	mutation := stateDB.NewBatch()
	err := u.Done(mutation)
	if err != nil {
		return fmt.Errorf("unwind Senders: reset: %v", err)
	}
	_, err = mutation.Commit()
	if err != nil {
		return fmt.Errorf("unwind Senders: failed to write db commit: %v", err)
	}
	return nil
}
