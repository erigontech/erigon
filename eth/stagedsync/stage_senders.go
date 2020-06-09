package stagedsync

import (
	"context"
	"fmt"
	"math/big"
	"os"
	"runtime"
	"runtime/pprof"
	"sync"

	"github.com/pkg/errors"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/crypto/secp256k1"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/params"
)

var numOfGoroutines int
var cryptoContexts []*secp256k1.Context

func init() {
	// To avoid bothering with creating/releasing the resources
	// but still not leak the contexts
	numOfGoroutines = 3 // We never get more than 3x improvement even if we use 8 goroutines
	if numOfGoroutines > runtime.NumCPU() {
		numOfGoroutines = runtime.NumCPU()
	}
	cryptoContexts = make([]*secp256k1.Context, numOfGoroutines)
	for i := 0; i < numOfGoroutines; i++ {
		cryptoContexts[i] = secp256k1.NewContext()
	}
}

func spawnRecoverSendersStage(s *StageState, stateDB ethdb.Database, config *params.ChainConfig, quitCh chan struct{}) error {
	if prof {
		f, err := os.Create("cpu3.prof")
		if err != nil {
			log.Error("could not create CPU profile", "error", err)
			return err
		}
		defer f.Close()
		if err = pprof.StartCPUProfile(f); err != nil {
			log.Error("could not start CPU profile", "error", err)
			return err
		}
		defer pprof.StopCPUProfile()
	}

	lastProcessedBlockNumber := s.BlockNumber
	nextBlockNumber := lastProcessedBlockNumber + 1

	mutation := stateDB.NewBatch()
	defer func() {
		_, dbErr := mutation.Commit()
		if dbErr != nil {
			log.Error("Sync (Senders): failed to write db commit", "err", dbErr)
		}
	}()

	emptyHash := common.Hash{}
	var blockNumber big.Int

	const batchSize = 1000

	jobs := make(chan *senderRecoveryJob, batchSize)
	out := make(chan *senderRecoveryJob, batchSize)

	wg := &sync.WaitGroup{}
	wg.Add(numOfGoroutines)
	defer func() {
		close(jobs)
		wg.Wait()
		close(out)
	}()
	for i := 0; i < numOfGoroutines; i++ {
		// each goroutine gets it's own crypto context to make sure they are really parallel
		go recoverSenders(cryptoContexts[i], jobs, out, quitCh, wg)
	}
	log.Info("Sync (Senders): Started recoverer goroutines", "numOfGoroutines", numOfGoroutines)

	needExit := false
	for !needExit {
		if err := common.Stopped(quitCh); err != nil {
			return err
		}

		written := 0
		for i := 0; i < batchSize; i++ {
			hash := rawdb.ReadCanonicalHash(mutation, nextBlockNumber)
			if hash == emptyHash {
				needExit = true
				break
			}
			body := rawdb.ReadBody(mutation, hash, nextBlockNumber)
			if body == nil {
				needExit = true
				break
			}
			blockNumber.SetUint64(nextBlockNumber)
			s := types.MakeSigner(config, &blockNumber)

			jobs <- &senderRecoveryJob{s, body, hash, nextBlockNumber, nil}
			written++

			nextBlockNumber++
		}

		for i := 0; i < written; i++ {
			j := <-out
			if j.err != nil {
				return errors.Wrap(j.err, "could not extract senders")
			}
			rawdb.WriteBody(context.Background(), mutation, j.hash, j.nextBlockNumber, j.blockBody)
		}

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
	signer          types.Signer
	blockBody       *types.Body
	hash            common.Hash
	nextBlockNumber uint64
	err             error
}

func recoverSenders(cryptoContext *secp256k1.Context, in chan *senderRecoveryJob, out chan *senderRecoveryJob, quit chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()

	for job := range in {
		if job == nil {
			return
		}
		for _, tx := range job.blockBody.Transactions {
			from, err := job.signer.SenderWithContext(cryptoContext, tx)
			if err != nil {
				job.err = errors.Wrap(err, fmt.Sprintf("error recovering sender for tx=%x\n", tx.Hash()))
				break
			}
			tx.SetFrom(from)
			if tx.Protected() && tx.ChainID().Cmp(job.signer.ChainID()) != 0 {
				job.err = errors.New("invalid chainId")
				break
			}
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
