package stagedsync

import (
	"context"
	"fmt"
	"math/big"
	"runtime"
	"sync"

	"github.com/pkg/errors"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/crypto/secp256k1"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
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

func spawnRecoverSendersStage(s *StageState, stateDB ethdb.Database, config *params.ChainConfig, datadir string, quitCh chan struct{}) error {
	if err := common.Stopped(quitCh); err != nil {
		return err
	}

	/*
		err := etl.Transform(
			stateDB,
			dbutils.PlainStateBucket,
			dbutils.CurrentStateBucket,
			datadir,
			keyTransformExtractFunc(transformPlainStateKey),
			etl.IdentityLoadFunc,
			etl.TransformArgs{Quit: quitCh},
		)

		if err != nil {
			return err
		}
	*/

	lastProcessedBlockNumber := s.BlockNumber
	nextBlockNumber := lastProcessedBlockNumber + 1

	mutation := &mutationSafe{mutation: stateDB.NewBatch()}
	defer func() {
		if dbErr := mutation.Commit(); dbErr != nil {
			log.Error("Sync (Senders): failed to write db commit", "err", dbErr)
		}
	}()

	emptyHash := common.Hash{}
	blockNumber := big.NewInt(0)

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
		ctx := cryptoContexts[i]
		go recoverSenders(ctx, jobs, out, quitCh, wg)
	}
	log.Info("Sync (Senders): Started recoverer goroutines", "numOfGoroutines", numOfGoroutines)

	errCh := make(chan error)
	go writeBatch(s, stateDB, out, mutation, errCh, quitCh, jobs)

	for {
		if err := common.Stopped(quitCh); err != nil {
			return err
		}

		hash := rawdb.ReadCanonicalHash(mutation, nextBlockNumber)
		if hash == emptyHash {
			break
		}
		body := rawdb.ReadBody(mutation, hash, nextBlockNumber)
		if body == nil {
			break
		}
		blockNumber.SetUint64(nextBlockNumber)
		s := types.MakeSigner(config, blockNumber)

		jobs <- &senderRecoveryJob{s, body, hash, nextBlockNumber, nil}

		nextBlockNumber++
	}

	fmt.Println("DONE?")

	err := <-errCh
	fmt.Println("DONE!")
	if err != nil {
		return err
	}
	s.Done()
	fmt.Println("DONE!!!")
	return nil
}

type mutationSafe struct {
	mutation ethdb.DbWithPendingMutations
	sync.RWMutex
}

func (m *mutationSafe) Has(bucket, key []byte) (bool, error) {
	m.RLock()
	defer m.RUnlock()
	return m.mutation.Has(bucket, key)
}
func (m *mutationSafe) Get(bucket, key []byte) ([]byte, error) {
	m.RLock()
	defer m.RUnlock()
	return m.mutation.Get(bucket, key)
}
func (m *mutationSafe) Put(bucket, key []byte, value []byte) error {
	m.RLock()
	defer m.RUnlock()
	return m.mutation.Put(bucket, key, value)
}
func (m *mutationSafe) Delete(bucket, key []byte) error {
	m.RLock()
	defer m.RUnlock()
	return m.mutation.Delete(bucket, key)
}
func (m *mutationSafe) Commit() error {
	m.RLock()
	defer m.RUnlock()
	_, err := m.mutation.Commit()
	return err
}
func (m *mutationSafe) BatchSize() int {
	m.RLock()
	defer m.RUnlock()
	return m.mutation.BatchSize()
}
func (m *mutationSafe) IdealBatchSize() int {
	m.RLock()
	defer m.RUnlock()
	return m.mutation.IdealBatchSize()
}
func (m *mutationSafe) Set(mutation ethdb.DbWithPendingMutations) {
	m.Lock()
	m.mutation = mutation
	m.Unlock()
}

func writeBatch(s *StageState, stateDB ethdb.Database, out chan *senderRecoveryJob, mutation *mutationSafe, errCh chan error, quitCh chan struct{}, in chan *senderRecoveryJob) {
	var nextBlockNumber uint64
	defer close(errCh)

	for j := range out {
		if err := common.Stopped(quitCh); err != nil {
			errCh <- err
			return
		}

		if j.err != nil {
			errCh <- errors.Wrap(j.err, "could not extract senders")
			return
		}

		if j.nextBlockNumber > nextBlockNumber {
			nextBlockNumber = j.nextBlockNumber
		}

		rawdb.WriteBody(context.Background(), mutation, j.hash, j.nextBlockNumber, j.blockBody)

		if mutation.BatchSize() >= mutation.IdealBatchSize() {
			if err := s.Update(mutation, nextBlockNumber); err != nil {
				errCh <- err
				return
			}

			log.Info("Recovered for blocks:", "blockNumber", nextBlockNumber, "out", len(out), "in", len(in))

			if err := mutation.Commit(); err != nil {
				errCh <- err
				return
			}

			mutation.Set(stateDB.NewBatch())
		}
	}

	return
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

func unwindSendersStage(stateDB ethdb.Database, unwindPoint uint64) error {
	// Does not require any special processing
	lastProcessedBlockNumber, err := stages.GetStageProgress(stateDB, stages.Senders)
	if err != nil {
		return fmt.Errorf("unwind Senders: get stage progress: %v", err)
	}
	if unwindPoint >= lastProcessedBlockNumber {
		err = stages.SaveStageUnwind(stateDB, stages.Senders, 0)
		if err != nil {
			return fmt.Errorf("unwind Senders: reset: %v", err)
		}
		return nil
	}
	mutation := stateDB.NewBatch()
	err = stages.SaveStageUnwind(mutation, stages.Senders, 0)
	if err != nil {
		return fmt.Errorf("unwind Senders: reset: %v", err)
	}
	_, err = mutation.Commit()
	if err != nil {
		return fmt.Errorf("unwind Senders: failed to write db commit: %v", err)
	}
	return nil
}
