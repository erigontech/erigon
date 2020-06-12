package stagedsync

import (
	"context"
	"fmt"
	"io"
	"math/big"
	"os"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

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

	lastProcessedBlockNumber := s.BlockNumber
	nextBlockNumber := lastProcessedBlockNumber + 1

	mutation := &mutationSafe{mutation: stateDB.NewBatch()}
	defer func() {
		if dbErr := mutation.Commit(); dbErr != nil {
			log.Error("Sync (Senders): failed to write db commit", "err", dbErr)
		}
	}()

	blockNumber := big.NewInt(0)

	const batchSize = 5000

	jobs := make(chan *senderRecoveryJob, 20*batchSize)
	out := make(chan TxsFroms, batchSize)

	wg := &sync.WaitGroup{}
	numOfGoroutines := numOfGoroutines

	numOfGoroutines = 32
	ctxLength := len(cryptoContexts)
	if ctxLength < numOfGoroutines {
		for i := 0; i < numOfGoroutines-ctxLength; i++ {
			cryptoContexts = append(cryptoContexts, secp256k1.NewContext())
		}
	}

	fmt.Println("=================", ctxLength, numOfGoroutines)

	wg.Add(numOfGoroutines)
	for i := 0; i < numOfGoroutines; i++ {
		// each goroutine gets it's own crypto context to make sure they are really parallel
		ctx := cryptoContexts[i]
		go recoverSenders(ctx, jobs, out, quitCh, wg)
	}
	log.Info("Sync (Senders): Started recoverer goroutines", "numOfGoroutines", numOfGoroutines)

	firstBlock := new(uint64)

	errCh := make(chan error)
	doneCh := make(chan struct{}, 1)
	go func() {
		defer func() {
			close(jobs)
			wg.Wait()
			close(doneCh)
			close(errCh)
		}()

		for {
			if err := common.Stopped(quitCh); err != nil {
				errCh <- err
				return
			}

			job := getBlockBody(mutation, config, blockNumber, nextBlockNumber)
			if job == nil {
				break
			}

			if atomic.LoadUint64(firstBlock) == 0 {
				atomic.StoreUint64(firstBlock, job.nextBlockNumber)
			}

			jobs <- job

			atomic.AddUint64(&nextBlockNumber, 1)
		}
	}()

	fmt.Println("DONE?")
	now := time.Now()


	f, err := os.Create(fmt.Sprintf("/mnt/sdb/turbo-geth/froms_%d_%d_%d.out", now.Day(), now.Hour(), now.Minute()))
	if err != nil {
		return err
	}

	buf := NewAddressBuffer(f, (4096 * 10 / 20) * 100000)
	defer buf.Close()

	fmt.Println("Storing into a file")
	err = writeOnDiskBatch(buf, firstBlock, out, quitCh, jobs, doneCh)
	fmt.Println("Storing into a file - DONE")

	if err != nil {
		return err
	}

	err = <-errCh
	if err != nil {
		return err
	}

	fmt.Println("DONE!")

	errCh = make(chan error)
	nextBlockNumber = lastProcessedBlockNumber + 1
	blockNumber = big.NewInt(0)
	writeBatchFromDisk(buf, s, stateDB, config, mutation, errCh, quitCh, blockNumber, nextBlockNumber)

	err = <-errCh
	fmt.Println("DONE!")
	if err != nil {
		return err
	}

	s.Done()
	fmt.Println("DONE!!!")
	panic("DONE!!!")
	return nil
}

func getBlockBody(mutation *mutationSafe, config *params.ChainConfig, blockNumber *big.Int, nextBlockNumber uint64) *senderRecoveryJob {
	hash := rawdb.ReadCanonicalHash(mutation, nextBlockNumber)
	if hash.IsEmpty() {
		return nil
	}

	body := rawdb.ReadBody(mutation, hash, nextBlockNumber)
	if body == nil {
		return nil
	}

	blockNumber.SetUint64(nextBlockNumber)
	s := types.MakeSigner(config, blockNumber)

	return &senderRecoveryJob{s, body, hash, nextBlockNumber, nil}
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

type TxsFroms struct {
	blockNumber uint64
	froms       []common.Address
	err         error
}

func writeOnDiskBatch(buf *AddressBuffer, firstBlock *uint64, out chan TxsFroms, quitCh chan struct{}, in chan *senderRecoveryJob, doneCh chan struct{}) error {
	n := 0

	defer func() {
		buf.Write()
	}()

	toSort := uint64(10)
	buffer := make([]TxsFroms, 0, 1000)
	var writeFroms []TxsFroms

	total := 0
	totalFroms := 0
	written := 0
	var err error
	defer func() {
		// store last blocks
		sort.SliceStable(buffer, func(i, j int) bool {
			return buffer[i].blockNumber < buffer[j].blockNumber
		})

		for _, job := range buffer {
			totalFroms += len(job.froms)
			for i := range job.froms {
				buf.buf = append(buf.buf, job.froms[i][:]...)
			}
			written, err = buf.Write()
			if err != nil {
				panic(err)
			}
			total += written
		}
	}()

	fmt.Println("xxx writeOnDiskBatch")

	isFirst := true
	currentBlock := uint64(0)
	for j := range out {
		if isFirst {
			currentBlock = atomic.LoadUint64(firstBlock)
			isFirst = false
		}

		if j.err != nil {
			return err
		}
		if err := common.Stopped(quitCh); err != nil {
			return err
		}
		if err := common.Stopped(doneCh); err != nil {
			return nil
		}

		if j.blockNumber%10000 == 0 {
			log.Info("Dumped on a disk:", "blockNumber", j.blockNumber, "out", len(out), "in", len(in), "toNextWrite", buf.Len(), "written", total, "txs", totalFroms, "bufLen", len(buffer), "bufCap", cap(buffer), "toWriteLen", buf.Len(), "toWriteCap", buf.Cap())
		}

		if j.err != nil {
			return errors.Wrap(j.err, "could not extract senders")
		}

		buffer = append(buffer, j)
		sort.SliceStable(buffer, func(i, j int) bool {
			return buffer[i].blockNumber < buffer[j].blockNumber
		})

		// check if we have 10 sequential blocks
		hasRow := true
		if uint64(len(buffer)) < toSort {
			hasRow = false
		} else {
			for i := range buffer {
				if uint64(i) > toSort {
					break
				}
				if buffer[i].blockNumber != currentBlock+uint64(i) {
					hasRow = false
					break
				}
			}
		}
		if !hasRow {
			continue
		}

		currentBlock += toSort
		writeFroms = buffer[:toSort]
		buffer = buffer[toSort:]

		for _, jobToWrite := range writeFroms {
			totalFroms += len(jobToWrite.froms)
			for i := range jobToWrite.froms {
				n++
				buf.Add(jobToWrite.froms[i][:])
				if 20*n == buf.size {
					written, err = buf.Write()
					if err != nil {
						return err
					}
					total += written

					n = 0
					buf.Reset()
				}
			}
		}
	}

	return nil
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

type AddressBuffer struct {
	buf []byte
	size int
	io.ReadWriteCloser
}

func NewAddressBuffer(f io.ReadWriteCloser, size int) *AddressBuffer {
	buf := make([]byte, size*len(common.Address{}))

	return &AddressBuffer{
		buf, size, f,
	}
}

func (a *AddressBuffer) Write() (int, error) {
	if len(a.buf) > 0 {
		return a.ReadWriteCloser.Write(a.buf)
	}
	return 0, nil
}

func (a *AddressBuffer) Read() (int, error) {
	return a.ReadWriteCloser.Read(a.buf)
}

func (a *AddressBuffer) Add(b []byte)  {
	a.buf = append(a.buf, b...)
}

func (a *AddressBuffer) Reset() {
	a.buf = a.buf[:0]
}

func (a *AddressBuffer) Len() int {
	return len(a.buf)
}

func (a *AddressBuffer) Cap() int {
	return cap(a.buf)
}

func writeBatchFromDisk(a *AddressBuffer, s *StageState,
	stateDB ethdb.Database, config *params.ChainConfig,
	mutation *mutationSafe,
	errCh chan error, quitCh chan struct{},
	blockNumber *big.Int, nextBlockNumber uint64,
) {
	defer close(errCh)

	for {
		n, err := a.Read()
		if err != nil && err != io.EOF {
			errCh <- err
			return
		}
		if n%len(common.Address{}) != 0 {
			errCh <- errors.New("got invalid address length")
			return
		}

		// insert for
		job := getBlockBody(mutation, config, blockNumber, nextBlockNumber)
		if job == nil {
			break
		}
		nextBlockNumber++

		for i := range job.blockBody.Transactions {
			from := common.Address{}
			from.SetBytes(a.buf[i : (i+1)*20])
			job.blockBody.Transactions[i].SetFrom(from)
		}

		rawdb.WriteBody(context.Background(), mutation, job.hash, job.nextBlockNumber, job.blockBody)

		if mutation.BatchSize() >= mutation.IdealBatchSize() {
			if err := s.Update(mutation, nextBlockNumber); err != nil {
				errCh <- err
				return
			}

			log.Info("Recovered for blocks:", "blockNumber", nextBlockNumber)

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

func recoverSenders(cryptoContext *secp256k1.Context, in chan *senderRecoveryJob, out chan TxsFroms, quit chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()

	fmt.Println("recoverSenders started")

	for job := range in {
		if job == nil {
			return
		}

		res := TxsFroms{blockNumber: job.nextBlockNumber}
		froms, err := recoverFrom(cryptoContext, job.blockBody, job.signer)
		if err != nil {
			res.err = err
		} else {
			res.froms = froms
		}

		// prevent sending to close channel
		if err := common.Stopped(quit); err != nil {
			res.err = err
		}

		if res.err == common.ErrStopped {
			return
		}

		out <- res
	}
}

func recoverFrom(cryptoContext *secp256k1.Context, blockBody *types.Body, signer types.Signer) ([]common.Address, error) {
	froms := make([]common.Address, len(blockBody.Transactions))
	for i, tx := range blockBody.Transactions {
		if tx.Protected() && tx.ChainID().Cmp(signer.ChainID()) != 0 {
			return nil, errors.New("invalid chainId")
		}

		from, err := signer.SenderWithContext(cryptoContext, tx)
		if err != nil {
			return nil, errors.Wrap(err, fmt.Sprintf("error recovering sender for tx=%x\n", tx.Hash()))
		}
		froms[i] = from
	}
	return froms, nil
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
