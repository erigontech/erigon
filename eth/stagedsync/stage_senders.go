package stagedsync

import (
	"context"
	"fmt"
	"io"
	"math/big"
	"os"
	"runtime"
	"runtime/pprof"
	"runtime/trace"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/crypto/secp256k1"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/params"
	"github.com/ledgerwatch/turbo-geth/rlp"
)

type stage3Config struct {
	batchSize       int
	blockSize       int
	bufferSize      int
	startTrace      bool
	prof            bool
	toProcess       int
	numOfGoroutines int
	readChLen       int
	now             time.Time
}

func spawnRecoverSendersStage(cfg stage3Config, s *StageState, stateDB ethdb.Database, config *params.ChainConfig, datadir string, quitCh chan struct{}) error {
	if cfg.startTrace {
		filePath := fmt.Sprintf("/mnt/sdb/trace_%d_%d_%d.out", cfg.now.Day(), cfg.now.Hour(), cfg.now.Minute())
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
	if cfg.prof {
		f2, err := os.Create(fmt.Sprintf("/mnt/sdb/cpu_%d_%d_%d.prof", cfg.now.Day(), cfg.now.Hour(), cfg.now.Minute()))
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

	lastProcessedBlockNumber := s.BlockNumber
	nextBlockNumber := new(uint64)
	*nextBlockNumber = lastProcessedBlockNumber

	mutation := &mutationSafe{mutation: stateDB.NewBatch()}
	defer func() {
		if dbErr := mutation.Commit(); dbErr != nil {
			log.Error("Sync (Senders): failed to write db commit", "err", dbErr)
		}
	}()

	firstBlockToProceed := lastProcessedBlockNumber+1

	onlySecondStage := false
	var filePath string
	if !onlySecondStage {
		fmt.Println("START 3.1")

		jobs := make(chan *senderRecoveryJob, cfg.numOfGoroutines*cfg.batchSize)
		out := make(chan TxsFroms, cfg.batchSize)

		wg := &sync.WaitGroup{}
		wg.Add(cfg.numOfGoroutines)
		for i := 0; i < cfg.numOfGoroutines; i++ {
			go func() {
				runtime.LockOSThread()
				defer runtime.UnlockOSThread()
				// each goroutine gets it's own crypto context to make sure they are really parallel
				recoverSenders(secp256k1.NewContext(), config, jobs, out, quitCh, wg)
			}()
		}
		log.Info("Sync (Senders): Started recoverer goroutines", "numOfGoroutines", cfg.numOfGoroutines)

		errCh := make(chan error, cfg.readChLen)
		doneCh := make(chan struct{})
		for i := 0; i < cfg.readChLen; i++ {
			go func() {
				defer common.SafeClose(doneCh)
				for {
					nextNumber := atomic.AddUint64(nextBlockNumber, 1)

					if err := common.Stopped(quitCh); err != nil {
						errCh <- err
						return
					}

					job := getBlockTxs(mutation, nextNumber)
					if job == nil {
						break
					}

					if cfg.prof || cfg.startTrace {
						if nextNumber == uint64(cfg.toProcess) {
							// Flush the profiler
							pprof.StopCPUProfile()
							common.SafeClose(quitCh)
							break
						}
					}

					jobs <- job
				}
			}()
		}

		fmt.Println("DONE?")

		filePath := fmt.Sprintf("/home/eugene/eth/froms_%d_%d_%d.out", cfg.now.Day(), cfg.now.Hour(), cfg.now.Minute())
		f, err := os.Create(filePath)
		if err != nil {
			return err
		}

		buf := NewAddressBuffer(f, cfg.bufferSize, true)

		fmt.Println("Storing into a file")
		err = writeOnDiskBatch(cfg, buf, firstBlockToProceed, out, quitCh, jobs, doneCh)
		fmt.Println("Storing into a file - DONE")

		if err != nil {
			buf.Close()

			// fixme simplify
			close(jobs)
			wg.Wait()
			close(errCh)
			close(out)

			return err
		}

		// fixme simplify
		<-doneCh
		fmt.Println("reading bodies is finished")

		close(jobs)
		wg.Wait()
		close(errCh)
		close(out)

		err = <-errCh
		buf.Close()
		if err != nil {
			return err
		}
		fmt.Println("DONE!")
	}

	fmt.Println("START 3.2")
	if onlySecondStage {
		filePath = "/mnt/sdb/turbo-geth/froms_13_0_17.out"
	}
	err := recoverSendersFromDisk(cfg, s, stateDB, config, mutation, quitCh, firstBlockToProceed, filePath)

	fmt.Println("DONE!")
	if err != nil && err != io.EOF {
		return err
	}

	s.Done()
	fmt.Println("DONE!!!")
	panic("DONE!!!")
	return nil
}

func recoverSendersFromDisk(cfg stage3Config, s *StageState, stateDB ethdb.Database, config *params.ChainConfig, mutation *mutationSafe, quitCh chan struct{}, lastProcessedBlockNumber uint64, filePath string) error {
	f, err := os.OpenFile(filePath, os.O_RDONLY, 0664)
	if err != nil {
		return err
	}

	buf := NewAddressBuffer(f, cfg.bufferSize, false)
	defer buf.Close()

	return writeBatchFromDisk(buf, s, stateDB, config, mutation, quitCh, lastProcessedBlockNumber)
}

type blockData struct {
	body   *types.Body
	hash   common.Hash
	number uint64
}

func getBlockBody(mutation *mutationSafe, nextBlockNumber uint64) *blockData {
	hash := rawdb.ReadCanonicalHash(mutation, nextBlockNumber)
	if hash.IsEmpty() {
		return nil
	}

	body := rawdb.ReadBody(mutation, hash, nextBlockNumber)
	if body == nil {
		return nil
	}

	return &blockData{body, hash, nextBlockNumber}
}

func getBlockTxs(mutation *mutationSafe, nextBlockNumber uint64) *senderRecoveryJob {
	hash := rawdb.ReadCanonicalHash(mutation, nextBlockNumber)
	if hash.IsEmpty() {
		return nil
	}
	v := rawdb.ReadBodyRLP(mutation, hash, nextBlockNumber)

	return &senderRecoveryJob{v, hash, nextBlockNumber, nil}
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

func writeOnDiskBatch(cfg stage3Config, buf *AddressBuffer, firstBlock uint64, out chan TxsFroms, quitCh chan struct{}, in chan *senderRecoveryJob, doneCh chan struct{}) error {
	n := 0

	toSort := uint64(cfg.numOfGoroutines * cfg.batchSize)
	buffer := make([]TxsFroms, 0, 2*toSort)
	var writeFroms []TxsFroms

	total := 0
	totalFroms := 0
	written := 0
	var err error
	m := &runtime.MemStats{}

	defer func() {
		fmt.Println("+++Block END+++")
		// store last blocks
		sort.Slice(buffer, func(i, j int) bool {
			return buffer[i].blockNumber < buffer[j].blockNumber
		})

		for _, job := range buffer {
			totalFroms += len(job.froms)
			for i := range job.froms {
				buf.Add(job.froms[i][:])
			}
		}

		written, err = buf.Write()
		if err != nil {
			panic(err)
		}
		total += written
	}()

	fmt.Println("xxx writeOnDiskBatch")

	currentBlock := firstBlock

	for j := range out {
		if j.err != nil {
			return err
		}
		if err := common.Stopped(quitCh); err != nil {
			return err
		}
		if err := common.Stopped(doneCh); err != nil {
			return nil
		}

		if j.blockNumber%uint64(cfg.batchSize) == 0 {
			runtime.ReadMemStats(m)
			log.Info("Dumped on a disk:", "blockNumber", j.blockNumber, "out", len(out), "in", len(in), "written", total, "txs", totalFroms, "bufLen", len(buffer), "bufCap", cap(buffer), "toWriteLen", buf.Len(), "toWriteCap", buf.Cap(),
				"alloc", int(m.Alloc/1024), "sys", int(m.Sys/1024), "numGC", int(m.NumGC))
		}

		if j.err != nil {
			return errors.Wrap(j.err, "could not extract senders")
		}

		buffer = append(buffer, j)

		if j.blockNumber%(toSort/2) == 0 {
			sort.Slice(buffer, func(i, j int) bool {
				return buffer[i].blockNumber < buffer[j].blockNumber
			})

			// check if we have toSort sequential blocks
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
					if 20*n >= buf.size {
						written, err = buf.Write()
						if err != nil {
							return err
						}
						total += written
						n = 0
					}
				}
			}
		}
	}

	return nil
}

type AddressBuffer struct {
	buf        []byte
	size       int
	currentIdx int
	isOpen     bool
	io.ReadWriteCloser
	sync.RWMutex
}

func NewAddressBuffer(f io.ReadWriteCloser, size int, fullLength bool) *AddressBuffer {
	length := size * len(common.Address{})
	var buf []byte
	if fullLength {
		fmt.Println("STARTED BUF=1", size, fullLength, len(buf), cap(buf))
		buf = make([]byte, 0, length)
		fmt.Println("STARTED BUF=2", size, fullLength, len(buf), cap(buf))
		buf = buf[0:0:length]
		fmt.Println("STARTED BUF=3", size, fullLength, len(buf), cap(buf))
	} else {
		fmt.Println("STARTED BUF=1", size, fullLength, len(buf), cap(buf))
		buf = make([]byte, length)
		fmt.Println("STARTED BUF=2", size, fullLength, len(buf), cap(buf))
	}

	fmt.Println("STARTED BUF=XXX", size, fullLength, len(buf), cap(buf))

	return &AddressBuffer{
		buf, size, -1, true, f, sync.RWMutex{},
	}
}

func (a *AddressBuffer) Write() (int, error) {
	a.Lock()
	defer a.Unlock()

	if !a.isOpen {
		return 0, nil
	}
	if len(a.buf) > 0 {
		n, err := a.ReadWriteCloser.Write(a.buf)
		if err != nil {
			return 0, err
		}

		a.Reset()
		return n, nil
	}
	return 0, nil
}

func (a *AddressBuffer) Read() (int, error) {
	a.RLock()
	defer a.RUnlock()

	if !a.isOpen {
		return 0, nil
	}
	return a.ReadWriteCloser.Read(a.buf)
}

func (a *AddressBuffer) Add(b []byte) {
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

func (a *AddressBuffer) Close() error {
	fmt.Println("AddressBuffer Close")
	a.Lock()
	defer a.Unlock()

	if !a.isOpen {
		return nil
	}
	err := a.ReadWriteCloser.Close()
	if err != nil {
		return err
	}
	a.isOpen = false
	return nil
}

func (a *AddressBuffer) Next() (common.Address, error) {
	if !a.isOpen {
		return common.Address{}, nil
	}

	if (a.currentIdx+2)*20 > len(a.buf) {
		a.currentIdx = -1
	}

	if a.currentIdx == -1 {
		n, err := a.Read()
		if err != nil {
			return common.Address{}, err
		}
		if n%len(common.Address{}) != 0 {
			return common.Address{}, errors.New("got invalid address length")
		}
		if n == 0 {
			return common.Address{}, io.EOF
		}
	}

	a.currentIdx++

	var addr common.Address
	addr.SetBytes(a.buf[a.currentIdx*20 : (a.currentIdx+1)*20])

	return addr, nil
}

func writeBatchFromDisk(buf *AddressBuffer, s *StageState,
	stateDB ethdb.Database, config *params.ChainConfig,
	mutation *mutationSafe,
	quitCh chan struct{},
	lastBlockNumber uint64,
) error {

	var err error
	var addr common.Address
	blockNumber := big.NewInt(0)
	nextBlockNumber := lastBlockNumber + 1
	m := &runtime.MemStats{}

	for {
		// insert for
		job := getBlockBody(mutation, nextBlockNumber)
		if job == nil {
			fmt.Println("111 1", blockNumber.String(), nextBlockNumber)
			break
		}
		nextBlockNumber++

		for i := range job.body.Transactions {
			addr, err = buf.Next()
			if err != nil {
				fmt.Println("111 2", err)
				return err
			}

			job.body.Transactions[i].SetFrom(addr)
		}

		rawdb.WriteBody(context.Background(), mutation, job.hash, job.number, job.body)

		if mutation.BatchSize() >= mutation.IdealBatchSize() {
			if err := s.Update(mutation, nextBlockNumber); err != nil {
				fmt.Println("111 3", err)
				return err
			}

			runtime.ReadMemStats(m)
			log.Info("Recovered for blocks:", "blockNumber", nextBlockNumber, "alloc", int(m.Alloc/1024), "sys", int(m.Sys/1024), "numGC", int(m.NumGC))

			if err := mutation.Commit(); err != nil {
				fmt.Println("111 4", err)
				return err
			}

			mutation.Set(stateDB.NewBatch())
		}
	}

	return nil
}

type senderRecoveryJob struct {
	blockTxs        rlp.RawValue
	hash            common.Hash
	nextBlockNumber uint64
	err             error
}

func recoverSenders(cryptoContext *secp256k1.Context, config *params.ChainConfig, in chan *senderRecoveryJob, out chan TxsFroms, quit chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()

	fmt.Println("recoverSenders started")

	for job := range in {
		if job == nil {
			return
		}

		txs := rawdb.DecodeBlockTxs(job.blockTxs)
		if txs == nil {
			return
		}
		s := types.MakeSigner(config, big.NewInt(int64(job.nextBlockNumber)))

		res := TxsFroms{blockNumber: job.nextBlockNumber}
		froms, err := recoverFrom(cryptoContext, txs, s)
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

func recoverFrom(cryptoContext *secp256k1.Context, blockTxs []*types.Transaction, signer types.Signer) ([]common.Address, error) {
	froms := make([]common.Address, len(blockTxs))
	for i, tx := range blockTxs {
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
