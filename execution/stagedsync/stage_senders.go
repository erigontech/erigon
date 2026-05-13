// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package stagedsync

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/erigontech/secp256k1"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/etl"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbutils"
	"github.com/erigontech/erigon/db/kv/prune"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/services"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/exec"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/stagedsync/headerdownload"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/node/ethconfig"
)

type SendersCfg struct {
	batchSize       int
	numOfGoroutines int
	badBlockHalt    bool
	tmpdir          string
	chainConfig     *chain.Config
	hd              *headerdownload.HeaderDownload
	blockReader     services.FullBlockReader
}

func StageSendersCfg(chainCfg *chain.Config, syncCfg ethconfig.Sync, badBlockHalt bool, tmpdir string, prune prune.Mode, blockReader services.FullBlockReader, hd *headerdownload.HeaderDownload) SendersCfg {
	const sendersBatchSize = 1000
	return SendersCfg{
		batchSize:       sendersBatchSize,
		numOfGoroutines: secp256k1.NumOfContexts(), // we can only be as parallels as our crypto library supports,
		badBlockHalt:    badBlockHalt,
		tmpdir:          tmpdir,
		chainConfig:     chainCfg,
		hd:              hd,
		blockReader:     blockReader,
	}
}

func SpawnRecoverSendersStage(cfg SendersCfg, s *StageState, u Unwinder, tx kv.RwTx, toBlock uint64, ctx context.Context, logger log.Logger) error {
	if s.BlockNumber < cfg.blockReader.FrozenBlocks() {
		s.BlockNumber = cfg.blockReader.FrozenBlocks()
	}

	quitCh := ctx.Done()

	prevStageProgress, errStart := stages.GetStageProgress(tx, stages.Bodies)
	if errStart != nil {
		return errStart
	}

	var to = prevStageProgress
	if toBlock > 0 {
		to = min(prevStageProgress, toBlock)
	}
	if to < s.BlockNumber {
		return nil
	}
	logPrefix := s.LogPrefix()
	if to > s.BlockNumber+16 {
		logger.Info(fmt.Sprintf("[%s] Started", logPrefix), "from", s.BlockNumber, "to", to)
	}

	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()

	startFrom := s.BlockNumber + 1
	recoveryStart := time.Now()

	jobs := make(chan *senderRecoveryJob, cfg.batchSize)
	out := make(chan *senderRecoveryJob, cfg.batchSize)
	wg := new(sync.WaitGroup)
	wg.Add(cfg.numOfGoroutines)
	ctx, cancelWorkers := context.WithCancel(context.Background())
	defer cancelWorkers()
	for i := 0; i < cfg.numOfGoroutines; i++ {
		go func(threadNo int) {
			defer dbg.LogPanic()
			defer wg.Done()
			// each goroutine gets it's own crypto context to make sure they are really parallel
			recoverSenders(ctx, secp256k1.ContextForThread(threadNo), cfg.chainConfig, jobs, out, quitCh)
		}(i)
	}

	collectorSenders := etl.NewCollectorWithAllocator(logPrefix, cfg.tmpdir, etl.SmallSortableBuffers, logger)
	defer collectorSenders.Close()
	collectorSenders.SortAndFlushInBackground(false)
	collectorSenders.LogLvl(log.LvlDebug)

	// pendingBlocks tracks blocks that are being processed
	// key: blockIndex, value: {senders slice, txCount, received count}
	type pendingBlock struct {
		senders  []byte
		txCount  int
		received int
		hash     common.Hash
	}
	pendingBlocks := make(map[int]*pendingBlock)
	var pendingMu sync.Mutex
	var lastBlockIndex int

	errCh := make(chan senderRecoveryError)
	go func() {
		defer dbg.LogPanic()
		defer close(errCh)
		defer cancelWorkers()
		var ok bool
		var j *senderRecoveryJob
		for {
			select {
			case <-quitCh:
				return
			case j, ok = <-out:
				if !ok {
					return
				}
				if j.err != nil {
					errCh <- senderRecoveryError{err: j.err, blockNumber: j.blockNumber, blockHash: j.blockHash}
					return
				}

				pendingMu.Lock()
				pb := pendingBlocks[j.blockIndex]
				if pb == nil {
					pendingMu.Unlock()
					errCh <- senderRecoveryError{err: fmt.Errorf("unexpected block index %d", j.blockIndex)}
					return
				}
				// Copy sender address to the correct position
				fromValue := j.from.Value()
				copy(pb.senders[j.txIndex*length.Addr:], fromValue[:])
				pb.received++

				// Check if block is complete
				if pb.received == pb.txCount {
					exec.AddSendersToGlobalReadAheader(pb.senders, pb.hash)
					if err := collectorSenders.Collect(dbutils.BlockBodyKey(s.BlockNumber+uint64(j.blockIndex)+1, pb.hash), pb.senders); err != nil {
						pendingMu.Unlock()
						errCh <- senderRecoveryError{err: err}
						return
					}
					delete(pendingBlocks, j.blockIndex)
					if j.blockIndex > lastBlockIndex {
						lastBlockIndex = j.blockIndex
					}
				}
				pendingMu.Unlock()
			}
		}
	}()

	var minBlockNum uint64 = math.MaxUint64
	var minBlockHash common.Hash
	var minBlockErr error
	handleRecoverErr := func(recErr senderRecoveryError) error {
		if recErr.blockHash == (common.Hash{}) {
			return recErr.err
		}

		if recErr.blockNumber < minBlockNum {
			minBlockNum = recErr.blockNumber
			minBlockHash = recErr.blockHash
			minBlockErr = recErr.err
		}
		return nil
	}

	bodiesC, err := tx.Cursor(kv.HeaderCanonical)
	if err != nil {
		return err
	}
	defer bodiesC.Close()

Loop:
	for k, v, err := bodiesC.Seek(hexutil.EncodeTs(startFrom)); k != nil; k, v, err = bodiesC.Next() {
		if err != nil {
			return err
		}
		if err := common.Stopped(quitCh); err != nil {
			return err
		}

		blockNumber := binary.BigEndian.Uint64(k)
		blockHash := common.BytesToHash(v)

		if blockNumber > to {
			break
		}

		has, err := cfg.blockReader.HasSenders(ctx, tx, blockHash, blockNumber)
		if err != nil {
			return err
		}
		if has {
			continue
		}

		var header *types.Header
		if header, err = cfg.blockReader.Header(ctx, tx, blockHash, blockNumber); err != nil {
			return err
		}
		if header == nil {
			logger.Warn(fmt.Sprintf("[%s] senders stage can't find header", logPrefix), "num", blockNumber, "hash", blockHash)
			continue
		}

		body, ok := exec.ReadBodyWithTransactionsFromGlobalReadAheader(blockHash)
		if body == nil || !ok {
			if body, err = cfg.blockReader.BodyWithTransactions(ctx, tx, blockHash, blockNumber); err != nil {
				return err
			}
			if body == nil {
				logger.Warn(fmt.Sprintf("[%s] ReadBodyWithTransactions can't find block", logPrefix), "num", blockNumber, "hash", blockHash)
				continue
			}
		}

		blockIndex := int(blockNumber) - int(s.BlockNumber) - 1
		if blockIndex < 0 {
			panic(blockIndex) //uint-underflow
		}

		// Register pending block
		pendingMu.Lock()
		// Skip blocks with no transactions
		if len(body.Transactions) == 0 {
			// Write empty senders for blocks with no transactions
			if err := collectorSenders.Collect(dbutils.BlockBodyKey(s.BlockNumber+uint64(blockIndex)+1, blockHash), nil); err != nil {
				return err
			}
			pendingMu.Unlock()
			continue
		}
		pendingBlocks[blockIndex] = &pendingBlock{
			senders: make([]byte, len(body.Transactions)*length.Addr),
			txCount: len(body.Transactions),
			hash:    blockHash,
		}
		pendingMu.Unlock()

		// Send individual transaction jobs
		for txIdx, txn := range body.Transactions {
			j := &senderRecoveryJob{
				txn:         txn,
				blockNumber: blockNumber,
				blockTime:   header.Time,
				blockHash:   blockHash,
				blockIndex:  blockIndex,
				txIndex:     txIdx,
			}
			select {
			case recoveryErr := <-errCh:
				if recoveryErr.err != nil {
					cancelWorkers()
					if err := handleRecoverErr(recoveryErr); err != nil {
						return err
					}
					break Loop
				}
			case jobs <- j:
			}
		}
	}

	close(jobs)
	wg.Wait()
	close(out)
	for recoveryErr := range errCh {
		if recoveryErr.err != nil {
			cancelWorkers()
			if err := handleRecoverErr(recoveryErr); err != nil {
				return err
			}
		}
	}
	if minBlockErr != nil {
		logger.Error(fmt.Sprintf("[%s] Error recovering senders for block %d %x): %v", logPrefix, minBlockNum, minBlockHash, minBlockErr))
		if cfg.badBlockHalt {
			return minBlockErr
		}
		minHeader := rawdb.ReadHeader(tx, minBlockHash, minBlockNum)
		if cfg.hd != nil && cfg.hd.POSSync() && errors.Is(minBlockErr, rules.ErrInvalidBlock) {
			cfg.hd.ReportBadHeaderPoS(minBlockHash, minHeader.ParentHash)
		}

		if to > s.BlockNumber {
			var unwindReason UnwindReason
			if errors.Is(minBlockErr, rules.ErrInvalidBlock) {
				unwindReason = BadBlock(minBlockHash, minBlockErr)
			} else {
				unwindReason = OperationalErr(minBlockErr)
			}
			if err := u.UnwindTo(minBlockNum-1, unwindReason, tx); err != nil {
				return err
			}
		}
	} else {
		if err := collectorSenders.Load(tx, kv.Senders, etl.IdentityLoadFunc, etl.TransformArgs{
			Quit: quitCh,
			LogDetailsLoad: func(k, v []byte) (additionalLogArguments []any) {
				return []any{"block", binary.BigEndian.Uint64(k)}
			},
		}); err != nil {
			return err
		}
		if err = s.Update(tx, to); err != nil {
			return err
		}
		log.Debug(fmt.Sprintf("[%s] Recovery done", logPrefix), "from", startFrom, "to", to, "blocks", to-startFrom+1, "took", time.Since(recoveryStart))
	}
	return nil
}

type senderRecoveryError struct {
	err         error
	blockNumber uint64
	blockHash   common.Hash
}

type senderRecoveryJob struct {
	txn         types.Transaction
	from        accounts.Address
	blockHash   common.Hash
	blockNumber uint64
	blockTime   uint64
	blockIndex  int // index of the block relative to s.BlockNumber
	txIndex     int // index of the tx within the block
	err         error
}

func recoverSenders(ctx context.Context, cryptoContext *secp256k1.Context, config *chain.Config, in, out chan *senderRecoveryJob, quit <-chan struct{}) {
	var job *senderRecoveryJob
	var ok bool
	for {
		select {
		case job, ok = <-in:
			if !ok {
				return
			}
			if job == nil {
				return
			}
		case <-ctx.Done():
			return
		case <-quit:
			return
		}

		signer := types.MakeSigner(config, job.blockNumber, job.blockTime)
		from, err := signer.SenderWithContext(cryptoContext, job.txn)
		if err != nil {
			job.err = fmt.Errorf("%w: error recovering sender for tx=%x, %v",
				rules.ErrInvalidBlock, job.txn.Hash(), err)
		} else {
			job.from = from
		}
		job.txn = nil // reduce ram usage and help GC

		// prevent sending to close channel
		if err := common.Stopped(quit); err != nil {
			job.err = err
		} else if err = common.Stopped(ctx.Done()); err != nil {
			job.err = err
		}
		out <- job

		if errors.Is(job.err, common.ErrStopped) {
			return
		}
	}
}

func UnwindSendersStage(u *UnwindState, tx kv.RwTx, cfg SendersCfg, ctx context.Context) (err error) {
	u.UnwindPoint = max(u.UnwindPoint, cfg.blockReader.FrozenBlocks()) // protect from unwind behind files
	if err = u.Done(tx); err != nil {
		return err
	}
	return nil
}
