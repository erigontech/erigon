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

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/common/length"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/etl"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbutils"
	"github.com/erigontech/erigon/db/kv/prune"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/consensus"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/stages/headerdownload"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/turbo/services"
)

type SendersCfg struct {
	db              kv.RwDB
	batchSize       int
	numOfGoroutines int
	readChLen       int
	badBlockHalt    bool
	tmpdir          string
	prune           prune.Mode
	chainConfig     *chain.Config
	hd              *headerdownload.HeaderDownload
	blockReader     services.FullBlockReader
	syncCfg         ethconfig.Sync
}

func StageSendersCfg(db kv.RwDB, chainCfg *chain.Config, syncCfg ethconfig.Sync, badBlockHalt bool, tmpdir string, prune prune.Mode, blockReader services.FullBlockReader, hd *headerdownload.HeaderDownload) SendersCfg {
	const sendersBatchSize = 1000

	return SendersCfg{
		db:              db,
		batchSize:       sendersBatchSize,
		numOfGoroutines: secp256k1.NumOfContexts(), // we can only be as parallels as our crypto library supports,
		readChLen:       4,
		badBlockHalt:    badBlockHalt,
		tmpdir:          tmpdir,
		chainConfig:     chainCfg,
		prune:           prune,
		hd:              hd,
		blockReader:     blockReader,
		syncCfg:         syncCfg,
	}
}

func SpawnRecoverSendersStage(cfg SendersCfg, s *StageState, u Unwinder, tx kv.RwTx, toBlock uint64, ctx context.Context, logger log.Logger) error {
	if s.BlockNumber < cfg.blockReader.FrozenBlocks() {
		s.BlockNumber = cfg.blockReader.FrozenBlocks()
	}

	quitCh := ctx.Done()
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
			recoverSenders(ctx, logPrefix, secp256k1.ContextForThread(threadNo), cfg.chainConfig, jobs, out, quitCh)
		}(i)
	}

	collectorSenders := etl.NewCollectorWithAllocator(logPrefix, cfg.tmpdir, etl.SmallSortableBuffers, logger)
	defer collectorSenders.Close()
	collectorSenders.SortAndFlushInBackground(true)
	collectorSenders.LogLvl(log.LvlDebug)

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
			case <-logEvery.C:
				n := s.BlockNumber
				if j != nil {
					n += uint64(j.index)
				}
				logger.Info(fmt.Sprintf("[%s] Recovery", logPrefix), "block_number", n, "ch", fmt.Sprintf("%d/%d", len(jobs), cap(jobs)))
			case j, ok = <-out:
				if !ok {
					return
				}
				if j.err != nil {
					errCh <- senderRecoveryError{err: j.err, blockNumber: j.blockNumber, blockHash: j.blockHash}
					return
				}

				k := make([]byte, 4)
				binary.BigEndian.PutUint32(k, uint32(j.index))
				index := int(binary.BigEndian.Uint32(k))
				if err := collectorSenders.Collect(dbutils.BlockBodyKey(s.BlockNumber+uint64(index)+1, j.blockHash), j.senders); err != nil {
					errCh <- senderRecoveryError{err: j.err}
					return
				}
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

		var body *types.Body
		if body, err = cfg.blockReader.BodyWithTransactions(ctx, tx, blockHash, blockNumber); err != nil {
			return err
		}
		if body == nil {
			logger.Warn(fmt.Sprintf("[%s] ReadBodyWithTransactions can't find block", logPrefix), "num", blockNumber, "hash", blockHash)
			continue
		}

		j := &senderRecoveryJob{
			body:        body,
			blockNumber: blockNumber,
			blockTime:   header.Time,
			blockHash:   blockHash,
			index:       int(blockNumber) - int(s.BlockNumber) - 1,
		}
		if j.index < 0 {
			panic(j.index) //uint-underflow
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
		if cfg.hd != nil && cfg.hd.POSSync() && errors.Is(minBlockErr, consensus.ErrInvalidBlock) {
			cfg.hd.ReportBadHeaderPoS(minBlockHash, minHeader.ParentHash)
		}

		if to > s.BlockNumber {
			if err := u.UnwindTo(minBlockNum-1, BadBlock(minBlockHash, minBlockErr), tx); err != nil {
				return err
			}
		}
	} else {
		if err := collectorSenders.Load(tx, kv.Senders, etl.IdentityLoadFunc, etl.TransformArgs{
			Quit: quitCh,
			LogDetailsLoad: func(k, v []byte) (additionalLogArguments []interface{}) {
				return []interface{}{"block", binary.BigEndian.Uint64(k)}
			},
		}); err != nil {
			return err
		}
		if err = s.Update(tx, to); err != nil {
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
	senders     []byte
	blockHash   common.Hash
	blockNumber uint64
	blockTime   uint64
	index       int
	err         error
}

func recoverSenders(ctx context.Context, logPrefix string, cryptoContext *secp256k1.Context, config *chain.Config, in, out chan *senderRecoveryJob, quit <-chan struct{}) {
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

		body := job.body
		job.body = nil // reduce ram usage and help GC
		signer := types.MakeSigner(config, job.blockNumber, job.blockTime)
		job.senders = make([]byte, len(body.Transactions)*length.Addr)
		for i, txn := range body.Transactions {
			from, err := signer.SenderWithContext(cryptoContext, txn)
			if err != nil {
				job.err = fmt.Errorf("%w: error recovering sender for tx=%x, %v",
					consensus.ErrInvalidBlock, txn.Hash(), err)
				break
			}
			copy(job.senders[i*length.Addr:], from[:])
		}

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

	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	if err = u.Done(tx); err != nil {
		return err
	}
	if !useExternalTx {
		if err = tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}
