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
	"time"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/dbservices"
	"github.com/erigontech/erigon/db/etl"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbutils"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/exec"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

type SendersCfg struct {
	badBlockHalt bool
	tmpdir       string
	chainConfig  *chain.Config
	blockReader  dbservices.FullBlockReader
	readAheader  *exec.BlockReadAheader
}

func StageSendersCfg(chainCfg *chain.Config, badBlockHalt bool, tmpdir string, blockReader dbservices.FullBlockReader, readAheader *exec.BlockReadAheader) SendersCfg {
	return SendersCfg{
		badBlockHalt: badBlockHalt,
		tmpdir:       tmpdir,
		chainConfig:  chainCfg,
		blockReader:  blockReader,
		readAheader:  readAheader,
	}
}

func SpawnRecoverSendersStage(cfg SendersCfg, s *StageState, u Unwinder, tx kv.RwTx, toBlock uint64, ctx context.Context, logger log.Logger) error {
	if s.BlockNumber < cfg.blockReader.FrozenBlocks() {
		s.BlockNumber = cfg.blockReader.FrozenBlocks()
	}

	prevStageProgress, err := stages.GetStageProgress(tx, stages.Bodies)
	if err != nil {
		return err
	}

	to := prevStageProgress
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

	startFrom := s.BlockNumber + 1
	recoveryStart := time.Now()

	collectorSenders := etl.NewCollectorWithAllocator(logPrefix, cfg.tmpdir, etl.SmallSortableBuffers, logger)
	defer collectorSenders.Close()
	collectorSenders.SortAndFlushInBackground(false)
	collectorSenders.LogLvl(log.LvlDebug)

	bodiesC, err := tx.Cursor(kv.HeaderCanonical)
	if err != nil {
		return err
	}
	defer bodiesC.Close()

	var batch sendersBatch
	var bad *badSendersBlock
	for k, v, err := bodiesC.Seek(hexutil.EncodeTs(startFrom)); k != nil && bad == nil; k, v, err = bodiesC.Next() {
		if err != nil {
			return err
		}
		if err := common.Stopped(ctx.Done()); err != nil {
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

		header, err := cfg.blockReader.Header(ctx, tx, blockHash, blockNumber)
		if err != nil {
			return err
		}
		if header == nil {
			logger.Warn(fmt.Sprintf("[%s] senders stage can't find header", logPrefix), "num", blockNumber, "hash", blockHash)
			continue
		}

		body, cached := cfg.readAheader.ReadBodyWithTransactions(blockHash)
		if body == nil || !cached {
			cached = false
			if body, err = cfg.blockReader.BodyWithTransactions(ctx, tx, blockHash, blockNumber); err != nil {
				return err
			}
			if body == nil {
				logger.Warn(fmt.Sprintf("[%s] ReadBodyWithTransactions can't find block", logPrefix), "num", blockNumber, "hash", blockHash)
				continue
			}
		}

		batch.add(cfg, header, blockHash, body, cached)
		if len(batch.txns) >= sendersBatchTxs || len(batch.blocks) >= sendersBatchBlocks {
			if bad, err = batch.flush(ctx, cfg, collectorSenders); err != nil {
				return err
			}
		}
	}
	if bad == nil {
		if bad, err = batch.flush(ctx, cfg, collectorSenders); err != nil {
			return err
		}
	}

	if bad != nil {
		logger.Error(fmt.Sprintf("[%s] Error recovering senders for block %d %x): %v", logPrefix, bad.number, bad.hash, bad.err))
		if cfg.badBlockHalt {
			return bad.err
		}
		if to > s.BlockNumber {
			unwindReason := OperationalErr(bad.err)
			if errors.Is(bad.err, rules.ErrInvalidBlock) {
				unwindReason = BadBlock(bad.hash, bad.err)
			}
			if err := u.UnwindTo(bad.number-1, unwindReason, tx); err != nil {
				return err
			}
		}
		return nil
	}

	if err := collectorSenders.Load(tx, kv.Senders, etl.IdentityLoadFunc, etl.TransformArgs{
		Quit: ctx.Done(),
		LogDetailsLoad: func(k, v []byte) (additionalLogArguments []any) {
			return []any{"block", binary.BigEndian.Uint64(k)}
		},
	}); err != nil {
		return err
	}
	if err := s.Update(tx, to); err != nil {
		return err
	}
	log.Debug(fmt.Sprintf("[%s] Recovery done", logPrefix), "from", startFrom, "to", to, "blocks", to-startFrom+1, "took", time.Since(recoveryStart))
	return nil
}

const (
	sendersBatchTxs = 64 * 1024
	// sendersBatchBlocks bounds the bodies a batch keeps: empty blocks add none to sendersBatchTxs.
	sendersBatchBlocks = 1024
)

type sendersBlock struct {
	number     uint64
	hash       common.Hash
	signer     *types.Signer
	body       *types.Body
	cached     bool
	registered *exec.SendersRecovery
	first      int
}

type badSendersBlock struct {
	number uint64
	hash   common.Hash
	err    error
}

type sendersBatch struct {
	blocks  []sendersBlock
	txns    types.Transactions
	signers []*types.Signer
}

func (b *sendersBatch) add(cfg SendersCfg, header *types.Header, hash common.Hash, body *types.Body, cached bool) {
	blk := sendersBlock{
		number: header.Number.Uint64(),
		hash:   hash,
		signer: types.MakeSigner(cfg.chainConfig, header.Number.Uint64(), header.Time),
		body:   body,
		cached: cached,
		first:  len(b.txns),
	}
	if r, ok := cfg.readAheader.TakeSendersRecovery(hash); ok {
		blk.registered = r
	} else {
		b.txns = append(b.txns, body.Transactions...)
		for range body.Transactions {
			b.signers = append(b.signers, blk.signer)
		}
	}
	b.blocks = append(b.blocks, blk)
}

func (b *sendersBatch) flush(ctx context.Context, cfg SendersCfg, collector *etl.Collector) (*badSendersBlock, error) {
	defer func() { b.blocks, b.txns, b.signers = b.blocks[:0], b.txns[:0], b.signers[:0] }()
	recovery := exec.StartBatchSendersRecovery(ctx, b.signers, b.txns)
	recovered, batchErr := recovery.Wait(ctx)
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	for _, blk := range b.blocks {
		n := len(blk.body.Transactions)
		var senders []accounts.Address
		var err error
		switch {
		case blk.registered != nil:
			if senders, err = blk.registered.Wait(ctx); err != nil || len(senders) != n {
				senders, err = exec.StartSendersRecovery(ctx, blk.signer, blk.body.Transactions).Wait(ctx)
			}
		case batchErr != nil:
			if failed := recovery.FailedTx(); failed >= blk.first && failed < blk.first+n {
				return &badSendersBlock{number: blk.number, hash: blk.hash, err: batchErr}, nil
			}
			continue
		default:
			senders = recovered[blk.first : blk.first+n]
		}
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return &badSendersBlock{number: blk.number, hash: blk.hash, err: err}, nil
		}
		if blk.cached {
			for i, txn := range blk.body.Transactions {
				txn.SetSender(senders[i])
			}
		}
		if err := collector.Collect(dbutils.BlockBodyKey(blk.number, blk.hash), sendersBytes(senders)); err != nil {
			return nil, err
		}
	}
	if batchErr != nil {
		return nil, batchErr
	}
	return nil, nil
}

func sendersBytes(senders []accounts.Address) []byte {
	if len(senders) == 0 {
		return nil
	}
	out := make([]byte, 0, len(senders)*length.Addr)
	for _, sender := range senders {
		addr := sender.Value()
		out = append(out, addr[:]...)
	}
	return out
}

func UnwindSendersStage(u *UnwindState, tx kv.RwTx, cfg SendersCfg, ctx context.Context) (err error) {
	u.UnwindPoint = max(u.UnwindPoint, cfg.blockReader.FrozenBlocks()) // protect from unwind behind files
	if err := u.Done(tx); err != nil {
		return err
	}
	return nil
}
