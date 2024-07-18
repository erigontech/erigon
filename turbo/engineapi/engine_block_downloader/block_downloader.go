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

package engine_block_downloader

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math/big"
	"sync"
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon/eth/ethconfig"

	"github.com/erigontech/erigon-lib/log/v3"

	"github.com/erigontech/erigon-lib/chain"
	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/etl"
	execution "github.com/erigontech/erigon-lib/gointerfaces/executionproto"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/dbutils"

	"github.com/erigontech/erigon/core/rawdb"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/rlp"
	"github.com/erigontech/erigon/turbo/adapter"
	"github.com/erigontech/erigon/turbo/execution/eth1/eth1_chain_reader.go"
	"github.com/erigontech/erigon/turbo/services"
	"github.com/erigontech/erigon/turbo/stages/bodydownload"
	"github.com/erigontech/erigon/turbo/stages/headerdownload"
)

const (
	logInterval                 = 30 * time.Second
	requestLoopCutOff       int = 1
	forkchoiceTimeoutMillis     = 5000
)

type RequestBodyFunction func(context.Context, *bodydownload.BodyRequest) ([64]byte, bool)

// EngineBlockDownloader is responsible to download blocks in reverse, and then insert them in the database.
type EngineBlockDownloader struct {
	bacgroundCtx context.Context

	// downloaders
	hd          *headerdownload.HeaderDownload
	bd          *bodydownload.BodyDownload
	bodyReqSend RequestBodyFunction

	// current status of the downloading process, aka: is it doing anything?
	status atomic.Value // it is a headerdownload.SyncStatus

	// data reader
	blockPropagator adapter.BlockPropagator
	blockReader     services.FullBlockReader
	db              kv.RoDB

	// Execution module
	chainRW eth1_chain_reader.ChainReaderWriterEth1

	// Misc
	tmpdir  string
	timeout int
	config  *chain.Config
	syncCfg ethconfig.Sync

	// lock
	lock sync.Mutex

	// logs
	logger log.Logger
}

func NewEngineBlockDownloader(ctx context.Context, logger log.Logger, hd *headerdownload.HeaderDownload, executionClient execution.ExecutionClient,
	bd *bodydownload.BodyDownload, blockPropagator adapter.BlockPropagator,
	bodyReqSend RequestBodyFunction, blockReader services.FullBlockReader, db kv.RoDB, config *chain.Config,
	tmpdir string, syncCfg ethconfig.Sync) *EngineBlockDownloader {
	timeout := syncCfg.BodyDownloadTimeoutSeconds
	var s atomic.Value
	s.Store(headerdownload.Idle)
	return &EngineBlockDownloader{
		bacgroundCtx:    ctx,
		hd:              hd,
		bd:              bd,
		db:              db,
		status:          s,
		config:          config,
		syncCfg:         syncCfg,
		tmpdir:          tmpdir,
		logger:          logger,
		blockReader:     blockReader,
		blockPropagator: blockPropagator,
		timeout:         timeout,
		bodyReqSend:     bodyReqSend,
		chainRW:         eth1_chain_reader.NewChainReaderEth1(config, executionClient, forkchoiceTimeoutMillis),
	}
}

func (e *EngineBlockDownloader) scheduleHeadersDownload(
	requestId int,
	hashToDownload libcommon.Hash,
	heightToDownload uint64,
) bool {
	if e.hd.PosStatus() != headerdownload.Idle {
		e.logger.Info("[EngineBlockDownloader] Postponing PoS download since another one is in progress", "height", heightToDownload, "hash", hashToDownload)
		return false
	}

	if heightToDownload == 0 {
		e.logger.Info("[EngineBlockDownloader] Downloading PoS headers...", "hash", hashToDownload, "requestId", requestId)
	} else {
		e.logger.Info("[EngineBlockDownloader] Downloading PoS headers...", "hash", hashToDownload, "requestId", requestId, "height", heightToDownload)
	}

	e.hd.SetRequestId(requestId)
	e.hd.SetHeaderToDownloadPoS(hashToDownload, heightToDownload)
	e.hd.SetPOSSync(true) // This needs to be called after SetHeaderToDownloadPOS because SetHeaderToDownloadPOS sets `posAnchor` member field which is used by ProcessHeadersPOS

	//nolint
	e.hd.SetHeadersCollector(etl.NewCollector("EngineBlockDownloader", e.tmpdir, etl.NewSortableBuffer(etl.BufferOptimalSize/2), e.logger))

	e.hd.SetPosStatus(headerdownload.Syncing)

	return true
}

// waitForEndOfHeadersDownload waits until the download of headers ends and returns the outcome.
func (e *EngineBlockDownloader) waitForEndOfHeadersDownload(ctx context.Context) (headerdownload.SyncStatus, error) {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()

	for {
		select {
		case <-ticker.C:
			if e.hd.PosStatus() != headerdownload.Syncing {
				return e.hd.PosStatus(), nil
			}
		case <-ctx.Done():
			return e.hd.PosStatus(), ctx.Err()
		case <-logEvery.C:
			e.logger.Info("[EngineBlockDownloader] Waiting for headers download to finish")
		}
	}
}

// waitForEndOfHeadersDownload waits until the download of headers ends and returns the outcome.
func (e *EngineBlockDownloader) loadDownloadedHeaders(tx kv.RwTx) (fromBlock uint64, toBlock uint64, fromHash libcommon.Hash, err error) {
	var lastValidHash libcommon.Hash
	var badChainError error // TODO(yperbasis): this is not set anywhere
	var foundPow bool

	headerLoadFunc := func(key, value []byte, _ etl.CurrentTableReader, _ etl.LoadNextFunc) error {
		var h types.Header
		// no header to process
		if value == nil {
			return nil
		}
		if err := rlp.DecodeBytes(value, &h); err != nil {
			return err
		}
		if badChainError != nil {
			e.hd.ReportBadHeaderPoS(h.Hash(), lastValidHash)
			return nil
		}
		lastValidHash = h.ParentHash
		// If we are in PoW range then block validation is not required anymore.
		if foundPow {
			if (fromHash == libcommon.Hash{}) {
				fromHash = h.Hash()
				fromBlock = h.Number.Uint64()
			}
			toBlock = h.Number.Uint64()
			return saveHeader(tx, &h, h.Hash())
		}

		foundPow = h.Difficulty.Cmp(libcommon.Big0) != 0
		if foundPow {
			if (fromHash == libcommon.Hash{}) {
				fromHash = h.Hash()
				fromBlock = h.Number.Uint64()
			}
			toBlock = h.Number.Uint64()
			return saveHeader(tx, &h, h.Hash())
		}
		if (fromHash == libcommon.Hash{}) {
			fromHash = h.Hash()
			fromBlock = h.Number.Uint64()
		}
		toBlock = h.Number.Uint64()
		// Validate state if possible (bodies will be retrieved through body download)
		return saveHeader(tx, &h, h.Hash())
	}

	err = e.hd.HeadersCollector().Load(tx, kv.Headers, headerLoadFunc, etl.TransformArgs{
		LogDetailsLoad: func(k, v []byte) (additionalLogArguments []interface{}) {
			return []interface{}{"block", binary.BigEndian.Uint64(k)}
		},
	})
	return
}

func saveHeader(db kv.RwTx, header *types.Header, hash libcommon.Hash) error {
	blockHeight := header.Number.Uint64()
	// TODO(yperbasis): do we need to check if the header is already inserted (oldH)?

	parentTd, err := rawdb.ReadTd(db, header.ParentHash, blockHeight-1)
	if err != nil || parentTd == nil {
		return fmt.Errorf("[saveHeader] parent's total difficulty not found with hash %x and height %d for header %x %d: %v", header.ParentHash, blockHeight-1, hash, blockHeight, err)
	}
	td := new(big.Int).Add(parentTd, header.Difficulty)
	if err = rawdb.WriteHeader(db, header); err != nil {
		return fmt.Errorf("[saveHeader] failed to WriteHeader: %w", err)
	}
	if err = rawdb.WriteTd(db, hash, blockHeight, td); err != nil {
		return fmt.Errorf("[saveHeader] failed to WriteTd: %w", err)
	}
	if err = rawdb.WriteCanonicalHash(db, hash, blockHeight); err != nil {
		return fmt.Errorf("[saveHeader] failed to canonical hash: %w", err)
	}
	return nil
}

func (e *EngineBlockDownloader) insertHeadersAndBodies(ctx context.Context, tx kv.Tx, fromBlock uint64, fromHash libcommon.Hash, toBlock uint64) error {
	blockBatchSize := 500
	blockWrittenLogSize := 20_000
	// We divide them in batches
	blocksBatch := []*types.Block{}

	headersCursors, err := tx.Cursor(kv.Headers)
	if err != nil {
		return err
	}
	inserted := uint64(0)

	log.Info("Beginning downloaded blocks insertion")
	// Start by seeking headers
	for k, v, err := headersCursors.Seek(dbutils.HeaderKey(fromBlock, fromHash)); k != nil; k, v, err = headersCursors.Next() {
		if err != nil {
			return err
		}
		if len(blocksBatch) == blockBatchSize {
			if err := e.chainRW.InsertBlocksAndWait(ctx, blocksBatch); err != nil {
				return err
			}
			currentHeader := e.chainRW.CurrentHeader(ctx)
			lastBlockNumber := blocksBatch[len(blocksBatch)-1].NumberU64()
			isForkChoiceNeeded := currentHeader == nil || lastBlockNumber > currentHeader.Number.Uint64()
			inserted += uint64(len(blocksBatch))
			if inserted >= uint64(e.syncCfg.LoopBlockLimit) {
				lastHash := blocksBatch[len(blocksBatch)-1].Hash()
				if isForkChoiceNeeded {
					if _, _, _, err := e.chainRW.UpdateForkChoice(ctx, lastHash, lastHash, lastHash); err != nil {
						return err
					}
				}
				inserted = 0
			}
			blocksBatch = blocksBatch[:0]
		}
		header := new(types.Header)
		if err := rlp.Decode(bytes.NewReader(v), header); err != nil {
			e.logger.Error("Invalid block header RLP", "err", err)
			return nil
		}
		number := header.Number.Uint64()
		if number > toBlock {
			return e.chainRW.InsertBlocksAndWait(ctx, blocksBatch)
		}
		hash := header.Hash()
		body, err := rawdb.ReadBodyWithTransactions(tx, hash, number)
		if err != nil {
			return err
		}
		if body == nil {
			return fmt.Errorf("missing body at block=%d", number)
		}
		blocksBatch = append(blocksBatch, types.NewBlockFromStorage(hash, header, body.Transactions, body.Uncles, body.Withdrawals, body.Requests))
		if number%uint64(blockWrittenLogSize) == 0 {
			e.logger.Info("[insertHeadersAndBodies] Written blocks", "progress", number, "to", toBlock)
		}
	}
	return e.chainRW.InsertBlocksAndWait(ctx, blocksBatch)

}
