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

	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/execution"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/turbo/adapter"
	"github.com/ledgerwatch/erigon/turbo/execution/eth1/eth1_chain_reader.go"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/erigon/turbo/stages/bodydownload"
	"github.com/ledgerwatch/erigon/turbo/stages/headerdownload"
)

const (
	logInterval           = 30 * time.Second
	requestLoopCutOff int = 1
)

type RequestBodyFunction func(context.Context, *bodydownload.BodyRequest) ([64]byte, bool)

// EngineBlockDownloader is responsible to download blocks in reverse, and then insert them in the database.
type EngineBlockDownloader struct {
	ctx context.Context
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

	// lock
	lock sync.Mutex

	// logs
	logger log.Logger
}

func NewEngineBlockDownloader(ctx context.Context, logger log.Logger, hd *headerdownload.HeaderDownload, executionClient execution.ExecutionClient,
	bd *bodydownload.BodyDownload, blockPropagator adapter.BlockPropagator,
	bodyReqSend RequestBodyFunction, blockReader services.FullBlockReader, db kv.RoDB, config *chain.Config,
	tmpdir string, timeout int) *EngineBlockDownloader {
	var s atomic.Value
	s.Store(headerdownload.Idle)
	return &EngineBlockDownloader{
		ctx:             ctx,
		hd:              hd,
		bd:              bd,
		db:              db,
		status:          s,
		config:          config,
		tmpdir:          tmpdir,
		logger:          logger,
		blockReader:     blockReader,
		blockPropagator: blockPropagator,
		timeout:         timeout,
		bodyReqSend:     bodyReqSend,
		chainRW:         eth1_chain_reader.NewChainReaderEth1(ctx, config, executionClient, 1000),
	}
}

func (e *EngineBlockDownloader) scheduleHeadersDownload(
	requestId int,
	hashToDownload libcommon.Hash,
	heightToDownload uint64,
	downloaderTip libcommon.Hash,
) bool {
	if e.hd.PosStatus() != headerdownload.Idle {
		e.logger.Info("[EngineBlockDownloader] Postponing PoS download since another one is in progress", "height", heightToDownload, "hash", hashToDownload)
		return false
	}

	if heightToDownload == 0 {
		e.logger.Info("[EngineBlockDownloader] Downloading PoS headers...", "height", "unknown", "hash", hashToDownload, "requestId", requestId)
	} else {
		e.logger.Info("[EngineBlockDownloader] Downloading PoS headers...", "height", heightToDownload, "hash", hashToDownload, "requestId", requestId)
	}

	e.hd.SetRequestId(requestId)
	e.hd.SetPoSDownloaderTip(downloaderTip)
	e.hd.SetHeaderToDownloadPoS(hashToDownload, heightToDownload)
	e.hd.SetPOSSync(true) // This needs to be called after SetHeaderToDownloadPOS because SetHeaderToDownloadPOS sets `posAnchor` member field which is used by ProcessHeadersPOS

	//nolint
	e.hd.SetHeadersCollector(etl.NewCollector("EngineBlockDownloader", e.tmpdir, etl.NewSortableBuffer(etl.BufferOptimalSize), e.logger))

	e.hd.SetPosStatus(headerdownload.Syncing)

	return true
}

// waitForEndOfHeadersDownload waits until the download of headers ends and returns the outcome.
func (e *EngineBlockDownloader) waitForEndOfHeadersDownload() headerdownload.SyncStatus {
	for e.hd.PosStatus() == headerdownload.Syncing {
		time.Sleep(10 * time.Millisecond)
	}
	return e.hd.PosStatus()
}

// waitForEndOfHeadersDownload waits until the download of headers ends and returns the outcome.
func (e *EngineBlockDownloader) loadDownloadedHeaders(tx kv.RwTx) (fromBlock uint64, toBlock uint64, fromHash libcommon.Hash, err error) {
	var lastValidHash libcommon.Hash
	var badChainError error
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

func (e *EngineBlockDownloader) insertHeadersAndBodies(tx kv.Tx, fromBlock uint64, fromHash libcommon.Hash, toBlock uint64) error {
	blockBatchSize := 500
	blockWrittenLogSize := 20_000
	// We divide them in batches
	blocksBatch := []*types.Block{}

	headersCursors, err := tx.Cursor(kv.Headers)
	if err != nil {
		return err
	}

	log.Info("Beginning downloaded blocks insertion")
	// Start by seeking headers
	for k, v, err := headersCursors.Seek(dbutils.HeaderKey(fromBlock, fromHash)); k != nil; k, v, err = headersCursors.Next() {
		if err != nil {
			return err
		}
		if len(blocksBatch) == blockBatchSize {
			if err := e.chainRW.InsertBlocksAndWait(blocksBatch); err != nil {
				return err
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
			return e.chainRW.InsertBlocksAndWait(blocksBatch)
		}
		hash := header.Hash()
		body, err := rawdb.ReadBodyWithTransactions(tx, hash, number)
		if err != nil {
			return err
		}
		if body == nil {
			return fmt.Errorf("missing body at block=%d", number)
		}
		blocksBatch = append(blocksBatch, types.NewBlockFromStorage(hash, header, body.Transactions, nil, body.Withdrawals))
		if number%uint64(blockWrittenLogSize) == 0 {
			e.logger.Info("[insertHeadersAndBodies] Written blocks", "progress", number, "to", toBlock)
		}
	}
	return e.chainRW.InsertBlocksAndWait(blocksBatch)

}
