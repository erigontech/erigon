package block_collector

import (
	"context"
	"fmt"
	"sync"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/dbutils"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/phase1/execution_client"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/log/v3"
)

var (
	etlPrefix = "Caplin-Blocks"
	batchSize = 1000
)

type BlockCollector interface {
	AddBlock(block *cltypes.BeaconBlock) error
	Flush(ctx context.Context) error
}

type blockCollector struct {
	collector      *etl.Collector // simple etl.Collector
	tmpdir         string
	beaconChainCfg *clparams.BeaconChainConfig
	size           uint64
	logger         log.Logger
	engine         execution_client.ExecutionEngine

	mu sync.Mutex
}

// NewBlockCollector creates a new block collector
func NewBlockCollector(logger log.Logger, engine execution_client.ExecutionEngine, beaconChainCfg *clparams.BeaconChainConfig, tmpdir string) BlockCollector {
	return &blockCollector{
		collector:      etl.NewCollector(etlPrefix, tmpdir, etl.NewSortableBuffer(etl.BufferOptimalSize), logger),
		tmpdir:         tmpdir,
		beaconChainCfg: beaconChainCfg,
		logger:         logger,
		engine:         engine,
	}
}

// AddBlock adds the execution payload part of the block to the collector
func (b *blockCollector) AddBlock(block *cltypes.BeaconBlock) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	payload := block.Body.ExecutionPayload
	encodedBlock, err := encodeBlock(payload, block.ParentRoot)
	if err != nil {
		return err
	}
	key, err := payloadKey(payload)
	if err != nil {
		return err
	}
	b.size++
	return b.collector.Collect(key, encodedBlock)
}

func (b *blockCollector) Flush(ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.size == 0 {
		return nil
	}
	tmpDB := memdb.New(b.tmpdir)
	defer tmpDB.Close()
	defer b.collector.Close()
	tmpTx, err := tmpDB.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer tmpTx.Rollback()
	blocksBatch := []*types.Block{}

	if err := b.collector.Load(tmpTx, kv.Headers, func(k, v []byte, table etl.CurrentTableReader, next etl.LoadNextFunc) error {
		if len(v) == 0 {
			return nil
		}
		v, err = utils.DecompressSnappy(v)
		if err != nil {
			return err
		}
		version := clparams.StateVersion(v[0])
		parentRoot := common.BytesToHash(v[1:33])
		v = v[33:]
		executionPayload := cltypes.NewEth1Block(version, b.beaconChainCfg)
		if err := executionPayload.DecodeSSZ(v, int(version)); err != nil {
			return err
		}
		body := executionPayload.Body()
		txs, err := types.DecodeTransactions(body.Transactions)
		if err != nil {
			b.logger.Warn("bad blocks segment received", "err", err)
			return err
		}
		header, err := executionPayload.RlpHeader(&parentRoot)
		if err != nil {
			b.logger.Warn("bad blocks segment received", "err", err)
			return err
		}
		blocksBatch = append(blocksBatch, types.NewBlockFromStorage(executionPayload.BlockHash, header, txs, nil, body.Withdrawals))
		if len(blocksBatch) >= batchSize {
			if err := b.engine.InsertBlocks(ctx, blocksBatch, true); err != nil {
				b.logger.Warn("failed to insert blocks", "err", err)
			}
			b.logger.Info("[Caplin] Inserted blocks", "progress", blocksBatch[len(blocksBatch)-1].NumberU64())
			blocksBatch = []*types.Block{}
		}
		return next(k, nil, nil)
	}, etl.TransformArgs{}); err != nil {
		return err
	}
	if len(blocksBatch) > 0 {
		if err := b.engine.InsertBlocks(ctx, blocksBatch, true); err != nil {
			b.logger.Warn("failed to insert blocks", "err", err)
		}
	}
	b.size = 0
	// Create a new collector
	b.collector = etl.NewCollector(etlPrefix, b.tmpdir, etl.NewSortableBuffer(etl.BufferOptimalSize), b.logger)
	return nil

}

// serializes block value
func encodeBlock(payload *cltypes.Eth1Block, parentRoot common.Hash) ([]byte, error) {
	encodedPayload, err := payload.EncodeSSZ(nil)
	if err != nil {
		return nil, fmt.Errorf("error encoding execution payload during download: %s", err)
	}
	// Use snappy compression that the temporary files do not take too much disk.
	return utils.CompressSnappy(append([]byte{byte(payload.Version())}, append(parentRoot[:], encodedPayload...)...)), nil
}

// payloadKey returns the key for the payload: number + payload.HashTreeRoot()
func payloadKey(payload *cltypes.Eth1Block) ([]byte, error) {
	root, err := payload.HashSSZ()
	if err != nil {
		return nil, err
	}
	numberBytes := dbutils.EncodeBlockNumber(payload.BlockNumber)
	return append(numberBytes, root[:]...), nil
}
