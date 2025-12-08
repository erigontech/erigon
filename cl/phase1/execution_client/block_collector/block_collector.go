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

package block_collector

import (
	"context"
	"fmt"
	"sync"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/db/etl"
	"github.com/erigontech/erigon/db/kv/dbutils"
	"github.com/erigontech/erigon/execution/types"
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
	syncBackLoop   uint64

	mu sync.Mutex
}

// NewBlockCollector creates a new block collector
func NewBlockCollector(logger log.Logger, engine execution_client.ExecutionEngine, beaconChainCfg *clparams.BeaconChainConfig, syncBackLoopAmount uint64, tmpdir string) BlockCollector {
	return &blockCollector{
		collector:      etl.NewCollector(etlPrefix, tmpdir, etl.NewSortableBuffer(etl.BufferOptimalSize), logger),
		tmpdir:         tmpdir,
		beaconChainCfg: beaconChainCfg,
		logger:         logger,
		engine:         engine,
		syncBackLoop:   syncBackLoopAmount,
	}
}

// AddBlock adds the execution payload part of the block to the collector
func (b *blockCollector) AddBlock(block *cltypes.BeaconBlock) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	payload := block.Body.ExecutionPayload
	encodedBlock, err := encodeBlock(payload, block.ParentRoot, block.Body.GetExecutionRequestsList())
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
	blocksBatch := []*types.Block{}
	var err error
	inserted := uint64(0)

	if err := b.collector.Load(nil, "", func(k, v []byte, table etl.CurrentTableReader, next etl.LoadNextFunc) error {
		if len(v) == 0 {
			return nil
		}
		v, err = utils.DecompressSnappy(v, false)
		if err != nil {
			return err
		}
		version := clparams.StateVersion(v[0])
		parentRoot := common.BytesToHash(v[1:33])
		requestsHash := common.Hash{}
		if version >= clparams.ElectraVersion {
			// get the requests hash
			requestsHash = common.BytesToHash(v[33:65])
			v = v[65:]
		} else {
			v = v[33:]
		}
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
		// We expect the genesis to be present in DB already
		if executionPayload.BlockNumber == 0 {
			return nil
		}
		header, err := executionPayload.RlpHeader(&parentRoot, requestsHash)
		if err != nil {
			b.logger.Warn("bad blocks segment received", "err", err)
			return err
		}
		blocksBatch = append(blocksBatch, types.NewBlockFromStorage(executionPayload.BlockHash, header, txs, nil, body.Withdrawals))
		if len(blocksBatch) >= batchSize {
			b.logger.Info("[Caplin] Inserting blocks", "from", blocksBatch[0].NumberU64(), "to", blocksBatch[len(blocksBatch)-1].NumberU64())
			if err := b.engine.InsertBlocks(ctx, blocksBatch, true); err != nil {
				b.logger.Warn("failed to insert blocks", "err", err)
			}
			inserted += uint64(len(blocksBatch))
			b.logger.Info("[Caplin] Inserted blocks", "progress", blocksBatch[len(blocksBatch)-1].NumberU64())
			// If we have inserted enough blocks, update fork choice (Optimation for E35)
			lastBlockHash := blocksBatch[len(blocksBatch)-1].Hash()
			currentHeader, err := b.engine.CurrentHeader(ctx)
			if err != nil {
				b.logger.Warn("failed to get current header", "err", err)
			}
			isForkchoiceNeeded := currentHeader == nil || blocksBatch[len(blocksBatch)-1].NumberU64() > currentHeader.Number.Uint64()
			if inserted >= b.syncBackLoop {
				if isForkchoiceNeeded {
					if _, err := b.engine.ForkChoiceUpdate(ctx, lastBlockHash, lastBlockHash, lastBlockHash, nil); err != nil {
						b.logger.Warn("failed to update fork choice", "err", err)
					}
				}
				inserted = 0
			}
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
		b.logger.Info("[Caplin] Inserted blocks", "progress", blocksBatch[len(blocksBatch)-1].NumberU64())
	}
	b.size = 0
	// Create a new collector
	b.collector = etl.NewCollector(etlPrefix, b.tmpdir, etl.NewSortableBuffer(etl.BufferOptimalSize), b.logger)
	return nil

}

// serializes block value
func encodeBlock(payload *cltypes.Eth1Block, parentRoot common.Hash, executionRequestsList []hexutil.Bytes) ([]byte, error) {
	encodedPayload, err := payload.EncodeSSZ(nil)
	if err != nil {
		return nil, fmt.Errorf("error encoding execution payload during download: %s", err)
	}
	if executionRequestsList != nil {
		// electra version
		requestsHash := cltypes.ComputeExecutionRequestHash(executionRequestsList)
		// version + parentRoot + requestsHash + encodedPayload
		return utils.CompressSnappy(append([]byte{byte(payload.Version())}, append(append(parentRoot[:], requestsHash[:]...), encodedPayload...)...)), nil
	}
	// Use snappy compression that the temporary files do not take too much disk.
	// version + parentRoot + encodedPayload
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
