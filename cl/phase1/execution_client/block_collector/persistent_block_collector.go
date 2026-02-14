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
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/c2h5oh/datasize"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/execution/types"
)

// PersistentBlockCollector stores downloaded blocks to an MDBX database
// so they survive restarts. The database is cleared after successful loading.
type PersistentBlockCollector struct {
	db             kv.RwDB
	persistDir     string
	beaconChainCfg *clparams.BeaconChainConfig
	logger         log.Logger
	engine         execution_client.ExecutionEngine
	syncBackLoop   uint64

	mu sync.Mutex
}

func openPersistentDB(ctx context.Context, logger log.Logger, persistDir string) (kv.RwDB, error) {
	return mdbx.New(kv.Label(dbcfg.CaplinDB), logger).
		Path(persistDir).
		WithTableCfg(func(_ kv.TableCfg) kv.TableCfg {
			return kv.TableCfg{
				kv.Headers: kv.TableCfgItem{},
			}
		}).
		GrowthStep(16 * datasize.MB).
		MapSize(1 * datasize.TB).
		Open(ctx)
}

// NewPersistentBlockCollector creates a new persistent block collector
// that stores blocks in an MDBX database at the given directory
func NewPersistentBlockCollector(
	logger log.Logger,
	engine execution_client.ExecutionEngine,
	beaconChainCfg *clparams.BeaconChainConfig,
	syncBackLoopAmount uint64,
	persistDir string,
) *PersistentBlockCollector {
	ctx := context.Background()
	db, err := openPersistentDB(ctx, logger, persistDir)
	if err != nil {
		logger.Error("[PersistentBlockCollector] Failed to open database", "err", err, "path", persistDir)
		return nil
	}

	go func() {
		<-ctx.Done()
		if db != nil {
			db.Close()
		}
	}()

	return &PersistentBlockCollector{
		db:             db,
		persistDir:     persistDir,
		beaconChainCfg: beaconChainCfg,
		logger:         logger,
		engine:         engine,
		syncBackLoop:   syncBackLoopAmount,
	}
}

// AddBlock adds a block to the collector, persisting it to the database
func (p *PersistentBlockCollector) AddBlock(block *cltypes.BeaconBlock) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.db == nil {
		return fmt.Errorf("database not initialized")
	}

	// Encode the block
	payload := block.Body.ExecutionPayload
	encodedBlock, err := encodeBlock(payload, block.ParentRoot, block.Body.GetExecutionRequestsList())
	if err != nil {
		return fmt.Errorf("failed to encode block: %w", err)
	}

	// Create key for sorting (block number + hash)
	key, err := payloadKey(payload)
	if err != nil {
		return fmt.Errorf("failed to create payload key: %w", err)
	}

	// Store in database (skip if already exists)
	return p.db.Update(context.Background(), func(tx kv.RwTx) error {
		return tx.Put(kv.Headers, key, encodedBlock)
	})
}

// Flush loads all collected blocks into the execution engine and clears the database
func (p *PersistentBlockCollector) Flush(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.db == nil {
		return fmt.Errorf("database not initialized")
	}

	blocksBatch := []*types.Block{}
	inserted := uint64(0)

	minInsertableBlockNumber := p.engine.FrozenBlocks(ctx)
	var prevBlockNum uint64
	if err := p.db.View(ctx, func(tx kv.Tx) error {
		cursor, err := tx.Cursor(kv.Headers)
		if err != nil {
			return err
		}
		defer cursor.Close()

		for k, v, err := cursor.First(); k != nil; k, v, err = cursor.Next() {
			if err != nil {
				return err
			}

			block, err := p.decodeBlock(v)
			if err != nil {
				p.logger.Warn("[BlockCollector] Failed to decode block", "key", common.Bytes2Hex(k), "err", err)
				continue
			}
			if block == nil {
				continue
			}
			if block.NumberU64() < minInsertableBlockNumber {
				continue
			}

			if prevBlockNum > 0 && block.NumberU64() != prevBlockNum+1 {
				panic(fmt.Sprintf("assert: BlockCollector inserting gap: %d -> %d. To fix try: `rm datadir/caplin/history datadir/chaindata`", prevBlockNum, block.NumberU64()))
			}
			prevBlockNum = block.NumberU64()
			blocksBatch = append(blocksBatch, block)

			if len(blocksBatch) >= batchSize {
				if err := p.insertBatch(ctx, blocksBatch, &inserted); err != nil {
					return err
				}
				blocksBatch = []*types.Block{}
			}
		}
		return nil
	}); err != nil {
		return fmt.Errorf("failed to flush blocks from database: %w", err)
	}

	// Insert remaining blocks
	if len(blocksBatch) > 0 {
		if err := p.insertBatch(ctx, blocksBatch, &inserted); err != nil {
			return err
		}
	}

	// Close, remove, and reopen the database to clear it
	p.db.Close()

	if err := dir.RemoveAll(p.persistDir); err != nil {
		p.logger.Warn("[BlockCollector] Failed to remove database directory", "err", err)
	}

	db, err := openPersistentDB(ctx, p.logger, p.persistDir)
	if err != nil {
		p.logger.Error("[BlockCollector] Failed to reopen database", "err", err)
		p.db = nil
		return fmt.Errorf("failed to reopen database: %w", err)
	}
	p.db = db

	p.logger.Info("[BlockCollector] Flush complete", "blocksInserted", inserted)

	return nil
}

func (p *PersistentBlockCollector) decodeBlock(v []byte) (*types.Block, error) {
	if len(v) == 0 {
		return nil, nil
	}

	v, err := utils.DecompressSnappy(v, false)
	if err != nil {
		return nil, err
	}

	version := clparams.StateVersion(v[0])
	parentRoot := common.BytesToHash(v[1:33])
	requestsHash := common.Hash{}

	if version >= clparams.ElectraVersion {
		requestsHash = common.BytesToHash(v[33:65])
		v = v[65:]
	} else {
		v = v[33:]
	}

	executionPayload := cltypes.NewEth1Block(version, p.beaconChainCfg)
	if err := executionPayload.DecodeSSZ(v, int(version)); err != nil {
		return nil, err
	}

	body := executionPayload.Body()
	txs, err := types.DecodeTransactions(body.Transactions)
	if err != nil {
		return nil, err
	}

	// Skip genesis block
	if executionPayload.BlockNumber == 0 {
		return nil, nil
	}

	header, err := executionPayload.RlpHeader(&parentRoot, requestsHash)
	if err != nil {
		return nil, err
	}

	return types.NewBlockFromStorage(executionPayload.BlockHash, header, txs, nil, body.Withdrawals), nil
}

func (p *PersistentBlockCollector) insertBatch(ctx context.Context, blocksBatch []*types.Block, inserted *uint64) error {
	p.logger.Info("[BlockCollector] Inserting blocks",
		"from", blocksBatch[0].NumberU64(),
		"to", blocksBatch[len(blocksBatch)-1].NumberU64())

	if err := p.engine.InsertBlocks(ctx, blocksBatch, true); err != nil {
		p.logger.Warn("[BlockCollector] Failed to insert blocks", "err", err)
		return err
	}

	*inserted += uint64(len(blocksBatch))
	p.logger.Info("[BlockCollector] Inserted blocks", "progress", blocksBatch[len(blocksBatch)-1].NumberU64())

	lastBlockHash := blocksBatch[len(blocksBatch)-1].Hash()
	currentHeader, err := p.engine.CurrentHeader(ctx)
	if err != nil {
		p.logger.Warn("[BlockCollector] Failed to get current header", "err", err)
	}

	isForkchoiceNeeded := currentHeader == nil || blocksBatch[len(blocksBatch)-1].NumberU64() > currentHeader.Number.Uint64()
	if *inserted >= p.syncBackLoop {
		if isForkchoiceNeeded {
			if _, err := p.engine.ForkChoiceUpdate(ctx, lastBlockHash, lastBlockHash, lastBlockHash, nil); err != nil {
				p.logger.Warn("[BlockCollector] Failed to update fork choice", "err", err)
			}
		}
		*inserted = 0
	}

	return nil
}

// HasBlock checks if a block with the given number is already in the collector
func (p *PersistentBlockCollector) HasBlock(blockNumber uint64) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.db == nil {
		return false
	}

	var hasBlock bool
	if err := p.db.View(context.Background(), func(tx kv.Tx) error {
		cursor, err := tx.Cursor(kv.Headers)
		if err != nil {
			return err
		}
		defer cursor.Close()
		// Keys are prefixed with block number (8 bytes big-endian)
		prefix := make([]byte, 8)
		binary.BigEndian.PutUint64(prefix, blockNumber)
		k, _, err := cursor.Seek(prefix)
		if err != nil {
			return err
		}
		// Check if the key starts with our block number
		hasBlock = len(k) >= 8 && binary.BigEndian.Uint64(k[:8]) == blockNumber
		return nil
	}); err != nil {
		p.logger.Warn("[BlockCollector] Failed to check for block", "err", err)
	}

	return hasBlock
}

// Close closes the database
func (p *PersistentBlockCollector) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.db != nil {
		p.db.Close()
		p.db = nil
	}
	return nil
}
