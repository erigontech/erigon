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

// Flush loads all collected blocks into the execution engine and clears the database.
// Keys are block-number + payload SSZ root. Identical execution payloads therefore
// collide on payloadKey and tx.Put overwrites the existing row, so multiple rows at
// the same block number only exist when the execution payload itself differs (that
// is, competing execution forks at the same height). The variant chosen is the one
// whose BlockHash matches the ParentHash of the next row — a single-row look-ahead.
// If a real gap is detected, rows past the gap are kept so the next Flush can retry
// once the missing range is re-downloaded.
func (p *PersistentBlockCollector) Flush(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.db == nil {
		return fmt.Errorf("database not initialized")
	}

	blocksBatch := []*types.Block{}
	inserted := uint64(0)

	minInsertableBlockNumber := p.engine.FrozenBlocks(ctx)
	var pending []*types.Block // variants at pendingHeight, awaiting resolution
	var pendingHeight uint64
	var lastCommittedHeight uint64
	gapDetected := false

	// resolvePending picks the variant from `pending` whose BlockHash matches
	// next.ParentHash. With one variant (no ambiguity) or next == nil (end of
	// cursor, nothing to match against), the first variant is returned. Returns
	// nil only when pending has multiple variants and none chains onto next.
	resolvePending := func(next *types.Block) *types.Block {
		if len(pending) == 0 {
			return nil
		}
		if len(pending) == 1 || next == nil {
			return pending[0]
		}
		for _, c := range pending {
			if c.Hash() == next.ParentHash() {
				return c
			}
		}
		return nil
	}

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

			// Another variant at the current height: buffer it for look-ahead resolution.
			if pendingHeight > 0 && block.NumberU64() == pendingHeight {
				pending = append(pending, block)
				continue
			}

			// Different height. If not the immediate successor, it's a real gap —
			// we can't use this block to disambiguate competing variants at pendingHeight.
			if pendingHeight > 0 && block.NumberU64() != pendingHeight+1 {
				// Commit the pending group only if it's unambiguous. With multiple
				// variants and no successor to match against, leave rows for retry.
				if len(pending) == 1 {
					blocksBatch = append(blocksBatch, pending[0])
					lastCommittedHeight = pendingHeight
				}
				p.logger.Warn("[BlockCollector] Gap detected in collected blocks, will re-download missing range",
					"lastBlock", pendingHeight, "nextBlock", block.NumberU64(),
					"gap", block.NumberU64()-pendingHeight-1)
				gapDetected = true
				break
			}

			// Immediate successor: resolve the pending group against this block's parent.
			if pendingHeight > 0 {
				resolved := resolvePending(block)
				if resolved == nil {
					p.logger.Warn("[BlockCollector] Fork detected: no stored variant matches next block's parent, leaving rows for retry",
						"height", pendingHeight, "nextBlock", block.NumberU64(), "variants", len(pending))
					gapDetected = true
					break
				}
				blocksBatch = append(blocksBatch, resolved)
				lastCommittedHeight = pendingHeight
				if len(blocksBatch) >= batchSize {
					if err := p.insertBatch(ctx, blocksBatch, &inserted); err != nil {
						return err
					}
					blocksBatch = []*types.Block{}
				}
			}

			pending = []*types.Block{block}
			pendingHeight = block.NumberU64()
		}

		// End of cursor: resolve the final pending group with no successor to match
		// against. Single variants are unambiguous. With multiple variants we can't
		// disambiguate, so leave them for a future Flush (same policy as the
		// mid-cursor gap branch) rather than guessing a pick the clean-path DB wipe
		// could permanently discard.
		if !gapDetected && pendingHeight > 0 {
			if len(pending) == 1 {
				blocksBatch = append(blocksBatch, pending[0])
				lastCommittedHeight = pendingHeight
			} else {
				p.logger.Warn("[BlockCollector] Fork at final height with no successor, leaving rows for retry",
					"height", pendingHeight, "variants", len(pending))
				gapDetected = true
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

	if gapDetected {
		// Prune only rows the caller is done with; rows past the gap stay so a
		// future re-download of the missing range unblocks the next Flush.
		// Use a non-cancelable context: if ctx was cancelled the caller cares
		// about stopping, but skipping cleanup would leave already-inserted
		// rows in place and the next Flush would re-read and re-insert them.
		cutoff := minInsertableBlockNumber
		if lastCommittedHeight+1 > cutoff {
			cutoff = lastCommittedHeight + 1
		}
		if err := p.db.Update(context.Background(), func(tx kv.RwTx) error {
			cursor, err := tx.RwCursor(kv.Headers)
			if err != nil {
				return err
			}
			defer cursor.Close()
			for k, _, err := cursor.First(); k != nil; k, _, err = cursor.Next() {
				if err != nil {
					return err
				}
				if len(k) < 8 {
					// Defensive: payloadKey always produces 40-byte keys.
					continue
				}
				if binary.BigEndian.Uint64(k[:8]) >= cutoff {
					break
				}
				if err := cursor.DeleteCurrent(); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			p.logger.Warn("[BlockCollector] Failed to prune consumed blocks", "err", err)
		}
		return nil
	}

	// No gap: drop the whole DB — cheaper than walking keys.
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
			// DenebVersion maps to ForkchoiceUpdatedV3 which is valid for Deneb through Fulu.
			// The block collector only runs during initial sync, not for Gloas+ (Amsterdam) blocks yet.
			if _, err := p.engine.ForkChoiceUpdate(ctx, lastBlockHash, lastBlockHash, lastBlockHash, nil, clparams.DenebVersion); err != nil {
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
