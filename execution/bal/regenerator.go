// Copyright 2026 The Erigon Authors
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

package bal

import (
	"context"
	"fmt"
	"runtime"
	"sync"

	lru "github.com/hashicorp/golang-lru/v2"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/dbservices"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/exec"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
)

var (
	balsCacheLimit      = dbg.EnvInt("BAL_LRU", 1024) // ~100KiB avg encoded BAL → ~100MB RAM per Regenerator instance
	balsExecConcurrency = max(1, dbg.EnvInt("BAL_EXEC_CONCURRENCY", runtime.GOMAXPROCS(0)/2))
)

// Regenerator reproduces Block Access Lists for blocks whose stored copy has
// been pruned, by re-executing them against historical state. It mirrors
// rpc/jsonrpc/receipts.Generator: caching and history-env preparation live
// here, the replay itself is RederiveBlockAccessList.
type Regenerator struct {
	blockReader    dbservices.FullBlockReader
	txNumReader    rawdbv3.TxNumsReader
	engine         rules.Engine
	cache          *lru.Cache[common.Hash, []byte]
	perBlockExecMu *loaderMutex[common.Hash]
	execSem        chan struct{} // bounds concurrent block replays
	logger         log.Logger
}

func NewRegenerator(blockReader dbservices.FullBlockReader, engine rules.Engine, logger log.Logger) *Regenerator {
	cache, err := lru.New[common.Hash, []byte](balsCacheLimit)
	if err != nil {
		panic(err)
	}
	return &Regenerator{
		blockReader:    blockReader,
		txNumReader:    blockReader.TxnumReader(),
		engine:         engine,
		cache:          cache,
		perBlockExecMu: &loaderMutex[common.Hash]{},
		execSem:        make(chan struct{}, balsExecConcurrency),
		logger:         logger,
	}
}

// GetBlockAccessListBytes returns the canonical RLP-encoded Block Access List
// for the given block, regenerating it via re-execution against historical
// state. Returns (nil, nil) for blocks without a BAL commitment in the header
// (pre-Amsterdam) and for non-canonical blocks (only canonical history can be
// replayed), and state.ErrPruned-wrapped errors when the required history is
// no longer available. The returned bytes are shared with the internal cache
// and must be treated as read-only.
// If set, beforeReplay must succeed before replay starts. Cache hits and
// rejected preflight checks do not call it.
func (g *Regenerator) GetBlockAccessListBytes(ctx context.Context, cfg *chain.Config, tx kv.TemporalTx, blockHash common.Hash, blockNum uint64, beforeReplay func() error) ([]byte, error) {
	if cached, ok := g.cache.Get(blockHash); ok {
		return cached, nil
	}
	header, err := g.blockReader.Header(ctx, tx, blockHash, blockNum)
	if err != nil {
		return nil, err
	}
	if header == nil || header.BlockAccessListHash == nil {
		return nil, nil
	}
	canonicalHash, ok, err := g.blockReader.CanonicalHash(ctx, tx, blockNum)
	if err != nil {
		return nil, err
	}
	if !ok || canonicalHash != blockHash {
		return nil, nil
	}
	done, err := g.perBlockExecMu.lock(ctx, blockHash)
	if err == nil {
		defer g.perBlockExecMu.unlock(done, blockHash)
	}
	// Completed cached results remain usable after cancellation.
	if cached, ok := g.cache.Get(blockHash); ok {
		return cached, nil
	}
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// Check body metadata before admission; loading transactions is part of the limited work.
	body, err := g.blockReader.CanonicalBodyForStorage(ctx, tx, blockNum)
	if err != nil {
		return nil, err
	}
	if body == nil {
		return nil, nil
	}
	blockStartTxNum, err := g.historyStartTxNum(ctx, tx, blockNum)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if beforeReplay != nil {
		if err := beforeReplay(); err != nil {
			return nil, err
		}
	}
	select {
	case g.execSem <- struct{}{}:
		defer func() { <-g.execSem }()
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	block, _, err := g.blockReader.BlockWithSenders(ctx, tx, blockHash, blockNum)
	if err != nil {
		return nil, err
	}
	if block == nil {
		return nil, nil
	}
	vio, err := RederiveFromHistory(ctx, cfg, g.engine, g.blockReader, tx, block, blockStartTxNum, g.logger)
	if err != nil {
		return nil, err
	}
	bal := vio.AsBlockAccessList()
	computedHash := bal.Hash()
	if computedHash != *header.BlockAccessListHash {
		// A canonical block whose replay diverges from its commitment is a
		// re-derivation bug: keep it observable but degrade to unavailable.
		g.logger.Warn("bal.Regenerator: re-derived BAL hash mismatch", "block", blockNum, "hash", blockHash, "got", computedHash, "want", *header.BlockAccessListHash)
		return nil, nil
	}
	encoded, err := types.EncodeBlockAccessListBytes(bal)
	if err != nil {
		return nil, err
	}
	g.cache.Add(blockHash, encoded)
	return encoded, nil
}

// historyStartTxNum returns the block's first txNum, i.e. the txNum of its init
// system call, once the state history there is available.
func (g *Regenerator) historyStartTxNum(ctx context.Context, tx kv.TemporalTx, blockNum uint64) (uint64, error) {
	minTxNum, err := g.txNumReader.Min(ctx, tx, blockNum)
	if err != nil {
		return 0, err
	}
	minHistoryTxNum, err := state.StateHistoryStartTxNum(tx)
	if err != nil {
		return 0, err
	}
	if minTxNum < minHistoryTxNum {
		firstAvailBlock, _, _ := g.txNumReader.FindBlockNum(ctx, tx, minHistoryTxNum)
		return 0, fmt.Errorf("%w: requested block %d, history is available from block %d", state.ErrPruned, blockNum, firstAvailBlock)
	}
	return minTxNum, nil
}

// RederiveFromHistory replays block on the state history at blockStartTxNum,
// the txNum of its init system call, and returns the I/O its access list is
// built from.
func RederiveFromHistory(ctx context.Context, cfg *chain.Config, engine rules.Engine, blockReader dbservices.FullBlockReader, tx kv.TemporalTx, block *types.Block, blockStartTxNum uint64, logger log.Logger) (*state.VersionedIO, error) {
	ibs := state.New(state.NewHistoryReaderV3(tx, blockStartTxNum))
	defer ibs.Close()
	ibs.SetVersionMap(state.NewVersionMap(nil))
	getHeader := func(hash common.Hash, number uint64) (*types.Header, error) {
		return blockReader.Header(ctx, tx, hash, number)
	}
	chainReader := exec.NewChainReader(cfg, tx, blockReader, logger)
	return RederiveBlockAccessList(ctx, cfg, engine, chainReader, getHeader, block.HeaderNoCopy(), block.Transactions(), block.Uncles(), block.Withdrawals(), ibs, logger)
}

type loaderMutex[K comparable] struct {
	sync.Map
}

func (m *loaderMutex[K]) lock(ctx context.Context, key K) (chan struct{}, error) {
	done := make(chan struct{})
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		value, loaded := m.LoadOrStore(key, done)
		if !loaded {
			return done, nil
		}
		select {
		case <-value.(chan struct{}):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (m *loaderMutex[K]) unlock(done chan struct{}, key K) {
	m.Delete(key)
	close(done)
}
