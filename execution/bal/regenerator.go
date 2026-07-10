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
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/services"
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
	blockReader    services.FullBlockReader
	txNumReader    rawdbv3.TxNumsReader
	engine         rules.Engine
	cache          *lru.Cache[common.Hash, []byte]
	perBlockExecMu *loaderMutex[common.Hash]
	execSem        chan struct{} // bounds concurrent block replays
	logger         log.Logger
}

func NewRegenerator(blockReader services.FullBlockReader, engine rules.Engine, logger log.Logger) *Regenerator {
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
// replayed), and state.PrunedError-wrapped errors when the required history is
// no longer available. The returned bytes are shared with the internal cache
// and must be treated as read-only.
func (g *Regenerator) GetBlockAccessListBytes(ctx context.Context, cfg *chain.Config, tx kv.TemporalTx, blockHash common.Hash, blockNum uint64) ([]byte, error) {
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
	mu := g.perBlockExecMu.lock(blockHash)
	defer g.perBlockExecMu.unlock(mu, blockHash)
	if cached, ok := g.cache.Get(blockHash); ok {
		return cached, nil
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
	reader, err := g.historyStateReader(ctx, tx, blockNum)
	if err != nil {
		return nil, err
	}
	ibs := state.New(reader)
	ibs.SetVersionMap(state.NewVersionMap(nil))
	getHeader := func(hash common.Hash, number uint64) (*types.Header, error) {
		return g.blockReader.Header(ctx, tx, hash, number)
	}
	chainReader := exec.NewChainReader(cfg, tx, g.blockReader, g.logger)
	bal, err := RederiveBlockAccessList(ctx, cfg, g.engine, chainReader, reader, getHeader, header, block.Transactions(), block.Uncles(), block.Withdrawals(), ibs, g.logger)
	if err != nil {
		return nil, err
	}
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

// historyStateReader positions a history reader at the block's first txNum,
// i.e. before its init system call, so the whole block can be replayed.
func (g *Regenerator) historyStateReader(ctx context.Context, tx kv.TemporalTx, blockNum uint64) (state.StateReader, error) {
	minTxNum, err := g.txNumReader.Min(ctx, tx, blockNum)
	if err != nil {
		return nil, err
	}
	if minHistoryTxNum := state.StateHistoryStartTxNum(tx); minTxNum < minHistoryTxNum {
		firstAvailBlock, _, _ := g.txNumReader.FindBlockNum(ctx, tx, minHistoryTxNum)
		return nil, fmt.Errorf("%w: requested block %d, history is available from block %d", state.PrunedError, blockNum, firstAvailBlock)
	}
	return state.NewHistoryReaderV3(tx, minTxNum), nil
}

type loaderMutex[K comparable] struct {
	sync.Map
}

func (m *loaderMutex[K]) lock(key K) *sync.Mutex {
	value, _ := m.LoadOrStore(key, &sync.Mutex{})
	mu := value.(*sync.Mutex)
	mu.Lock()
	return mu
}

func (m *loaderMutex[K]) unlock(mu *sync.Mutex, key K) {
	mu.Unlock()
	m.Delete(key)
}
