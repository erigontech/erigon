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

package sentry

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"sync/atomic"
	"time"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/services"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/sentryproto"
	"github.com/erigontech/erigon/p2p/forkid"
)

const statusDataCacheTTL = 2 * time.Second

var (
	ErrNoHead      = errors.New("ReadChainHead: ReadCurrentHeader error")
	ErrNoSnapshots = errors.New("ReadChainHeadFromSnapshots: no snapshot data available")
)

type ChainHead struct {
	HeadHeight    uint64
	HeadTime      uint64
	HeadHash      common.Hash
	MinimumHeight uint64
	HeadTd        *uint256.Int
}

// cachedChainHead holds a cached ChainHead with its fetch time.
// We cache ChainHead (not the protobuf) so that makeStatusData builds a fresh
// *sentryproto.StatusData per call, avoiding shared-mutable-protobuf issues.
type cachedChainHead struct {
	head      ChainHead
	fetchedAt time.Time
}

type StatusDataProvider struct {
	db          kv.RoDB
	blockReader services.FullBlockReader

	networkId   uint64
	genesisHash common.Hash
	genesisHead ChainHead
	heightForks []uint64
	timeForks   []uint64

	logger log.Logger

	// Cache: double-checked locking to coalesce concurrent DB reads.
	// refreshSem is a capacity-1 channel used as a context-aware mutex:
	// sending acquires; receiving releases. This lets waiters honour ctx
	// cancellation instead of blocking unconditionally on sync.Mutex.
	cache      atomic.Pointer[cachedChainHead]
	refreshSem chan struct{}
}

func NewStatusDataProvider(
	db kv.RoDB,
	chainConfig *chain.Config,
	genesis *types.Block,
	networkId uint64,
	logger log.Logger,
	blockReader services.FullBlockReader,
) *StatusDataProvider {
	s := &StatusDataProvider{
		db:          db,
		blockReader: blockReader,
		networkId:   networkId,
		genesisHash: genesis.Hash(),
		genesisHead: makeGenesisChainHead(genesis),
		logger:      logger,
		refreshSem:  make(chan struct{}, 1),
	}

	s.heightForks, s.timeForks = forkid.GatherForks(chainConfig, genesis.Time())

	return s
}

func uint256FromBigInt(num *big.Int) (*uint256.Int, error) {
	if num == nil {
		num = new(big.Int)
	}
	num256 := new(uint256.Int)
	overflow := num256.SetFromBig(num)
	if overflow {
		return nil, errors.New("uint256FromBigInt: big.Int greater than 2^256-1")
	}
	return num256, nil
}

func makeGenesisChainHead(genesis *types.Block) ChainHead {
	genesisDifficulty := genesis.Difficulty()

	return ChainHead{
		HeadHeight:    genesis.NumberU64(),
		HeadTime:      genesis.Time(),
		HeadHash:      genesis.Hash(),
		MinimumHeight: genesis.NumberU64(),
		HeadTd:        &genesisDifficulty,
	}
}

func (s *StatusDataProvider) makeStatusData(head ChainHead) *sentryproto.StatusData {
	return &sentryproto.StatusData{
		NetworkId:          s.networkId,
		TotalDifficulty:    gointerfaces.ConvertUint256IntToH256(head.HeadTd),
		BestHash:           gointerfaces.ConvertHashToH256(head.HeadHash),
		MaxBlockHeight:     head.HeadHeight,
		MaxBlockTime:       head.HeadTime,
		MinimumBlockHeight: head.MinimumHeight,
		ForkData: &sentryproto.Forks{
			Genesis:     gointerfaces.ConvertHashToH256(s.genesisHash),
			HeightForks: s.heightForks,
			TimeForks:   s.timeForks,
		},
	}
}

// GetStatusData returns the current StatusData.
//
// The ChainHead is cached for statusDataCacheTTL (2s). A fresh
// *sentryproto.StatusData is built per call so callers may safely mutate
// the returned protobuf without corrupting a shared object.
//
// Concurrent callers share a single DB read via double-checked locking:
// the first goroutine to find an expired cache acquires refreshSem and
// refreshes; others wait in a ctx-aware select and reuse the refreshed
// value. Callers whose context is cancelled while waiting get ctx.Err()
// instead of blocking indefinitely.
//
// The two former db.View() calls (MinimumBlockAvailable + ReadChainHead)
// are merged into a single read transaction to halve MDBX reader pressure.
//
// Falls back to snapshot data when the DB head is unavailable.
func (s *StatusDataProvider) GetStatusData(ctx context.Context) (*sentryproto.StatusData, error) {
	// Fast path: serve from cache if fresh (lock-free).
	if c := s.cache.Load(); c != nil && time.Since(c.fetchedAt) < statusDataCacheTTL {
		return s.makeStatusData(c.head), nil
	}

	// Slow path: acquire refresh semaphore, respecting ctx cancellation.
	select {
	case s.refreshSem <- struct{}{}:
		// We are the refresher.
		defer func() { <-s.refreshSem }()
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	// Double-check: another goroutine may have refreshed while we waited.
	if c := s.cache.Load(); c != nil && time.Since(c.fetchedAt) < statusDataCacheTTL {
		return s.makeStatusData(c.head), nil
	}

	head, err := s.fetchChainHead(ctx)
	if err != nil {
		return nil, err
	}

	s.cache.Store(&cachedChainHead{head: head, fetchedAt: time.Now()})
	return s.makeStatusData(head), nil
}

// fetchChainHead reads MinimumBlockAvailable and ChainHead in a single DB
// read transaction. Falls back to snapshot data when the DB head is missing.
func (s *StatusDataProvider) fetchChainHead(ctx context.Context) (ChainHead, error) {
	var (
		chainHead    ChainHead
		minimumBlock uint64
		headErr      error
	)

	if err := s.db.View(ctx, func(tx kv.Tx) error {
		var err error
		minimumBlock, err = s.blockReader.MinimumBlockAvailable(ctx, tx)
		if err != nil {
			return fmt.Errorf("MinimumBlockAvailable: %w", err)
		}
		chainHead, headErr = ReadChainHeadWithTx(tx, minimumBlock)
		return nil // headErr handled below (ErrNoHead → snapshot fallback)
	}); err != nil {
		return ChainHead{}, fmt.Errorf("GetStatusData: %w", err)
	}

	if headErr == nil {
		return chainHead, nil
	}
	if !errors.Is(headErr, ErrNoHead) {
		return ChainHead{}, headErr
	}

	s.logger.Warn("sentry.StatusDataProvider: The canonical chain current header not found in the database. Check the database consistency. Using latest available snapshot data.")

	snapHead, err := s.ReadChainHeadFromSnapshots(ctx, minimumBlock)
	if err != nil {
		return ChainHead{}, fmt.Errorf("failed to read chain head from snapshots: %w", err)
	}
	return snapHead, nil
}

// ReadChainHeadWithTx reads chain head in DB
func ReadChainHeadWithTx(tx kv.Tx, minimumBlock uint64) (ChainHead, error) {
	header := rawdb.ReadCurrentHeaderHavingBody(tx)
	if header == nil {
		return ChainHead{}, ErrNoHead
	}

	height := header.Number.Uint64()
	hash := header.Hash()
	time := header.Time

	td, err := rawdb.ReadTd(tx, hash, height)
	if err != nil {
		return ChainHead{}, fmt.Errorf("ReadChainHead: ReadTd error at height %d and hash %s: %w", height, hash, err)
	}
	td256, err := uint256FromBigInt(td)
	if err != nil {
		return ChainHead{}, fmt.Errorf("ReadChainHead: total difficulty conversion error: %w", err)
	}

	return ChainHead{height, time, hash, minimumBlock, td256}, nil
}

func ReadChainHead(ctx context.Context, db kv.RoDB, minimumBlock uint64) (ChainHead, error) {
	var head ChainHead
	var err error
	err = db.View(ctx, func(tx kv.Tx) error {
		head, err = ReadChainHeadWithTx(tx, minimumBlock)
		return err
	})
	return head, err
}

// ReadChainHeadFromSnapshots attempts to construct a ChainHead from snapshot data.
func (s *StatusDataProvider) ReadChainHeadFromSnapshots(ctx context.Context, minimumBlock uint64) (ChainHead, error) {
	latest := s.blockReader.FrozenBlocks()
	if latest == 0 {
		return ChainHead{}, ErrNoSnapshots
	}

	header, err := s.blockReader.HeaderByNumber(ctx, nil, latest)
	if err != nil || header == nil {
		return ChainHead{}, fmt.Errorf("failed reading snapshot header %d: %w", latest, err)
	}

	return ChainHead{
		HeadHeight:    header.Number.Uint64(),
		HeadTime:      header.Time,
		HeadHash:      header.Hash(),
		MinimumHeight: minimumBlock,
		HeadTd:        &header.Difficulty,
	}, nil
}
