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
	"sync"
	"sync/atomic"

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

type StatusDataProvider struct {
	db          kv.RoDB
	blockReader services.FullBlockReader

	networkId   uint64
	genesisHash common.Hash
	genesisHead ChainHead
	heightForks []uint64
	timeForks   []uint64

	logger log.Logger

	// cache holds the latest ChainHead, invalidated by new-header
	// notifications from the execution pipeline (via Run).
	// Protected by cacheMu for concurrent fetch coalescing.
	// cacheVer is bumped on each invalidation so that a fetch in
	// progress doesn't store stale data after a concurrent invalidation.
	cache    atomic.Pointer[ChainHead]
	cacheMu  sync.Mutex
	cacheVer atomic.Uint64
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
			HeightForks: append([]uint64(nil), s.heightForks...),
			TimeForks:   append([]uint64(nil), s.timeForks...),
		},
	}
}

// Run listens for new-header notifications and invalidates the cached
// ChainHead so the next GetStatusData call fetches fresh data from the DB.
// Blocks until ctx is cancelled.
func (s *StatusDataProvider) Run(ctx context.Context, headersCh <-chan [][]byte, snapshotsCh <-chan struct{}) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-headersCh:
			s.cacheVer.Add(1)
			s.cache.Store(nil)
		case <-snapshotsCh:
			s.cacheVer.Add(1)
			s.cache.Store(nil)
		}
	}
}

// GetStatusData returns the current StatusData.
//
// The ChainHead is cached and invalidated by new-header notifications from
// the execution pipeline (via Run). Concurrent callers share a single DB
// fetch through cacheMu. This eliminates repeated MDBX read transactions
// that previously blocked GC page reclamation.
//
// Falls back to snapshot data when the DB head is unavailable.
func (s *StatusDataProvider) GetStatusData(ctx context.Context) (*sentryproto.StatusData, error) {
	if head := s.cache.Load(); head != nil {
		return s.makeStatusData(*head), nil
	}

	// Cache miss — fetch from DB, coalescing concurrent callers.
	s.cacheMu.Lock()
	defer s.cacheMu.Unlock()

	// Double-check after acquiring lock.
	if head := s.cache.Load(); head != nil {
		return s.makeStatusData(*head), nil
	}

	ver := s.cacheVer.Load()
	head, err := s.fetchChainHead(ctx)
	if err != nil {
		return nil, err
	}

	// Only store if no invalidation happened during the fetch.
	if s.cacheVer.Load() == ver {
		s.cache.Store(&head)
	}
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
