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

	// Cached status data — avoids opening MDBX readers on every
	// peer status exchange. Valid until the next expected block
	// (headTime + 12s slot duration).
	cacheMu      sync.Mutex
	cachedStatus *sentryproto.StatusData
	cacheExpiry  time.Time // when the cached data becomes stale
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
			HeightForks: s.heightForks,
			TimeForks:   s.timeForks,
		},
	}
}

// GetStatusData returns the current StatusData.
// Results are cached until the next expected block (headTime + 12s)
// to avoid opening MDBX readers on every peer status exchange.
func (s *StatusDataProvider) GetStatusData(ctx context.Context) (*sentryproto.StatusData, error) {
	s.cacheMu.Lock()
	if s.cachedStatus != nil && time.Now().Before(s.cacheExpiry) {
		cached := s.cachedStatus
		s.cacheMu.Unlock()
		return cached, nil
	}
	s.cacheMu.Unlock()

	var minimumBlock uint64
	if err := s.db.View(ctx, func(tx kv.Tx) error {
		var err error
		minimumBlock, err = s.blockReader.MinimumBlockAvailable(ctx, tx)
		return err
	}); err != nil {
		return nil, fmt.Errorf("GetStatusData: minimumBlock error: %w", err)
	}

	var result *sentryproto.StatusData

	chainHead, err := ReadChainHead(ctx, s.db, minimumBlock)
	if err == nil {
		result = s.makeStatusData(chainHead)
	} else if !errors.Is(err, ErrNoHead) {
		return nil, err
	} else {
		s.logger.Warn("sentry.StatusDataProvider: The canonical chain current header not found in the database. Check the database consistency. Using latest available snapshot data.")
		snapHead, err := s.ReadChainHeadFromSnapshots(ctx, minimumBlock)
		if err != nil {
			return nil, fmt.Errorf("failed to read chain head from snapshots: %w", err)
		}
		result = s.makeStatusData(snapHead)
	}

	// Cache duration depends on how close we are to the chain tip.
	// At tip (headTime within 60s of now): 12s (one slot).
	// During initial sync (headTime far in the past): 60s.
	cacheDuration := 60 * time.Second
	if result.MaxBlockTime > 0 && time.Since(time.Unix(int64(result.MaxBlockTime), 0)) < 60*time.Second {
		cacheDuration = 12 * time.Second
	}
	s.cacheMu.Lock()
	s.cachedStatus = result
	s.cacheExpiry = time.Now().Add(cacheDuration)
	s.cacheMu.Unlock()

	return result, nil
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
