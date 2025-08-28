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

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/gointerfaces"
	proto_sentry "github.com/erigontech/erigon-lib/gointerfaces/sentryproto"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/p2p/forkid"
)

var ErrNoHead = errors.New("ReadChainHead: ReadCurrentHeader error")

type ChainHead struct {
	HeadHeight uint64
	HeadTime   uint64
	HeadHash   common.Hash
	HeadTd     *uint256.Int
}

type StatusDataProvider struct {
	db kv.RoDB

	networkId   uint64
	genesisHash common.Hash
	genesisHead ChainHead
	heightForks []uint64
	timeForks   []uint64

	logger log.Logger
}

func NewStatusDataProvider(
	db kv.RoDB,
	chainConfig *chain.Config,
	genesis *types.Block,
	networkId uint64,
	logger log.Logger,
) *StatusDataProvider {
	s := &StatusDataProvider{
		db:          db,
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
	genesisDifficulty, err := uint256FromBigInt(genesis.Difficulty())
	if err != nil {
		panic(fmt.Errorf("makeGenesisChainHead: difficulty conversion error: %w", err))
	}

	return ChainHead{
		HeadHeight: genesis.NumberU64(),
		HeadTime:   genesis.Time(),
		HeadHash:   genesis.Hash(),
		HeadTd:     genesisDifficulty,
	}
}

func (s *StatusDataProvider) makeStatusData(head ChainHead) *proto_sentry.StatusData {
	return &proto_sentry.StatusData{
		NetworkId:       s.networkId,
		TotalDifficulty: gointerfaces.ConvertUint256IntToH256(head.HeadTd),
		BestHash:        gointerfaces.ConvertHashToH256(head.HeadHash),
		MaxBlockHeight:  head.HeadHeight,
		MaxBlockTime:    head.HeadTime,
		ForkData: &proto_sentry.Forks{
			Genesis:     gointerfaces.ConvertHashToH256(s.genesisHash),
			HeightForks: s.heightForks,
			TimeForks:   s.timeForks,
		},
	}
}

func (s *StatusDataProvider) GetStatusData(ctx context.Context) (*proto_sentry.StatusData, error) {
	chainHead, err := ReadChainHead(ctx, s.db)
	if err != nil {
		if errors.Is(err, ErrNoHead) {
			s.logger.Warn("sentry.StatusDataProvider: The canonical chain current header not found in the database. Check the database consistency. Using genesis as a fallback.")
			return s.makeStatusData(s.genesisHead), nil
		}
		return nil, err
	}
	return s.makeStatusData(chainHead), err
}

func ReadChainHeadWithTx(tx kv.Tx) (ChainHead, error) {
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

	return ChainHead{height, time, hash, td256}, nil
}

func ReadChainHead(ctx context.Context, db kv.RoDB) (ChainHead, error) {
	var head ChainHead
	var err error
	err = db.View(ctx, func(tx kv.Tx) error {
		head, err = ReadChainHeadWithTx(tx)
		return err
	})
	return head, err
}
