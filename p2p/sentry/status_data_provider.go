package sentry

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"sync"

	"github.com/holiman/uint256"

	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	proto_sentry "github.com/ledgerwatch/erigon-lib/gointerfaces/sentry"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/core/forkid"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
)

type ChainHead struct {
	HeadHeight uint64
	HeadTime   uint64
	HeadHash   libcommon.Hash
	HeadTd     *uint256.Int
}

type StatusDataProvider struct {
	ChainHead

	db kv.RoDB

	networkId   uint64
	genesisHash libcommon.Hash
	heightForks []uint64
	timeForks   []uint64

	lock sync.RWMutex
}

func NewStatusDataProvider(
	ctx context.Context,
	db kv.RoDB,
	chainConfig *chain.Config,
	genesis *types.Block,
	networkId uint64,
) (*StatusDataProvider, error) {
	chainHead, err := ReadChainHead(ctx, db)
	if err != nil {
		return nil, err
	}

	s := &StatusDataProvider{
		ChainHead:   chainHead,
		db:          db,
		networkId:   networkId,
		genesisHash: genesis.Hash(),
	}

	s.heightForks, s.timeForks = forkid.GatherForks(chainConfig, genesis.Time())

	return s, nil
}

func (s *StatusDataProvider) MakeStatusData() *proto_sentry.StatusData {
	s.lock.RLock()
	defer s.lock.RUnlock()

	return &proto_sentry.StatusData{
		NetworkId:       s.networkId,
		TotalDifficulty: gointerfaces.ConvertUint256IntToH256(s.HeadTd),
		BestHash:        gointerfaces.ConvertHashToH256(s.HeadHash),
		MaxBlockHeight:  s.HeadHeight,
		MaxBlockTime:    s.HeadTime,
		ForkData: &proto_sentry.Forks{
			Genesis:     gointerfaces.ConvertHashToH256(s.genesisHash),
			HeightForks: s.heightForks,
			TimeForks:   s.timeForks,
		},
	}
}

func (s *StatusDataProvider) setHead(chainHead ChainHead) {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.ChainHead = chainHead
}

func (s *StatusDataProvider) RefreshHead(ctx context.Context) error {
	chainHead, err := ReadChainHead(ctx, s.db)
	if err != nil {
		return err
	}
	s.setHead(chainHead)
	return nil
}

func (s *StatusDataProvider) RefreshStatusData(ctx context.Context) (*proto_sentry.StatusData, error) {
	err := s.RefreshHead(ctx)
	return s.MakeStatusData(), err
}

func ReadChainHeadWithTx(tx kv.Tx) (ChainHead, error) {
	header := rawdb.ReadCurrentHeader(tx)
	if header == nil {
		return ChainHead{}, errors.New("ReadChainHead: ReadCurrentHeader error")
	}

	height := header.Number.Uint64()
	hash := header.Hash()

	var time uint64
	if header != nil {
		time = header.Time
	}

	td, err := rawdb.ReadTd(tx, hash, height)
	if err != nil {
		return ChainHead{}, fmt.Errorf("ReadChainHead: ReadTd error at height %d and hash %s: %w", height, hash, err)
	}
	if td == nil {
		td = new(big.Int)
	}
	td256 := new(uint256.Int)
	overflow := td256.SetFromBig(td)
	if overflow {
		return ChainHead{}, fmt.Errorf("ReadChainHead: total difficulty higher than 2^256-1")
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
