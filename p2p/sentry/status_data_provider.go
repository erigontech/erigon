package sentry

import (
	"context"
	"errors"
	"fmt"
	"math/big"

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

var ErrNoHead = errors.New("ReadChainHead: ReadCurrentHeader error")

type ChainHead struct {
	HeadHeight uint64
	HeadTime   uint64
	HeadHash   libcommon.Hash
	HeadTd     *uint256.Int
}

type StatusDataProvider struct {
	db kv.RoDB

	networkId   uint64
	genesisHash libcommon.Hash
	heightForks []uint64
	timeForks   []uint64
}

func NewStatusDataProvider(
	db kv.RoDB,
	chainConfig *chain.Config,
	genesis *types.Block,
	networkId uint64,
) *StatusDataProvider {
	s := &StatusDataProvider{
		db:          db,
		networkId:   networkId,
		genesisHash: genesis.Hash(),
	}

	s.heightForks, s.timeForks = forkid.GatherForks(chainConfig, genesis.Time())

	return s
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
