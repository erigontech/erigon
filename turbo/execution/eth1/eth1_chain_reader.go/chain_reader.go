package eth1_chain_reader

import (
	"context"
	"math/big"

	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/execution"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/turbo/execution/eth1/eth1_utils"
	"github.com/ledgerwatch/log/v3"
	"google.golang.org/protobuf/types/known/emptypb"
)

type ChainReaderEth1 struct {
	ctx             context.Context
	cfg             *chain.Config
	executionModule execution.ExecutionClient
}

func NewChainReaderEth1(ctx context.Context, cfg *chain.Config, executionModule execution.ExecutionClient) *ChainReaderEth1 {
	return &ChainReaderEth1{
		ctx:             ctx,
		cfg:             cfg,
		executionModule: executionModule,
	}
}

func (c ChainReaderEth1) Config() *chain.Config {
	return c.cfg
}

func (c ChainReaderEth1) CurrentHeader() *types.Header {
	resp, err := c.executionModule.CurrentHeader(c.ctx, &emptypb.Empty{})
	if err != nil {
		log.Error("GetHeader failed", "err", err)
		return nil
	}
	if resp == nil || resp.Header == nil {
		return nil
	}
	ret, err := eth1_utils.HeaderRpcToHeader(resp.Header)
	if err != nil {
		log.Error("GetHeader decoding", "err", err)
		return nil
	}
	return ret
}

func (ChainReaderEth1) FrozenBlocks() uint64 {
	panic("ChainReaderEth1.FrozenBlocks not implemented")
}

func (c ChainReaderEth1) GetHeader(hash libcommon.Hash, number uint64) *types.Header {
	resp, err := c.executionModule.GetHeader(c.ctx, &execution.GetSegmentRequest{
		BlockNumber: &number,
		BlockHash:   gointerfaces.ConvertHashToH256(hash),
	})
	if err != nil {
		log.Error("GetHeader failed", "err", err)
		return nil
	}
	if resp == nil || resp.Header == nil {
		return nil
	}
	ret, err := eth1_utils.HeaderRpcToHeader(resp.Header)
	if err != nil {
		log.Error("GetHeader decoding", "err", err)
		return nil
	}
	return ret
}

func (c ChainReaderEth1) GetHeaderByHash(hash libcommon.Hash) *types.Header {
	resp, err := c.executionModule.GetHeader(c.ctx, &execution.GetSegmentRequest{
		BlockNumber: nil,
		BlockHash:   gointerfaces.ConvertHashToH256(hash),
	})
	if err != nil {
		log.Error("GetHeaderByHash failed", "err", err)
		return nil
	}
	if resp == nil || resp.Header == nil {
		return nil
	}
	ret, err := eth1_utils.HeaderRpcToHeader(resp.Header)
	if err != nil {
		log.Error("GetHeaderByHash decoding", "err", err)
		return nil
	}
	return ret
}

func (c ChainReaderEth1) GetHeaderByNumber(number uint64) *types.Header {
	resp, err := c.executionModule.GetHeader(c.ctx, &execution.GetSegmentRequest{
		BlockNumber: &number,
		BlockHash:   nil,
	})
	if err != nil {
		log.Error("GetHeaderByHash failed", "err", err)
		return nil
	}
	if resp == nil || resp.Header == nil {
		return nil
	}
	ret, err := eth1_utils.HeaderRpcToHeader(resp.Header)
	if err != nil {
		log.Error("GetHeaderByHash decoding", "err", err)
		return nil
	}
	return ret
}

func (c ChainReaderEth1) GetTd(hash libcommon.Hash, number uint64) *big.Int {
	resp, err := c.executionModule.GetTD(c.ctx, &execution.GetSegmentRequest{
		BlockNumber: &number,
		BlockHash:   gointerfaces.ConvertHashToH256(hash),
	})
	if err != nil {
		log.Error("GetHeaderByHash failed", "err", err)
		return nil
	}
	if resp == nil || resp.Td == nil {
		return nil
	}
	return eth1_utils.ConvertBigIntFromRpc(resp.Td)
}

func (c ChainReaderEth1) HeaderNumber(hash libcommon.Hash) (*uint64, error) {
	resp, err := c.executionModule.GetHeaderHashNumber(c.ctx, gointerfaces.ConvertHashToH256(hash))
	if err != nil {
		return nil, err
	}
	if resp == nil {
		return nil, nil
	}
	return resp.BlockNumber, nil
}

func (c ChainReaderEth1) IsCanonicalHash(hash libcommon.Hash) (bool, error) {
	resp, err := c.executionModule.IsCanonicalHash(c.ctx, gointerfaces.ConvertHashToH256(hash))
	if err != nil {
		return false, err
	}
	if resp == nil {
		return false, nil
	}
	return resp.Canonical, nil
}
