package jsonrpc

import (
	"bytes"
	"context"
	"math/big"

	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/accounts/abi"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/eth/stagedsync/otscontracts"
)

type ERC4626Match struct {
	Block       *hexutil.Uint64 `json:"blockNumber"`
	Address     *common.Address `json:"address"`
	Name        string          `json:"name"`
	Symbol      string          `json:"symbol"`
	Decimals    uint8           `json:"decimals"`
	Asset       common.Address  `json:"asset"`
	TotalAssets *big.Int        `json:"totalAssets"`
}

func (api *Otterscan2APIImpl) GetERC4626List(ctx context.Context, idx, count uint64) (*ContractListResult, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	res, err := api.genericMatchingList(ctx, tx, kv.OtsERC4626, kv.OtsERC4626Counter, idx, count)
	if err != nil {
		return nil, err
	}

	extraData, err := api.newERC4626ExtraData(ctx)
	if err != nil {
		return nil, err
	}

	results, err := api.genericExtraData(ctx, tx, res, extraData)
	if err != nil {
		return nil, err
	}
	blocksSummary, err := api.newBlocksSummaryFromResults(ctx, tx, ToBlockSlice(res))
	if err != nil {
		return nil, err
	}
	return &ContractListResult{
		BlocksSummary: blocksSummary,
		Results:       results,
	}, nil
}

func (api *Otterscan2APIImpl) newERC4626ExtraData(ctx context.Context) (ExtraDataExtractor, error) {
	erc4626ABI, err := abi.JSON(bytes.NewReader(otscontracts.IERC4626))
	if err != nil {
		return nil, err
	}

	asset, err := erc4626ABI.Pack("asset")
	if err != nil {
		return nil, err
	}
	totalAssets, err := erc4626ABI.Pack("totalAssets")
	if err != nil {
		return nil, err
	}

	return func(tx kv.Tx, res *AddrMatch, addr common.Address, evm *vm.EVM, header *types.Header, chainConfig *chain.Config, ibs *state.IntraBlockState, stateReader state.StateReader) (interface{}, error) {
		erc20Extra, err := api.newERC20ExtraData(ctx)
		if err != nil {
			return nil, err
		}
		erc20Match, err := erc20Extra(tx, res, addr, evm, header, chainConfig, ibs, stateReader)
		if err != nil {
			return nil, err
		}

		// asset()
		retAsset, err := decodeReturnData(ctx, &addr, asset, "asset", header, evm, chainConfig, ibs, &erc4626ABI)
		if err != nil {
			return nil, err
		}
		addrAsset := common.Address{}
		if retAsset != nil {
			addrAsset = retAsset.(common.Address)
		}

		// totalAssets()
		retTotalAssets, err := decodeReturnData(ctx, &addr, totalAssets, "totalAssets", header, evm, chainConfig, ibs, &erc4626ABI)
		if err != nil {
			return nil, err
		}
		var nTotalAssets *big.Int
		if retTotalAssets != nil {
			nTotalAssets = retTotalAssets.(*big.Int)
		}

		return &ERC4626Match{
			Block:       erc20Match.(*ERC20Match).Block,
			Address:     erc20Match.(*ERC20Match).Address,
			Name:        erc20Match.(*ERC20Match).Name,
			Symbol:      erc20Match.(*ERC20Match).Symbol,
			Decimals:    erc20Match.(*ERC20Match).Decimals,
			Asset:       addrAsset,
			TotalAssets: nTotalAssets,
		}, nil
	}, nil
}

func (api *Otterscan2APIImpl) GetERC4626Count(ctx context.Context) (uint64, error) {
	return api.genericMatchingCounter(ctx, kv.OtsERC4626Counter)
}
