package jsonrpc

import (
	"bytes"
	"context"

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

type ERC20Match struct {
	Block    *hexutil.Uint64 `json:"blockNumber"`
	Address  *common.Address `json:"address"`
	Name     string          `json:"name"`
	Symbol   string          `json:"symbol"`
	Decimals uint8           `json:"decimals"`
}

func (api *Otterscan2APIImpl) GetERC20List(ctx context.Context, idx, count uint64) (*ContractListResult, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	res, err := api.genericMatchingList(ctx, tx, kv.OtsERC20, kv.OtsERC20Counter, idx, count)
	if err != nil {
		return nil, err
	}

	extraData, err := api.newERC20ExtraData(ctx)
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

func (api *Otterscan2APIImpl) newERC20ExtraData(ctx context.Context) (ExtraDataExtractor, error) {
	erc20ABI, err := abi.JSON(bytes.NewReader(otscontracts.ERC20))
	if err != nil {
		return nil, err
	}

	name, err := erc20ABI.Pack("name")
	if err != nil {
		return nil, err
	}
	symbol, err := erc20ABI.Pack("symbol")
	if err != nil {
		return nil, err
	}
	decimals, err := erc20ABI.Pack("decimals")
	if err != nil {
		return nil, err
	}

	return func(tx kv.Tx, res *AddrMatch, addr common.Address, evm *vm.EVM, header *types.Header, chainConfig *chain.Config, ibs *state.IntraBlockState, stateReader state.StateReader) (interface{}, error) {
		// name()
		retName, err := decodeReturnData(ctx, &addr, name, "name", header, evm, chainConfig, ibs, &erc20ABI)
		if err != nil {
			return nil, err
		}
		strName := "<ERROR>"
		if retName != nil {
			strName = retName.(string)
		}

		// symbol()
		retSymbol, err := decodeReturnData(ctx, &addr, symbol, "symbol", header, evm, chainConfig, ibs, &erc20ABI)
		if err != nil {
			return nil, err
		}
		strSymbol := "<ERROR>"
		if retSymbol != nil {
			strSymbol = retSymbol.(string)
		}

		// decimals()
		retDecimals, err := decodeReturnData(ctx, &addr, decimals, "decimals", header, evm, chainConfig, ibs, &erc20ABI)
		if err != nil {
			return nil, err
		}
		nDecimals := uint8(0)
		if retDecimals != nil {
			nDecimals = retDecimals.(uint8)
		}

		return &ERC20Match{
			res.Block,
			res.Address,
			strName,
			strSymbol,
			nDecimals,
		}, nil
	}, nil
}

func (api *Otterscan2APIImpl) GetERC20Count(ctx context.Context) (uint64, error) {
	return api.genericMatchingCounter(ctx, kv.OtsERC20Counter)
}
