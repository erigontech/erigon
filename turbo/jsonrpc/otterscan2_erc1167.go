package jsonrpc

import (
	"context"

	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/turbo/rpchelper"
)

type ERC1167Match struct {
	Block          *hexutil.Uint64 `json:"blockNumber"`
	Address        *common.Address `json:"address"`
	Implementation *common.Address `json:"implementation"`
}

func (api *Otterscan2APIImpl) GetERC1167List(ctx context.Context, idx, count uint64) (*ContractListResult, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	res, err := api.genericMatchingList(ctx, tx, kv.OtsERC1167, kv.OtsERC1167Counter, idx, count)
	if err != nil {
		return nil, err
	}

	extraData, err := api.newERC1167ExtraData(ctx)
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

func (api *Otterscan2APIImpl) newERC1167ExtraData(ctx context.Context) (ExtraDataExtractor, error) {
	return func(tx kv.Tx, res *AddrMatch, addr common.Address, evm *vm.EVM, header *types.Header, chainConfig *chain.Config, ibs *state.IntraBlockState, stateReader state.StateReader) (interface{}, error) {
		acc, err := stateReader.ReadAccountData(addr)
		if err != nil {
			return nil, err
		}
		code, err := stateReader.ReadAccountCode(addr, acc.Incarnation, acc.CodeHash)
		if err != nil {
			return nil, err
		}

		impl := common.BytesToAddress(code[10:30])

		return &ERC1167Match{
			res.Block,
			res.Address,
			&impl,
		}, nil
	}, nil
}

func (api *Otterscan2APIImpl) GetERC1167Count(ctx context.Context) (uint64, error) {
	return api.genericMatchingCounter(ctx, kv.OtsERC1167Counter)
}

func (api *Otterscan2APIImpl) GetERC1167Impl(ctx context.Context, addr common.Address) (common.Address, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return common.Address{}, err
	}
	defer tx.Rollback()

	chainConfig, err := api.chainConfig(tx)
	if err != nil {
		return common.Address{}, err
	}
	reader, err := rpchelper.CreateStateReader(ctx, tx, rpc.BlockNumberOrHashWithNumber(rpc.LatestBlockNumber), 0, api.filters, api.stateCache, api.historyV3(tx), chainConfig.ChainName)
	if err != nil {
		return common.Address{}, err
	}
	acc, err := reader.ReadAccountData(addr)
	if err != nil {
		return common.Address{}, err
	}
	code, err := reader.ReadAccountCode(addr, acc.Incarnation, acc.CodeHash)
	if err != nil {
		return common.Address{}, err
	}

	return common.BytesToAddress(code[10:30]), nil
}
