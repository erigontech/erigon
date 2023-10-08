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
)

type ContractMatch struct {
	Block   *hexutil.Uint64 `json:"blockNumber"`
	Address *common.Address `json:"address"`
}

func (api *Otterscan2APIImpl) GetAllContractsList(ctx context.Context, idx, count uint64) (*ContractListResult, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	res, err := api.genericMatchingList(ctx, tx, kv.OtsAllContracts, kv.OtsAllContractsCounter, idx, count)
	if err != nil {
		return nil, err
	}

	extraData, err := api.newContractExtraData(ctx)
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

func (api *Otterscan2APIImpl) newContractExtraData(ctx context.Context) (ExtraDataExtractor, error) {
	return func(tx kv.Tx, res *AddrMatch, addr common.Address, evm *vm.EVM, header *types.Header, chainConfig *chain.Config, ibs *state.IntraBlockState, stateReader state.StateReader) (interface{}, error) {
		return &ContractMatch{
			res.Block,
			res.Address,
		}, nil
	}, nil
}

func (api *Otterscan2APIImpl) GetAllContractsCount(ctx context.Context) (uint64, error) {
	return api.genericMatchingCounter(ctx, kv.OtsAllContractsCounter)
}
