package jsonrpc

import (
	"context"
	"encoding/binary"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/turbo/services"
)

type WithdrawalsListResult struct {
	BlocksSummary map[hexutil.Uint64]*BlockSummary `json:"blocksSummary"`
	Results       []*WithdrawalMatch               `json:"results"`
}

type WithdrawalMatch struct {
	Index     hexutil.Uint64 `json:"index"`
	BlockNum  hexutil.Uint64 `json:"blockNumber"`
	Validator hexutil.Uint64 `json:"validatorIndex"`
	Amount    hexutil.Uint64 `json:"amount"`
}

type withdrawalsSearchResultMaterializer struct {
	blockReader services.FullBlockReader
	idx2Block   kv.Cursor
}

func NewWithdrawalsSearchResultMaterializer(tx kv.Tx, blockReader services.FullBlockReader) (*withdrawalsSearchResultMaterializer, error) {
	idx2Block, err := tx.Cursor(kv.OtsWithdrawalIdx2Block)
	if err != nil {
		return nil, err
	}

	return &withdrawalsSearchResultMaterializer{blockReader, idx2Block}, nil
}

func (w *withdrawalsSearchResultMaterializer) Convert(ctx context.Context, tx kv.Tx, idx uint64) (*WithdrawalMatch, error) {
	idx2Block, err := tx.Cursor(kv.OtsWithdrawalIdx2Block)
	if err != nil {
		return nil, err
	}
	defer idx2Block.Close()

	k, v, err := idx2Block.Seek(hexutility.EncodeTs(idx))
	if err != nil {
		return nil, err
	}
	if k == nil {
		return nil, nil
	}

	blockNum := binary.BigEndian.Uint64(v)
	hash, err := w.blockReader.CanonicalHash(ctx, tx, blockNum)
	if err != nil {
		return nil, err
	}
	body, _, err := w.blockReader.Body(ctx, tx, hash, blockNum)
	if err != nil {
		return nil, err
	}

	var match *types.Withdrawal
	for _, w := range body.Withdrawals {
		if w.Index == idx {
			match = w
			break
		}
	}
	if match == nil {
		// TODO: error
		return nil, nil
	}

	result := &WithdrawalMatch{
		Index:     hexutil.Uint64(idx),
		BlockNum:  hexutil.Uint64(blockNum),
		Validator: hexutil.Uint64(match.Validator),
		Amount:    hexutil.Uint64(match.Amount),
	}
	return result, nil
}

func (w *withdrawalsSearchResultMaterializer) Dispose() {
	w.idx2Block.Close()
}

func (api *Otterscan2APIImpl) GetWithdrawalsList(ctx context.Context, addr common.Address, idx, count uint64) (*WithdrawalsListResult, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	srm, err := NewWithdrawalsSearchResultMaterializer(tx, api._blockReader)
	if err != nil {
		return nil, err
	}
	defer srm.Dispose()

	ret, err := genericResultList(ctx, tx, addr, idx, count, kv.OtsWithdrawalsIndex, kv.OtsWithdrawalsCounter, (SearchResultMaterializer[WithdrawalMatch])(srm))
	if err != nil {
		return nil, err
	}

	blocks := make([]hexutil.Uint64, 0, len(ret))
	for _, r := range ret {
		blocks = append(blocks, hexutil.Uint64(r.BlockNum))
	}

	blocksSummary, err := api.newBlocksSummaryFromResults(ctx, tx, blocks)
	if err != nil {
		return nil, err
	}
	return &WithdrawalsListResult{
		BlocksSummary: blocksSummary,
		Results:       ret,
	}, nil
}

func (api *Otterscan2APIImpl) GetWithdrawalsCount(ctx context.Context, addr common.Address) (uint64, error) {
	return api.genericGetCount(ctx, addr, kv.OtsWithdrawalsCounter)
}
