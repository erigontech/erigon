package jsonrpc

import (
	"context"
	"encoding/binary"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/kv"
)

type HoldingMatch struct {
	Address common.Address `json:"address"`
	Tx      uint64         `json:"ethTx"`
}

func (api *Otterscan2APIImpl) GetERC20Holdings(ctx context.Context, holder common.Address) ([]*HoldingMatch, error) {
	return api.getHoldings(ctx, holder, kv.OtsERC20Holdings)
}

func (api *Otterscan2APIImpl) GetERC721Holdings(ctx context.Context, holder common.Address) ([]*HoldingMatch, error) {
	return api.getHoldings(ctx, holder, kv.OtsERC721Holdings)
}

func (api *Otterscan2APIImpl) getHoldings(ctx context.Context, holder common.Address, holdingsBucket string) ([]*HoldingMatch, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, nil
	}
	defer tx.Rollback()

	holdings, err := tx.CursorDupSort(holdingsBucket)
	if err != nil {
		return nil, err
	}
	defer holdings.Close()

	k, v, err := holdings.SeekExact(holder.Bytes())
	if err != nil {
		return nil, err
	}
	if k == nil {
		return make([]*HoldingMatch, 0), nil
	}

	ret := make([]*HoldingMatch, 0)
	for k != nil {
		token := common.BytesToAddress(v[:length.Addr])
		ethTx := binary.BigEndian.Uint64(v[length.Addr:])

		ret = append(ret, &HoldingMatch{
			Address: token,
			Tx:      ethTx,
		})

		k, v, err = holdings.NextDup()
		if err != nil {
			return nil, err
		}
	}

	return ret, nil
}
