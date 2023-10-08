package jsonrpc

import (
	"bytes"
	"context"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/bitmapdb"
)

type AddrAttributes struct {
	ERC20   bool `json:"erc20,omitempty"`
	ERC165  bool `json:"erc165,omitempty"`
	ERC721  bool `json:"erc721,omitempty"`
	ERC1155 bool `json:"erc1155,omitempty"`
	ERC1167 bool `json:"erc1167,omitempty"`
}

func (api *Otterscan2APIImpl) GetAddressAttributes(ctx context.Context, addr common.Address) (*AddrAttributes, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	v, err := tx.GetOne(kv.OtsAddrAttributes, addr.Bytes())
	if err != nil {
		return nil, err
	}
	if v == nil {
		return &AddrAttributes{}, nil
	}

	bm := bitmapdb.NewBitmap64()
	defer bitmapdb.ReturnToPool64(bm)
	if _, err := bm.ReadFrom(bytes.NewReader(v)); err != nil {
		return nil, err
	}

	attr := AddrAttributes{
		ERC20:   bm.Contains(kv.ADDR_ATTR_ERC20),
		ERC165:  bm.Contains(kv.ADDR_ATTR_ERC165),
		ERC721:  bm.Contains(kv.ADDR_ATTR_ERC721),
		ERC1155: bm.Contains(kv.ADDR_ATTR_ERC1155),
		ERC1167: bm.Contains(kv.ADDR_ATTR_ERC1167),
	}
	return &attr, nil
}
