package jsonrpc

import (
	"context"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
)

func (api *Otterscan2APIImpl) GetERC20TransferList(ctx context.Context, addr common.Address, idx, count uint64) (*TransactionListResult, error) {
	return api.genericTransferList(ctx, addr, idx, count, kv.OtsERC20TransferIndex, kv.OtsERC20TransferCounter)
}

func (api *Otterscan2APIImpl) GetERC20TransferCount(ctx context.Context, addr common.Address) (uint64, error) {
	return api.genericGetCount(ctx, addr, kv.OtsERC20TransferCounter)
}

func (api *Otterscan2APIImpl) GetERC721TransferList(ctx context.Context, addr common.Address, idx, count uint64) (*TransactionListResult, error) {
	return api.genericTransferList(ctx, addr, idx, count, kv.OtsERC721TransferIndex, kv.OtsERC721TransferCounter)
}

func (api *Otterscan2APIImpl) GetERC721TransferCount(ctx context.Context, addr common.Address) (uint64, error) {
	return api.genericGetCount(ctx, addr, kv.OtsERC721TransferCounter)
}
