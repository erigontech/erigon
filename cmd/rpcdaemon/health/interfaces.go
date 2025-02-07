package health

import (
	"context"

	"github.com/ledgerwatch/erigon-lib/common/hexutil"

	"github.com/ledgerwatch/erigon/rpc"
)

type NetAPI interface {
	PeerCount(ctx context.Context) (hexutil.Uint, error)
}

type EthAPI interface {
	GetBlockByNumber(ctx context.Context, number rpc.BlockNumber, fullTx *bool) (map[string]interface{}, error)
	Syncing(ctx context.Context) (interface{}, error)
}

type TxPoolAPI interface {
	Content(ctx context.Context) (interface{}, error)
}
