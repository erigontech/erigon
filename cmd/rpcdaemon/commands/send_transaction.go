package commands

import (
	"context"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/hexutil"
)

func (api *APIImpl) SendRawTransaction(_ context.Context, encodedTx hexutil.Bytes) (common.Hash, error) {
	res, err := api.txpool.AddLocal(encodedTx)
	return common.BytesToHash(res), err
}
