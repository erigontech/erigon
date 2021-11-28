package commands

import (
	"context"

	"github.com/ledgerwatch/erigon/p2p"
)

func (api *ErigonImpl) NodeInfo(ctx context.Context) ([]p2p.NodeInfo, error) {
	return api.ethBackend.NodeInfo(ctx, 0)
}
