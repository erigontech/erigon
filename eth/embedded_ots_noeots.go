//go:build noeots

package eth

import (
	"context"

	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/cli/httpcfg"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/log/v3"
)

func initEmbeddedOts(ctx context.Context, logger log.Logger, httpRpcCfg httpcfg.HttpCfg, apiList []rpc.API) error {
	logger.Warn("Embedded Otterscan not supported, remove \"-tags noeots\" from compilation flags")
	return nil
}
