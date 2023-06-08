package commands

import (
	"github.com/ledgerwatch/erigon/cmd/devnet/node"
	"github.com/ledgerwatch/log/v3"
)

func pingErigonRpc(node *node.Node, logger log.Logger) error {
	err := node.PingErigonRpc().Err
	if err != nil {
		logger.Error("FAILURE", "error", err)
	}
	return err
}
