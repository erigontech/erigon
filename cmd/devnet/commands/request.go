package commands

import (
	"github.com/ledgerwatch/erigon/cmd/devnet/requests"
	"github.com/ledgerwatch/log/v3"
)

func pingErigonRpc(reqGen *requests.RequestGenerator, logger log.Logger) error {
	err := requests.PingErigonRpc(reqGen, logger)
	if err != nil {
		logger.Error("FAILURE", "error", err)
	}
	return err
}
