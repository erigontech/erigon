package commands

import (
	"github.com/ledgerwatch/erigon/cmd/devnet/models"
	"github.com/ledgerwatch/erigon/cmd/devnet/requests"
	"github.com/ledgerwatch/log/v3"
)

func pingErigonRpc(logger log.Logger) error {
	err := requests.PingErigonRpc(models.ReqId, logger)
	if err != nil {
		logger.Error("FAILURE", "error", err)
	}
	return err
}
