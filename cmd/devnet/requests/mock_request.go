package requests

import (
	"fmt"

	"github.com/ledgerwatch/log/v3"
)

func PingErigonRpc(reqId int, logger log.Logger) error {
	reqGen := initialiseRequestGenerator(reqId, logger)
	res := reqGen.PingErigonRpc()
	if res.Err != nil {
		return fmt.Errorf("failed to ping erigon rpc url: %v", res.Err)
	}
	logger.Info("SUCCESS => OK")
	return nil
}
