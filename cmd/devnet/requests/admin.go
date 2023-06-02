package requests

import (
	"fmt"

	"github.com/ledgerwatch/erigon/cmd/devnet/models"
	"github.com/ledgerwatch/erigon/p2p"
	"github.com/ledgerwatch/log/v3"
)

func AdminNodeInfo(reqGen *RequestGenerator, logger log.Logger) (p2p.NodeInfo, error) {
	var b models.AdminNodeInfoResponse

	if res := reqGen.Erigon(models.AdminNodeInfo, reqGen.GetAdminNodeInfo(), &b); res.Err != nil {
		return p2p.NodeInfo{}, fmt.Errorf("failed to get admin node info: %v", res.Err)
	}

	return b.Result, nil
}
