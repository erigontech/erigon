package requests

import (
	"fmt"

	"github.com/ledgerwatch/erigon/cmd/devnet/models"
	"github.com/ledgerwatch/erigon/p2p"
)

func AdminNodeInfo(reqId int) (p2p.NodeInfo, error) {
	reqGen := initialiseRequestGenerator(reqId)
	var b models.AdminNodeInfoResponse

	if res := reqGen.Erigon(models.AdminNodeInfo, reqGen.GetAdminNodeInfo(), &b); res.Err != nil {
		return p2p.NodeInfo{}, fmt.Errorf("failed to get admin node info: %v", res.Err)
	}

	return b.Result, nil
}
