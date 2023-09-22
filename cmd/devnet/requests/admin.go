package requests

import (
	"github.com/ledgerwatch/erigon/p2p"
)

func (reqGen *requestGenerator) AdminNodeInfo() (p2p.NodeInfo, error) {
	var result p2p.NodeInfo

	if err := reqGen.callCli(&result, Methods.AdminNodeInfo); err != nil {
		return p2p.NodeInfo{}, err
	}

	return result, nil
}
