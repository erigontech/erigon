package requests

import (
	"fmt"

	"github.com/ledgerwatch/erigon/p2p"
)

// AdminNodeInfoResponse is the response for calls made to admin_nodeInfo
type AdminNodeInfoResponse struct {
	CommonResponse
	Result p2p.NodeInfo `json:"result"`
}

func (reqGen *requestGenerator) AdminNodeInfo() (p2p.NodeInfo, error) {
	var b AdminNodeInfoResponse

	method, body := reqGen.adminNodeInfo()
	if res := reqGen.call(method, body, &b); res.Err != nil {
		return p2p.NodeInfo{}, fmt.Errorf("failed to get admin node info: %w", res.Err)
	}

	return b.Result, nil
}

func (req *requestGenerator) adminNodeInfo() (RPCMethod, string) {
	const template = `{"jsonrpc":"2.0","method":%q,"id":%d}`
	return Methods.AdminNodeInfo, fmt.Sprintf(template, Methods.AdminNodeInfo, req.reqID)
}
