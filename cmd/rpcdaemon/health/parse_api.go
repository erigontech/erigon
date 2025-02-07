package health

import (
	"github.com/ledgerwatch/erigon/rpc"
)

func parseAPI(api []rpc.API) (netAPI NetAPI, ethAPI EthAPI, txPoolAPI TxPoolAPI) {
	for _, rpc := range api {
		if rpc.Service == nil {
			continue
		}

		if netCandidate, ok := rpc.Service.(NetAPI); ok {
			netAPI = netCandidate
		}

		if ethCandidate, ok := rpc.Service.(EthAPI); ok {
			ethAPI = ethCandidate
		}

		if txPoolCandidate, ok := rpc.Service.(TxPoolAPI); ok {
			txPoolAPI = txPoolCandidate
		}
	}
	return netAPI, ethAPI, txPoolAPI
}
