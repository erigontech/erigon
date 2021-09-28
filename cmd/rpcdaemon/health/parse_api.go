package health

import "github.com/ledgerwatch/erigon/rpc"

func parseAPI(api []rpc.API) (netAPI NetAPI, ethAPI EthAPI) {
	for _, rpc := range api {
		if netCandidate, ok := rpc.(NetAPI); ok {
			netAPI = netCandidate
		}

		if ethCandidate, ok := rpc.(EthAPI); ok {
			ethAPI = ethCandidate
		}
	}
	return netAPI, ethAPI
}
