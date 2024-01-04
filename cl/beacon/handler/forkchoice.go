package handler

import (
	"net/http"

	"github.com/ledgerwatch/erigon/cl/beacon/beaconhttp"
)

func (a *ApiHandler) GetEthV2DebugBeaconHeads(w http.ResponseWriter, r *http.Request) (*beaconResponse, error) {
	if a.syncedData.Syncing() {
		return nil, beaconhttp.NewEndpointError(http.StatusServiceUnavailable, "beacon node is syncing")
	}
	hash, slotNumber, err := a.forkchoiceStore.GetHead()
	if err != nil {
		return nil, err
	}
	return newBeaconResponse(
		[]interface{}{
			map[string]interface{}{
				"slot":                 slotNumber,
				"root":                 hash,
				"execution_optimistic": false,
			},
		}), nil
}
