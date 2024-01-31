package handler

import (
	"fmt"
	"net/http"

	"github.com/ledgerwatch/erigon/cl/beacon/beaconhttp"
)

func (a *ApiHandler) EthV1BeaconLightClientBootstrap(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
	ctx := r.Context()
	tx, err := a.indiciesDB.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	blockId, err := beaconhttp.BlockIdFromRequest(r)
	if err != nil {
		return nil, err
	}
	root, err := a.rootFromBlockId(ctx, tx, blockId)
	if err != nil {
		return nil, err
	}
	bootstrap, ok := a.forkchoiceStore.GetLightClientBootstrap(root)
	if !ok {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Errorf("bootstrap object evicted"))
	}
	return newBeaconResponse(bootstrap).WithVersion(bootstrap.Header.Version()), nil
}
