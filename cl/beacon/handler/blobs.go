package handler

import (
	"fmt"
	"net/http"

	"github.com/ledgerwatch/erigon/cl/beacon/beaconhttp"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/persistence/beacon_indicies"
)

var blobSidecarSSZLenght = (*cltypes.BlobSidecar)(nil).EncodingSizeSSZ()

func (a *ApiHandler) GetEthV1BeaconBlobSidecars(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
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
	blockRoot, err := a.rootFromBlockId(ctx, tx, blockId)
	if err != nil {
		return nil, err
	}
	slot, err := beacon_indicies.ReadBlockSlotByBlockRoot(tx, blockRoot)
	if err != nil {
		return nil, err
	}
	if slot == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Errorf("block not found"))
	}
	if a.caplinSnapshots != nil && *slot <= a.caplinSnapshots.FrozenBlobs() {
		out, err := a.caplinSnapshots.ReadBlobSidecars(*slot)
		if err != nil {
			return nil, err
		}
		resp := solid.NewStaticListSSZ[*cltypes.BlobSidecar](696969, blobSidecarSSZLenght)
		for _, v := range out {
			resp.Append(v)
		}
		return beaconhttp.NewBeaconResponse(resp), nil

	}
	out, found, err := a.blobStoage.ReadBlobSidecars(ctx, *slot, blockRoot)
	if err != nil {
		return nil, err
	}

	resp := solid.NewStaticListSSZ[*cltypes.BlobSidecar](696969, blobSidecarSSZLenght)
	if !found {
		return beaconhttp.NewBeaconResponse(resp), nil
	}
	for _, v := range out {
		resp.Append(v)
	}
	return beaconhttp.NewBeaconResponse(resp), nil
}
