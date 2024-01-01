package handler

import (
	"fmt"
	"net/http"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/beacon/beaconhttp"
	"github.com/ledgerwatch/erigon/cl/persistence/beacon_indicies"
)

func (a *ApiHandler) getHeaders(w http.ResponseWriter, r *http.Request) (*beaconResponse, error) {
	ctx := r.Context()

	querySlot, err := uint64FromQueryParams(r, "slot")
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err.Error())
	}
	queryParentHash, err := hashFromQueryParams(r, "parent_root")
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err.Error())
	}

	tx, err := a.indiciesDB.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	var candidates []libcommon.Hash
	var slot *uint64
	var potentialRoot libcommon.Hash
	// First lets find some good candidates for the query. TODO(Giulio2002): this does not give all the headers.
	switch {
	case queryParentHash != nil && querySlot != nil:
		// get all blocks with this parent
		slot, err = beacon_indicies.ReadBlockSlotByBlockRoot(tx, *queryParentHash)
		if err != nil {
			return nil, err
		}
		if slot == nil {
			break
		}
		if *slot+1 != *querySlot {
			break
		}
		potentialRoot, err = beacon_indicies.ReadCanonicalBlockRoot(tx, *slot+1)
		if err != nil {
			return nil, err
		}
		candidates = append(candidates, potentialRoot)
	case queryParentHash == nil && querySlot != nil:
		potentialRoot, err = beacon_indicies.ReadCanonicalBlockRoot(tx, *querySlot)
		if err != nil {
			return nil, err
		}
		candidates = append(candidates, potentialRoot)
	case queryParentHash == nil && querySlot == nil:
		headSlot := a.syncedData.HeadSlot()
		if headSlot == 0 {
			break
		}
		potentialRoot, err = beacon_indicies.ReadCanonicalBlockRoot(tx, headSlot)
		if err != nil {
			return nil, err
		}
		candidates = append(candidates, potentialRoot)
	}
	// Now we assemble the response
	headers := make([]*headerResponse, 0, len(candidates))
	for _, root := range candidates {
		signedHeader, err := a.blockReader.ReadHeaderByRoot(ctx, tx, root)
		if err != nil {
			return nil, err
		}
		if signedHeader == nil || (queryParentHash != nil && signedHeader.Header.ParentRoot != *queryParentHash) || (querySlot != nil && signedHeader.Header.Slot != *querySlot) {
			continue
		}

		canonicalRoot, err := beacon_indicies.ReadCanonicalBlockRoot(tx, signedHeader.Header.Slot)
		if err != nil {
			return nil, err
		}
		headers = append(headers, &headerResponse{
			Root:      root,
			Canonical: canonicalRoot == root,
			Header:    signedHeader,
		})
	}
	return newBeaconResponse(headers), nil
}

func (a *ApiHandler) getHeader(w http.ResponseWriter, r *http.Request) (*beaconResponse, error) {
	ctx := r.Context()
	tx, err := a.indiciesDB.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	blockId, err := blockIdFromRequest(r)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err.Error())
	}
	root, err := a.rootFromBlockId(ctx, tx, blockId)
	if err != nil {
		return nil, err
	}

	signedHeader, err := a.blockReader.ReadHeaderByRoot(ctx, tx, root)
	if err != nil {
		return nil, err
	}

	if signedHeader == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Sprintf("block not found %x", root))
	}
	var canonicalRoot libcommon.Hash
	canonicalRoot, err = beacon_indicies.ReadCanonicalBlockRoot(tx, signedHeader.Header.Slot)
	if err != nil {
		return nil, err
	}

	version := a.beaconChainCfg.GetCurrentStateVersion(signedHeader.Header.Slot / a.beaconChainCfg.SlotsPerEpoch)

	return newBeaconResponse(&headerResponse{
		Root:      root,
		Canonical: canonicalRoot == root,
		Header:    signedHeader,
	}).withFinalized(canonicalRoot == root && signedHeader.Header.Slot <= a.forkchoiceStore.FinalizedSlot()).withVersion(version), nil
}
