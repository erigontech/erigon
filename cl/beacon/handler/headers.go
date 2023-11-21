package handler

import (
	"fmt"
	"net/http"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/persistence/beacon_indicies"
)

func (a *ApiHandler) getHeaders(r *http.Request) (data any, finalized *bool, version *clparams.StateVersion, httpStatus int, err error) {
	ctx := r.Context()

	finalized = new(bool)
	*finalized = true

	querySlot, err := uint64FromQueryParams(r, "slot")
	if err != nil {
		httpStatus = http.StatusBadRequest
		return
	}
	queryParentHash, err := hashFromQueryParams(r, "parent_root")
	if err != nil {
		httpStatus = http.StatusBadRequest
		return
	}

	tx, err := a.indiciesDB.BeginRo(ctx)
	if err != nil {
		httpStatus = http.StatusInternalServerError
		return
	}
	defer tx.Rollback()
	var candidates []libcommon.Hash
	var slot *uint64
	var potentialRoot libcommon.Hash
	// First lets find some good candidates for the query.
	switch {
	case queryParentHash != nil && querySlot != nil:
		// get all blocks with this parent
		slot, err = beacon_indicies.ReadBlockSlotByBlockRoot(tx, *queryParentHash)
		if err != nil {
			httpStatus = http.StatusInternalServerError
			return
		}
		if slot == nil {
			httpStatus = http.StatusNotFound
			err = fmt.Errorf("could not find block: %x", queryParentHash)
			return
		}
		if *slot+1 != *querySlot {
			break
		}
		potentialRoot, err = beacon_indicies.ReadCanonicalBlockRoot(tx, *slot+1)
		if err != nil {
			httpStatus = http.StatusInternalServerError
			return
		}
		candidates = append(candidates, potentialRoot)
	case queryParentHash == nil && querySlot != nil:
		potentialRoot, err = beacon_indicies.ReadCanonicalBlockRoot(tx, *querySlot)
		if err != nil {
			httpStatus = http.StatusInternalServerError
			return
		}
		candidates = append(candidates, potentialRoot)
	case queryParentHash == nil && querySlot == nil:
		headSlot := a.syncedData.HeadSlot()
		if headSlot == 0 {
			break
		}
		potentialRoot, err = beacon_indicies.ReadCanonicalBlockRoot(tx, headSlot)
		if err != nil {
			httpStatus = http.StatusInternalServerError
			return
		}
		candidates = append(candidates, potentialRoot)
	}
	// Now we assemble the response
	headers := make([]*headerResponse, 0, len(candidates))
	for _, root := range candidates {
		signedHeader, err2 := a.blockReader.ReadHeaderByRoot(ctx, tx, root)
		if err2 != nil {
			httpStatus = http.StatusInternalServerError
			err = err2
			return
		}
		if signedHeader == nil || (queryParentHash != nil && signedHeader.Header.ParentRoot != *queryParentHash) || (querySlot != nil && signedHeader.Header.Slot != *querySlot) {
			continue
		}

		var canonicalRoot libcommon.Hash
		canonicalRoot, err = beacon_indicies.ReadCanonicalBlockRoot(tx, signedHeader.Header.Slot)
		if err != nil {
			httpStatus = http.StatusInternalServerError
			return
		}
		headers = append(headers, &headerResponse{
			Root:      root,
			Canonical: canonicalRoot == root,
			Header:    signedHeader,
		})
	}
	data = headers
	httpStatus = http.StatusAccepted
	return
}

func (a *ApiHandler) getHeader(r *http.Request) (data any, finalized *bool, version *clparams.StateVersion, httpStatus int, err error) {
	ctx := r.Context()
	tx, err2 := a.indiciesDB.BeginRo(ctx)
	if err2 != nil {
		httpStatus = http.StatusInternalServerError
		err = err2
		return
	}
	defer tx.Rollback()
	blockId, err2 := blockIdFromRequest(r)
	if err2 != nil {
		httpStatus = http.StatusBadRequest
		err = err2
		return
	}
	root, httpStatus2, err2 := a.rootFromBlockId(ctx, tx, blockId)
	if err2 != nil {
		httpStatus = httpStatus2
		err = err2
		return
	}

	signedHeader, err2 := a.blockReader.ReadHeaderByRoot(ctx, tx, root)
	if err2 != nil {
		httpStatus = http.StatusInternalServerError
		err = err2
		return
	}
	if signedHeader == nil {
		httpStatus = http.StatusNotFound
		err = fmt.Errorf("could not read block header: %x", root)
		return
	}
	var canonicalRoot libcommon.Hash
	canonicalRoot, err = beacon_indicies.ReadCanonicalBlockRoot(tx, signedHeader.Header.Slot)
	if err != nil {
		httpStatus = http.StatusInternalServerError
		return
	}
	data = &headerResponse{
		Root:      root,
		Canonical: canonicalRoot == root,
		Header:    signedHeader,
	}
	finalized = new(bool)
	*finalized = canonicalRoot == root && signedHeader.Header.Slot <= a.forkchoiceStore.FinalizedSlot()
	httpStatus = http.StatusAccepted
	return
}
