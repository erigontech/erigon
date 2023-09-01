package handler

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/persistence/beacon_indicies"
)

func (a *ApiHandler) getHeaders(r *http.Request) (data any, finalized *bool, version *clparams.StateVersion, httpStatus int, err error) {
	ctx := r.Context()
	req := &getHeadersRequest{}

	finalized = new(bool)
	*finalized = true
	// Try to decode the request body into the struct. If there is an error,
	// respond to the client with the error message and a 400 status code.
	err = json.NewDecoder(r.Body).Decode(&req)
	if errors.Is(err, io.EOF) {
		req = nil
	} else if err != nil {
		err = fmt.Errorf("error while decoding json: %s", err)
		httpStatus = http.StatusBadRequest
		return
	}
	var tx *sql.Tx
	tx, err = a.indiciesDB.Begin()
	if err != nil {
		httpStatus = http.StatusInternalServerError
		return
	}
	defer tx.Rollback()

	var headersList []*cltypes.SignedBeaconBlockHeader
	var canonicals []bool

	// 4 Cases empty request, only slot, only parent_root, both.
	if req == nil || (req.ParentRoot == nil && req.Slot == nil) { // case 1 empty request
		var headSlot uint64
		_, headSlot, err = a.forkchoiceStore.GetHead()
		if err != nil {
			httpStatus = http.StatusInternalServerError
			return
		}
		headersList, canonicals, err = beacon_indicies.ReadSignedHeadersBySlot(ctx, tx, headSlot)
	} else if req.Slot != nil {
		headersList, canonicals, err = beacon_indicies.ReadSignedHeadersBySlot(ctx, tx, *req.Slot)
	} else if req.ParentRoot != nil {
		headersList, canonicals, err = beacon_indicies.ReadSignedHeadersByParentRoot(ctx, tx, *req.ParentRoot)
	} else {
		headersList, canonicals, err = beacon_indicies.ReadSignedHeadersByParentRootAndSlot(ctx, tx, *req.ParentRoot, *req.Slot)
	}
	if err != nil {
		httpStatus = http.StatusInternalServerError
		return
	}
	if len(headersList) == 0 {
		*finalized = false
	}
	resp := []*headerResponse{}
	for idx, header := range headersList {
		var headerRoot libcommon.Hash
		headerRoot, err = header.HashSSZ()
		if err != nil {
			httpStatus = http.StatusInternalServerError
			return
		}
		if !canonicals[idx] || header.Header.Slot > a.forkchoiceStore.FinalizedSlot() {
			*finalized = false
		}
		resp = append(resp, &headerResponse{
			Root:      headerRoot,
			Canonical: canonicals[idx],
			Header:    header,
		})
	}
	httpStatus = http.StatusAccepted
	data = resp
	return
}

func (a *ApiHandler) getHeader(r *http.Request) (data any, finalized *bool, version *clparams.StateVersion, httpStatus int, err error) {
	ctx := r.Context()
	tx, err2 := a.indiciesDB.Begin()
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

	signedHeader, isCanonical, err2 := beacon_indicies.ReadSignedHeaderByBlockRoot(ctx, tx, root)
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
	data = &headerResponse{
		Root:      root,
		Canonical: isCanonical,
		Header:    signedHeader,
	}
	finalized = new(bool)
	*finalized = isCanonical && signedHeader.Header.Slot <= a.forkchoiceStore.FinalizedSlot()
	httpStatus = http.StatusAccepted
	return
}
