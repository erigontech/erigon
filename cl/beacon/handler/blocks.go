package handler

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/persistence/beacon_indicies"
)

type headerResponse struct {
	Root      libcommon.Hash                   `json:"root"`
	Canonical bool                             `json:"canonical"`
	Header    *cltypes.SignedBeaconBlockHeader `json:"header"`
}

type getHeadersRequest struct {
	Slot       *uint64         `json:"slot,omitempty"`
	ParentRoot *libcommon.Hash `json:"root,omitempty"`
}

func (a *ApiHandler) rootFromBlockId(ctx context.Context, tx *sql.Tx, blockId *segmentID) (root libcommon.Hash, httpStatusErr int, err error) {
	switch {
	case blockId.head():
		root, _, err = a.forkchoiceStore.GetHead()
		if err != nil {
			return libcommon.Hash{}, http.StatusInternalServerError, err
		}
	case blockId.finalized():
		root = a.forkchoiceStore.FinalizedCheckpoint().BlockRoot()
	case blockId.genesis():
		root, err = beacon_indicies.ReadCanonicalBlockRoot(ctx, tx, 0)
		if err != nil {
			return libcommon.Hash{}, http.StatusInternalServerError, err
		}
		if root == (libcommon.Hash{}) {
			return libcommon.Hash{}, http.StatusNotFound, fmt.Errorf("genesis block not found")
		}
	case blockId.getSlot() != nil:
		root, err = beacon_indicies.ReadCanonicalBlockRoot(ctx, tx, *blockId.getSlot())
		if err != nil {
			return libcommon.Hash{}, http.StatusInternalServerError, err
		}
		if root == (libcommon.Hash{}) {
			return libcommon.Hash{}, http.StatusNotFound, fmt.Errorf("block not found %d", *blockId.getSlot())
		}
	case blockId.getRoot() != nil:
		// first check if it exists
		root = *blockId.getRoot()
	default:
		return libcommon.Hash{}, http.StatusInternalServerError, fmt.Errorf("cannot parse block id")
	}
	return
}

func (a *ApiHandler) getBlock(w http.ResponseWriter, r *http.Request) {

}

func (a *ApiHandler) getBlockRoot(r *http.Request) (data any, finalized *bool, version *clparams.StateVersion, httpStatus int, err error) {
	var (
		tx          *sql.Tx
		blockId     *segmentID
		root        libcommon.Hash
		blockSlot   uint64
		isCanonical bool
	)

	ctx := r.Context()

	tx, err = a.indiciesDB.Begin()
	if err != nil {
		httpStatus = http.StatusInternalServerError
		return
	}
	defer tx.Rollback()

	blockId, err = blockIdFromRequest(r)
	if err != nil {
		httpStatus = http.StatusBadRequest
		return
	}
	root, httpStatus, err = a.rootFromBlockId(ctx, tx, blockId)
	if err != nil {
		return
	}

	// check if the root exist
	var blk *cltypes.SignedBeaconBlockHeader
	blk, isCanonical, err = beacon_indicies.ReadSignedHeaderByBlockRoot(ctx, a.indiciesDB, root)
	if err != nil {
		httpStatus = http.StatusInternalServerError
		return
	}
	if blk == nil {
		httpStatus = http.StatusNotFound
		err = fmt.Errorf("could not find block: %x", root)
		return
	}
	blockSlot = blk.Header.Slot

	// Pack the response
	finalized = new(bool)
	*finalized = isCanonical && blockSlot <= a.forkchoiceStore.FinalizedSlot()
	data = struct{ Root libcommon.Hash }{Root: root}
	httpStatus = http.StatusAccepted
	return
}
