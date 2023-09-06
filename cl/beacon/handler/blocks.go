package handler

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
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
		root = *blockId.getRoot()
		fmt.Println(root)
	default:
		return libcommon.Hash{}, http.StatusInternalServerError, fmt.Errorf("cannot parse block id")
	}
	return
}

func (a *ApiHandler) getBlock(w http.ResponseWriter, r *http.Request) {}

func (a *ApiHandler) getBlockRoot(w http.ResponseWriter, r *http.Request) {}
