package handler

import (
	"database/sql"
	"fmt"
	"net/http"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/persistence/beacon_indicies"
	"github.com/ledgerwatch/erigon/cl/utils"
)

func previousVersion(v clparams.StateVersion) clparams.StateVersion {
	if v == clparams.Phase0Version {
		return clparams.Phase0Version
	}
	return v - 1
}

func (a *ApiHandler) getStatesFork(r *http.Request) (data any, finalized *bool, version *clparams.StateVersion, httpStatus int, err error) {
	var (
		tx        *sql.Tx
		blockId   *segmentID
		root      libcommon.Hash
		blkHeader *cltypes.SignedBeaconBlockHeader
		canonical bool
	)

	ctx := r.Context()

	tx, err = a.indiciesDB.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
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

	blkHeader, canonical, err = beacon_indicies.ReadSignedHeaderByBlockRoot(ctx, tx, root)
	if err != nil {
		httpStatus = http.StatusInternalServerError
		return
	}
	if blkHeader == nil {
		err = fmt.Errorf("could not read block header: %x", root)
		httpStatus = http.StatusNotFound
		return
	}
	slot := blkHeader.Header.Slot

	epoch := slot / a.beaconChainCfg.SlotsPerEpoch
	stateVersion := a.beaconChainCfg.GetCurrentStateVersion(epoch)
	currentVersion := a.beaconChainCfg.GetForkVersionByVersion(stateVersion)
	previousVersion := a.beaconChainCfg.GetForkVersionByVersion(previousVersion(stateVersion))

	data = &cltypes.Fork{
		PreviousVersion: utils.Uint32ToBytes4(previousVersion),
		CurrentVersion:  utils.Uint32ToBytes4(currentVersion),
		Epoch:           epoch,
	}
	finalized = new(bool)
	*finalized = canonical && slot <= a.forkchoiceStore.FinalizedSlot()
	httpStatus = http.StatusAccepted
	return
}
