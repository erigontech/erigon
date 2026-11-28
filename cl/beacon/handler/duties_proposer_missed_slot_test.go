// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package handler

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/persistence/base_encoding"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
)

func TestGetDutiesProposerHistoricalEpochFirstSlotMissed(t *testing.T) {
	db, blocks, _, _, postState, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)

	headRoot, err := blocks[len(blocks)-1].Block.HashSSZ()
	require.NoError(t, err)
	fcu.HeadVal = headRoot
	fcu.HeadSlotVal = postState.Slot()

	epoch := uint64(3)
	firstSlot := epoch * handler.beaconChainCfg.SlotsPerEpoch
	fcu.FinalizedCheckpointVal = solid.Checkpoint{Epoch: epoch, Root: headRoot}
	fcu.FinalizedSlotVal = (epoch+1)*handler.beaconChainCfg.SlotsPerEpoch - 1
	fcu.LowestAvailableSlotVal = &firstSlot

	before := getProposerDutiesForEpoch(t, handler, epoch)
	require.Len(t, before.Data, int(handler.beaconChainCfg.SlotsPerEpoch))

	key := base_encoding.Encode64ToBytes4(firstSlot)
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		row, err := tx.GetOne(kv.SlotData, key)
		require.NoError(t, err)
		require.NotNil(t, row)
		return tx.Delete(kv.SlotData, key)
	}))

	recorder := httptest.NewRecorder()
	path := "/eth/v1/validator/duties/proposer/" + strconv.FormatUint(epoch, 10)
	handler.mux.ServeHTTP(recorder, httptest.NewRequestWithContext(t.Context(), http.MethodGet, path, http.NoBody))
	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())

	var after proposerDutiesResponse
	require.NoError(t, json.NewDecoder(recorder.Body).Decode(&after))
	require.Equal(t, before.Data, after.Data)
}
