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
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
)

func TestGetCommitteesSideBranchStateUsesBranchRandao(t *testing.T) {
	db, _, _, _, head, h, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	cfg := h.beaconChainCfg
	epoch := head.Slot() / cfg.SlotsPerEpoch

	side, err := head.Copy()
	require.NoError(t, err)
	mixIdx := int((epoch + cfg.EpochsPerHistoricalVector - cfg.MinSeedLookahead - 1) % cfg.EpochsPerHistoricalVector)
	require.NoError(t, side.SetRandaoMixAt(mixIdx, common.Hash{0xde, 0xad, 0xbe, 0xef}))

	sideHeader := &cltypes.SignedBeaconBlockHeader{Header: &cltypes.BeaconBlockHeader{
		Slot:       head.Slot(),
		ParentRoot: common.Hash{0x01},
		Root:       common.Hash{0x02},
		BodyRoot:   common.Hash{0x03},
	}}
	sideRoot, err := sideHeader.Header.HashSSZ()
	require.NoError(t, err)
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		return beacon_indicies.WriteBeaconBlockHeaderAndIndicies(tx, sideHeader, false)
	}))
	require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
		canonical, err := beacon_indicies.ReadCanonicalBlockRoot(tx, head.Slot())
		require.NoError(t, err)
		require.NotEqual(t, sideRoot, canonical)
		return nil
	}))
	fcu.StateAtBlockRootVal = map[common.Hash]*state.CachingBeaconState{sideRoot: side}
	lowestAvailable := uint64(0)
	fcu.LowestAvailableSlotVal = &lowestAvailable
	fcu.FinalizedSlotVal = head.Slot()

	want := committeesOf(t, side, epoch, cfg.SlotsPerEpoch)
	headCommittees := committeesOf(t, head, epoch, cfg.SlotsPerEpoch)
	require.NotEqual(t, headCommittees, want)

	path := fmt.Sprintf("/eth/v1/beacon/states/%s/committees?epoch=%d", sideHeader.Header.Root.Hex(), epoch)
	rec := httptest.NewRecorder()
	h.mux.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, path, nil))
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	var body struct {
		Data      []committeeResponse `json:"data"`
		Finalized bool                `json:"finalized"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	require.Equal(t, want, body.Data, "committees must come from the requested side-branch state")
	require.False(t, body.Finalized)

	delete(fcu.StateAtBlockRootVal, sideRoot)
	rec = httptest.NewRecorder()
	h.mux.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, path, nil))
	require.Equal(t, http.StatusNotFound, rec.Code, rec.Body.String())

	require.NoError(t, side.SetSlot(head.Slot()-1))
	sideHeader.Header.Slot = side.Slot()
	sideHeader.Header.Root = common.Hash{0x04}
	canonicalRoot, err := sideHeader.Header.HashSSZ()
	require.NoError(t, err)
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		return beacon_indicies.WriteBeaconBlockHeaderAndIndicies(tx, sideHeader, true)
	}))
	fcu.StateAtBlockRootVal[canonicalRoot] = side
	path = fmt.Sprintf("/eth/v1/beacon/states/%s/committees?epoch=%d", sideHeader.Header.Root.Hex(), epoch)
	rec = httptest.NewRecorder()
	h.mux.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, path, nil))
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	require.Equal(t, committeesOf(t, side, epoch, cfg.SlotsPerEpoch), body.Data)
	require.True(t, body.Finalized)
}

func committeesOf(t *testing.T, s *state.CachingBeaconState, epoch, slotsPerEpoch uint64) []committeeResponse {
	t.Helper()
	var out []committeeResponse
	for slot := epoch * slotsPerEpoch; slot < (epoch+1)*slotsPerEpoch; slot++ {
		for i := range s.CommitteeCount(epoch) {
			idxs, err := s.GetBeaconCommitee(slot, i)
			require.NoError(t, err)
			c := committeeResponse{Index: i, Slot: slot}
			for _, v := range idxs {
				c.Validators = append(c.Validators, strconv.FormatUint(v, 10))
			}
			out = append(out, c)
		}
	}
	return out
}
