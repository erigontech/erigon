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
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/persistence/base_encoding"
	state_accessors "github.com/erigontech/erigon/cl/persistence/state"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
)

func TestGetProposerLookaheadHistoricalReadsEpochData(t *testing.T) {
	db, blocks, _, _, _, h, _, _, _, _ := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), false)
	cfg := h.beaconChainCfg
	cfg.FuluForkEpoch = 0

	slot := blocks[len(blocks)-1].Block.Slot
	epoch := slot / cfg.SlotsPerEpoch
	require.NotZero(t, epoch%cfg.SlotsPerEpoch)

	lookahead := solid.NewUint64VectorSSZ(int((1 + cfg.MinSeedLookahead) * cfg.SlotsPerEpoch))
	for i := 0; i < lookahead.Length(); i++ {
		lookahead.Set(i, uint64(1000+i))
	}
	epochData := &state_accessors.EpochData{
		JustificationBits: &cltypes.JustificationBits{},
		ProposerLookahead: lookahead,
		BeaconConfig:      cfg,
		Version:           clparams.FuluVersion,
	}
	var buf bytes.Buffer
	require.NoError(t, epochData.WriteTo(&buf))
	tx, err := db.BeginRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, tx.Put(kv.EpochData, base_encoding.Encode64ToBytes4(epoch*cfg.SlotsPerEpoch), buf.Bytes()))
	require.NoError(t, tx.Commit())

	resp := getProposerLookahead(t, h, slot)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var out struct {
		Data []string `json:"data"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	require.Len(t, out.Data, lookahead.Length())
	require.Equal(t, "1000", out.Data[0])
}

func TestGetProposerLookaheadHistoricalWithoutEpochData(t *testing.T) {
	db, blocks, _, _, _, h, _, _, _, _ := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), false)
	cfg := h.beaconChainCfg
	cfg.FuluForkEpoch = 0
	slot := blocks[len(blocks)-1].Block.Slot

	tx, err := db.BeginRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, tx.Delete(kv.EpochData, base_encoding.Encode64ToBytes4(cfg.RoundSlotToEpoch(slot))))
	require.NoError(t, tx.Commit())

	resp := getProposerLookahead(t, h, slot)
	defer resp.Body.Close()
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func getProposerLookahead(t *testing.T, h *ApiHandler, slot uint64) *http.Response {
	server := httptest.NewServer(h.mux)
	t.Cleanup(server.Close)
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, server.URL+"/eth/v1/beacon/states/"+strconv.FormatUint(slot, 10)+"/proposer_lookahead", nil)
	require.NoError(t, err)
	resp, err := server.Client().Do(req)
	require.NoError(t, err)
	return resp
}
