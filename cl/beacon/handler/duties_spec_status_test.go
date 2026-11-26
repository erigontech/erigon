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
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/persistence/base_encoding"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	state_accessors "github.com/erigontech/erigon/cl/persistence/state"
	"github.com/erigontech/erigon/cl/persistence/state/historical_states_reader"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
)

func dutiesSpecStatusRequest(t *testing.T, handler *ApiHandler, method, path, body string) (int, string) {
	t.Helper()
	recorder := httptest.NewRecorder()
	handler.mux.ServeHTTP(recorder, httptest.NewRequestWithContext(t.Context(), method, path, strings.NewReader(body)))
	return recorder.Code, recorder.Body.String()
}

func TestDutiesEndpointsAnswerSpecListedStatuses(t *testing.T) {
	// proposer duties v1/v2 and attester duties list 200, 400, 500 and 503 only; none of
	// these failure modes may answer 404.
	cases := []struct {
		name       string
		setup      func(t *testing.T) *ApiHandler
		method     string
		path       string
		body       string
		wantStatus int
	}{
		{
			name: "proposer genesis block not found",
			setup: func(t *testing.T) *ApiHandler {
				_, _, _, _, _, handler, _, _, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
				return handler
			},
			method:     http.MethodGet,
			path:       "/eth/v1/validator/duties/proposer/0",
			wantStatus: http.StatusInternalServerError,
		},
		{
			name: "proposer dependent root not found",
			setup: func(t *testing.T) *ApiHandler {
				db, _, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
				fcu.FinalizedSlotVal = 127
				require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
					for slot := uint64(0); slot <= 95; slot++ {
						if err := tx.Delete(kv.BlockRoot, base_encoding.Encode64ToBytes4(slot)); err != nil {
							return err
						}
						if err := tx.Delete(kv.CanonicalBlockRoots, base_encoding.Encode64ToBytes4(slot)); err != nil {
							return err
						}
					}
					return nil
				}))
				return handler
			},
			method:     http.MethodGet,
			path:       "/eth/v1/validator/duties/proposer/3",
			wantStatus: http.StatusInternalServerError,
		},
		{
			name: "proposer genesis state not found",
			setup: func(t *testing.T) *ApiHandler {
				db, _, _, preState, _, handler, _, _, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
				genesisRoot, err := preState.GetBlockRootAtSlot(0)
				require.NoError(t, err)
				require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
					return tx.Put(kv.BlockRoot, base_encoding.Encode64ToBytes4(0), genesisRoot[:])
				}))
				return handler
			},
			method:     http.MethodGet,
			path:       "/eth/v1/validator/duties/proposer/0",
			wantStatus: http.StatusInternalServerError,
		},
		{
			name: "proposer validators not found",
			setup: func(t *testing.T) *ApiHandler {
				db, _, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
				fcu.FinalizedSlotVal = 127
				require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
					return tx.Delete(kv.SlotData, base_encoding.Encode64ToBytes4(96))
				}))
				return handler
			},
			method:     http.MethodGet,
			path:       "/eth/v1/validator/duties/proposer/3",
			wantStatus: http.StatusInternalServerError,
		},
		{
			name: "proposer active validators not found",
			setup: func(t *testing.T) *ApiHandler {
				_, _, _, preState, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
				fcu.FinalizedSlotVal = 127
				handler.stateReader = historical_states_reader.NewHistoricalStatesReader(handler.beaconChainCfg, nil, state_accessors.NewStaticValidatorTable(), preState, nil, handler.syncedData)
				return handler
			},
			method:     http.MethodGet,
			path:       "/eth/v1/validator/duties/proposer/3",
			wantStatus: http.StatusInternalServerError,
		},
		{
			name: "proposer duties with all-zero randao mix",
			setup: func(t *testing.T) *ApiHandler {
				db, _, _, preState, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
				fcu.FinalizedSlotVal = 95
				zeroMixState, err := preState.Copy()
				require.NoError(t, err)
				require.NoError(t, zeroMixState.SetRandaoMixAt(8188, common.Hash{}))
				validatorTable := state_accessors.NewStaticValidatorTable()
				require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
					return state_accessors.ReadValidatorsTable(tx, validatorTable)
				}))
				handler.stateReader = historical_states_reader.NewHistoricalStatesReader(handler.beaconChainCfg, nil, validatorTable, zeroMixState, nil, handler.syncedData)
				genesisRoot, err := preState.GetBlockRootAtSlot(0)
				require.NoError(t, err)
				require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
					return beacon_indicies.MarkRootCanonical(tx, 0, genesisRoot)
				}))
				return handler
			},
			method:     http.MethodGet,
			path:       "/eth/v1/validator/duties/proposer/2",
			wantStatus: http.StatusOK,
		},
		{
			name: "attester head dependent root not found at genesis slot",
			setup: func(t *testing.T) *ApiHandler {
				_, _, _, _, postState, handler, _, syncedData, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
				craftedHead, err := postState.Copy()
				require.NoError(t, err)
				require.NoError(t, craftedHead.SetSlot(262145))
				manager, ok := syncedData.(*synced_data.SyncedDataManager)
				require.True(t, ok)
				headRoot, err := postState.GetBlockRootAtSlot(postState.Slot() - 1)
				require.NoError(t, err)
				require.NoError(t, manager.OnHeadStateWithBlockRoot(craftedHead, headRoot))
				return handler
			},
			method:     http.MethodPost,
			path:       "/eth/v1/validator/duties/attester/0",
			body:       `["0"]`,
			wantStatus: http.StatusInternalServerError,
		},
		{
			name: "attester head dependent root not found",
			setup: func(t *testing.T) *ApiHandler {
				_, _, _, _, postState, handler, _, syncedData, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
				craftedHead, err := postState.Copy()
				require.NoError(t, err)
				require.NoError(t, craftedHead.SetSlot(264224))
				manager, ok := syncedData.(*synced_data.SyncedDataManager)
				require.True(t, ok)
				headRoot, err := postState.GetBlockRootAtSlot(postState.Slot() - 1)
				require.NoError(t, err)
				require.NoError(t, manager.OnHeadStateWithBlockRoot(craftedHead, headRoot))
				return handler
			},
			method:     http.MethodPost,
			path:       "/eth/v1/validator/duties/attester/66",
			body:       `["0"]`,
			wantStatus: http.StatusInternalServerError,
		},
		{
			name: "attester historical genesis block not found",
			setup: func(t *testing.T) *ApiHandler {
				_, _, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
				lowest := uint64(33)
				fcu.LowestAvailableSlotVal = &lowest
				return handler
			},
			method:     http.MethodPost,
			path:       "/eth/v1/validator/duties/attester/1",
			body:       `["0"]`,
			wantStatus: http.StatusInternalServerError,
		},
		{
			name: "attester historical dependent root not found",
			setup: func(t *testing.T) *ApiHandler {
				_, _, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
				lowest := uint64(97)
				fcu.LowestAvailableSlotVal = &lowest
				return handler
			},
			method:     http.MethodPost,
			path:       "/eth/v1/validator/duties/attester/2",
			body:       `["0"]`,
			wantStatus: http.StatusInternalServerError,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			handler := tc.setup(t)
			status, body := dutiesSpecStatusRequest(t, handler, tc.method, tc.path, tc.body)
			require.Equal(t, tc.wantStatus, status, body)
		})
	}
}
