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
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/antiquary"
	"github.com/erigontech/erigon/cl/beacon/beaconhttp"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/persistence/base_encoding"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	state_accessors "github.com/erigontech/erigon/cl/persistence/state"
	"github.com/erigontech/erigon/cl/persistence/state/historical_states_reader"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/snapshotsync"
	"github.com/erigontech/erigon/node/ethconfig"
)

func dutiesSpecStatusRequest(t *testing.T, handler *ApiHandler, method, path, body string) (int, string) {
	t.Helper()
	recorder := httptest.NewRecorder()
	handler.mux.ServeHTTP(recorder, httptest.NewRequestWithContext(t.Context(), method, path, strings.NewReader(body)))
	return recorder.Code, recorder.Body.String()
}

func TestDutiesEndpointsAnswerSpecListedStatuses(t *testing.T) {
	setProgress := func(t *testing.T, db kv.RwDB, slot uint64) {
		t.Helper()
		require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
			return state_accessors.SetStateProcessingProgress(tx, slot)
		}))
	}
	setupMissingProposerRoot := func(t *testing.T) *ApiHandler {
		t.Helper()
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
	}
	setupProposerValidatorTable := func(t *testing.T, tableSlot uint64, partial bool) *ApiHandler {
		t.Helper()
		db, _, _, preState, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
		fcu.FinalizedSlotVal = 127
		setProgress(t, db, 96)
		validatorTable := state_accessors.NewStaticValidatorTable()
		if partial {
			validator, err := preState.ValidatorForValidatorIndex(0)
			require.NoError(t, err)
			require.True(t, validator.Active(3))
			require.NoError(t, validatorTable.AddValidator(validator, 0, 0))
			require.Greater(t, preState.ValidatorLength(), validatorTable.Length())
		}
		validatorTable.SetSlot(tableSlot)
		handler.stateReader = historical_states_reader.NewHistoricalStatesReader(handler.beaconChainCfg, nil, validatorTable, preState, nil, handler.syncedData)
		return handler
	}

	// proposer duties v1/v2 and attester duties list 200, 400, 500 and 503 only; none of
	// these failure modes may answer 404.
	cases := []struct {
		name        string
		setup       func(t *testing.T) *ApiHandler
		method      string
		path        string
		body        string
		wantStatus  int
		wantMessage string
		checkDuties func(t *testing.T, handler *ApiHandler, body string)
	}{
		{
			name: "proposer genesis reconstruction pending",
			setup: func(t *testing.T) *ApiHandler {
				db, _, _, _, _, handler, _, _, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
				require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
					return tx.Delete(kv.StatesProcessingProgress, kv.StatesProcessingKey)
				}))
				return handler
			},
			method:      http.MethodGet,
			path:        "/eth/v1/validator/duties/proposer/0",
			wantStatus:  http.StatusServiceUnavailable,
			wantMessage: "historical state for slot 0 is not yet reconstructed",
		},
		{
			name: "proposer genesis block not found",
			setup: func(t *testing.T) *ApiHandler {
				db, _, _, _, _, handler, _, _, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
				setProgress(t, db, 0)
				return handler
			},
			method:      http.MethodGet,
			path:        "/eth/v1/validator/duties/proposer/0",
			wantStatus:  http.StatusInternalServerError,
			wantMessage: "genesis block not found",
		},
		{
			name: "proposer reconstruction pending with missing root",
			setup: func(t *testing.T) *ApiHandler {
				handler := setupMissingProposerRoot(t)
				setProgress(t, handler.indiciesDB, 95)
				return handler
			},
			method:      http.MethodGet,
			path:        "/eth/v1/validator/duties/proposer/3",
			wantStatus:  http.StatusServiceUnavailable,
			wantMessage: "historical state for slot 96 is not yet reconstructed",
		},
		{
			name: "proposer reconstruction pending with available root",
			setup: func(t *testing.T) *ApiHandler {
				db, _, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
				fcu.FinalizedSlotVal = 127
				setProgress(t, db, 95)
				return handler
			},
			method:      http.MethodGet,
			path:        "/eth/v1/validator/duties/proposer/3",
			wantStatus:  http.StatusServiceUnavailable,
			wantMessage: "historical state for slot 96 is not yet reconstructed",
		},
		{
			name: "proposer dependent root not found",
			setup: func(t *testing.T) *ApiHandler {
				handler := setupMissingProposerRoot(t)
				setProgress(t, handler.indiciesDB, 96)
				return handler
			},
			method:      http.MethodGet,
			path:        "/eth/v1/validator/duties/proposer/3",
			wantStatus:  http.StatusInternalServerError,
			wantMessage: "dependent root not found for epoch 3",
		},
		{
			name: "proposer genesis state not found",
			setup: func(t *testing.T) *ApiHandler {
				db, _, _, preState, _, handler, _, _, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
				setProgress(t, db, 0)
				genesisRoot, err := preState.GetBlockRootAtSlot(0)
				require.NoError(t, err)
				require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
					return tx.Put(kv.BlockRoot, base_encoding.Encode64ToBytes4(0), genesisRoot[:])
				}))
				return handler
			},
			method:      http.MethodGet,
			path:        "/eth/v1/validator/duties/proposer/0",
			wantStatus:  http.StatusInternalServerError,
			wantMessage: "state not found for slot 0",
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
			name: "proposer validator table empty during replay",
			setup: func(t *testing.T) *ApiHandler {
				return setupProposerValidatorTable(t, 0, false)
			},
			method:      http.MethodGet,
			path:        "/eth/v1/validator/duties/proposer/3",
			wantStatus:  http.StatusServiceUnavailable,
			wantMessage: "static validator table does not yet cover slot 96",
		},
		{
			name: "proposer validator table partial during replay",
			setup: func(t *testing.T) *ApiHandler {
				return setupProposerValidatorTable(t, 0, true)
			},
			method:      http.MethodGet,
			path:        "/eth/v1/validator/duties/proposer/3",
			wantStatus:  http.StatusServiceUnavailable,
			wantMessage: "static validator table does not yet cover slot 96",
		},
		{
			name: "proposer active validators not found",
			setup: func(t *testing.T) *ApiHandler {
				return setupProposerValidatorTable(t, 96, false)
			},
			method:      http.MethodGet,
			path:        "/eth/v1/validator/duties/proposer/3",
			wantStatus:  http.StatusInternalServerError,
			wantMessage: "active validators not found for slot 96",
		},
		{
			name: "proposer duties with all-zero randao mix",
			setup: func(t *testing.T) *ApiHandler {
				db, _, _, preState, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
				fcu.FinalizedSlotVal = 95
				zeroMixState, err := preState.Copy()
				require.NoError(t, err)
				require.Equal(t, uint64(64), preState.Slot())
				require.NotEqual(t, common.Hash{}, preState.GetRandaoMix(0))
				require.NoError(t, zeroMixState.SetRandaoMixAt(0, common.Hash{}))
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
			checkDuties: func(t *testing.T, handler *ApiHandler, body string) {
				var response proposerDutiesResponse
				require.NoError(t, json.Unmarshal([]byte(body), &response))
				slotsPerEpoch := handler.beaconChainCfg.SlotsPerEpoch
				require.Len(t, response.Data, int(slotsPerEpoch))
				for i, duty := range response.Data {
					require.Equal(t, 2*slotsPerEpoch+uint64(i), duty.Slot)
				}
			},
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
			name: "attester historical genesis reconstruction pending",
			setup: func(t *testing.T) *ApiHandler {
				db, _, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
				lowest := uint64(33)
				fcu.LowestAvailableSlotVal = &lowest
				setProgress(t, db, 31)
				return handler
			},
			method:      http.MethodPost,
			path:        "/eth/v1/validator/duties/attester/1",
			body:        `["0"]`,
			wantStatus:  http.StatusServiceUnavailable,
			wantMessage: "historical state for slot 32 is not yet reconstructed",
		},
		{
			name: "attester historical genesis block not found",
			setup: func(t *testing.T) *ApiHandler {
				db, _, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
				lowest := uint64(33)
				fcu.LowestAvailableSlotVal = &lowest
				setProgress(t, db, 32)
				return handler
			},
			method:      http.MethodPost,
			path:        "/eth/v1/validator/duties/attester/1",
			body:        `["0"]`,
			wantStatus:  http.StatusInternalServerError,
			wantMessage: "genesis block not found",
		},
		{
			name: "attester historical reconstruction pending",
			setup: func(t *testing.T) *ApiHandler {
				db, _, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
				lowest := uint64(97)
				fcu.LowestAvailableSlotVal = &lowest
				setProgress(t, db, 63)
				return handler
			},
			method:      http.MethodPost,
			path:        "/eth/v1/validator/duties/attester/2",
			body:        `["0"]`,
			wantStatus:  http.StatusServiceUnavailable,
			wantMessage: "historical state for slot 64 is not yet reconstructed",
		},
		{
			name: "attester historical dependent root not found",
			setup: func(t *testing.T) *ApiHandler {
				db, _, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
				lowest := uint64(97)
				fcu.LowestAvailableSlotVal = &lowest
				setProgress(t, db, 64)
				return handler
			},
			method:      http.MethodPost,
			path:        "/eth/v1/validator/duties/attester/2",
			body:        `["0"]`,
			wantStatus:  http.StatusInternalServerError,
			wantMessage: "dependent root not found for epoch 2",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			handler := tc.setup(t)
			status, body := dutiesSpecStatusRequest(t, handler, tc.method, tc.path, tc.body)
			require.Equal(t, tc.wantStatus, status, body)
			if tc.wantMessage != "" {
				var response beaconhttp.EndpointError
				require.NoError(t, json.Unmarshal([]byte(body), &response))
				require.Equal(t, tc.wantMessage, response.Message)
			}
			if tc.checkDuties != nil {
				tc.checkDuties(t, handler, body)
			}
		})
	}
}

func TestHistoricalDutiesUseSnapshotCoverage(t *testing.T) {
	logger := log.New()
	db, _, _, preState, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, logger, true)
	genesisRoot, err := preState.GetBlockRootAtSlot(0)
	require.NoError(t, err)
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		if err := beacon_indicies.MarkRootCanonical(tx, 0, genesisRoot); err != nil {
			return err
		}
		return state_accessors.SetStateProcessingProgress(tx, 63)
	}))
	fcu.FinalizedSlotVal = 127

	dirs := datadir.New(t.TempDir())
	types := snapshotsync.MakeCaplinStateSnapshotsTypes(db, handler.beaconChainCfg)
	types.KeyValueGetters = map[string]snapshotsync.KeyValueGetter{kv.StateEvents: types.KeyValueGetters[kv.StateEvents]}
	snapshots := snapshotsync.NewCaplinStateSnapshots(ethconfig.BlocksFreezing{}, dirs, types, logger)
	t.Cleanup(snapshots.Close)
	require.NoError(t, snapshots.DumpCaplinState(t.Context(), 1000, 1000, 0, dirs, 1, log.LvlDebug, logger))
	require.NoError(t, snapshots.OpenFolder())
	require.Equal(t, uint64(999), snapshots.BlocksAvailable())
	handler.caplinStateSnapshots = snapshots

	t.Run("attester state from snapshots", func(t *testing.T) {
		status, body := dutiesSpecStatusRequest(t, handler, http.MethodPost, "/eth/v1/validator/duties/attester/2", `["0"]`)
		require.Equal(t, http.StatusOK, status, body)
		var response struct {
			Data []attesterDutyResponse
		}
		require.NoError(t, json.Unmarshal([]byte(body), &response))
		require.Len(t, response.Data, 1)
		require.Equal(t, uint64(0), response.Data[0].ValidatorIndex)
		require.Equal(t, uint64(2), response.Data[0].Slot/handler.beaconChainCfg.SlotsPerEpoch)
	})

	t.Run("proposer validator coverage after replay", func(t *testing.T) {
		validatorTable := state_accessors.NewStaticValidatorTable()
		refilled, err := antiquary.FillStaticValidatorsTableIfNeeded(logger, snapshots, validatorTable)
		require.NoError(t, err)
		require.True(t, refilled)
		require.Equal(t, snapshots.BlocksAvailable(), validatorTable.Slot())
		handler.stateReader = historical_states_reader.NewHistoricalStatesReader(handler.beaconChainCfg, nil, validatorTable, preState, snapshots, handler.syncedData)
		status, body := dutiesSpecStatusRequest(t, handler, http.MethodGet, "/eth/v1/validator/duties/proposer/3", "")
		require.Equal(t, http.StatusOK, status, body)
	})
}
