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
	"path/filepath"
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

func dumpDutiesSnapshots(t *testing.T, dirs datadir.Dirs, types snapshotsync.SnapshotTypes, dumpEnd, width uint64, logger log.Logger) *snapshotsync.CaplinStateSnapshots {
	t.Helper()
	s := snapshotsync.NewCaplinStateSnapshots(ethconfig.BlocksFreezing{}, dirs, types, logger)
	t.Cleanup(s.Close)
	require.NoError(t, s.DumpCaplinState(t.Context(), dumpEnd, width, 0, dirs, 1, log.LvlDebug, logger))
	require.NoError(t, s.OpenFolder())
	return s
}

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
	removeProposerRoots := func(t *testing.T, db kv.RwDB) {
		t.Helper()
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
	}
	setupMissingProposerRoot := func(t *testing.T) *ApiHandler {
		t.Helper()
		db, _, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
		fcu.FinalizedSlotVal = 127
		removeProposerRoots(t, db)
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
			name: "proposer validator replay pending with missing root",
			setup: func(t *testing.T) *ApiHandler {
				handler := setupProposerValidatorTable(t, 0, true)
				removeProposerRoots(t, handler.indiciesDB)
				return handler
			},
			method:      http.MethodGet,
			path:        "/eth/v1/validator/duties/proposer/3",
			wantStatus:  http.StatusServiceUnavailable,
			wantMessage: "static validator table does not yet cover slot 96",
		},
		{
			name: "proposer validator table truncated after replay",
			setup: func(t *testing.T) *ApiHandler {
				return setupProposerValidatorTable(t, 96, true)
			},
			method:     http.MethodGet,
			path:       "/eth/v1/validator/duties/proposer/3",
			wantStatus: http.StatusInternalServerError,
		},
		{
			name: "proposer validator table empty after replay",
			setup: func(t *testing.T) *ApiHandler {
				return setupProposerValidatorTable(t, 96, false)
			},
			method:     http.MethodGet,
			path:       "/eth/v1/validator/duties/proposer/3",
			wantStatus: http.StatusInternalServerError,
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
		{
			name: "attester active indices missing after reconstruction",
			setup: func(t *testing.T) *ApiHandler {
				db, _, _, preState, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
				lowest := uint64(97)
				fcu.LowestAvailableSlotVal = &lowest
				setProgress(t, db, 64)
				genesisRoot, err := preState.GetBlockRootAtSlot(0)
				require.NoError(t, err)
				require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
					if err := beacon_indicies.MarkRootCanonical(tx, 0, genesisRoot); err != nil {
						return err
					}
					getter := state_accessors.GetValFnTxAndSnapshot(tx, nil)
					_, err := handler.stateReader.ReadRandaoMixBySlotAndIndex(getter, 64, 0)
					require.NoError(t, err)
					indices, err := state_accessors.ReadActiveIndicies(getter, 64)
					require.NoError(t, err)
					require.Contains(t, indices, uint64(0))
					return tx.Delete(kv.ActiveValidatorIndicies, base_encoding.Encode64ToBytes4(64))
				}))
				return handler
			},
			method:      http.MethodPost,
			path:        "/eth/v1/validator/duties/attester/2",
			body:        `["0"]`,
			wantStatus:  http.StatusInternalServerError,
			wantMessage: "active validator indices not found for slot 64",
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
		})
	}
}

func TestProposerDutiesWithZeroRandaoMix(t *testing.T) {
	db, _, _, preState, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	fcu.FinalizedSlotVal = 95
	require.Equal(t, uint64(64), preState.Slot())
	require.NotEqual(t, common.Hash{}, preState.GetRandaoMix(0))
	require.NoError(t, preState.SetRandaoMixAt(0, common.Hash{}))
	genesisRoot, err := preState.GetBlockRootAtSlot(0)
	require.NoError(t, err)
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error { return beacon_indicies.MarkRootCanonical(tx, 0, genesisRoot) }))
	response := getProposerDutiesForEpoch(t, handler, 2)
	slotsPerEpoch := handler.beaconChainCfg.SlotsPerEpoch
	require.Len(t, response.Data, int(slotsPerEpoch))
	for i, duty := range response.Data {
		require.Equal(t, 2*slotsPerEpoch+uint64(i), duty.Slot)
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
	types.KeyValueGetters = map[string]snapshotsync.KeyValueGetter{
		kv.StateEvents:               types.KeyValueGetters[kv.StateEvents],
		kv.ActiveValidatorIndicies:   types.KeyValueGetters[kv.ActiveValidatorIndicies],
		kv.SlotData:                  types.KeyValueGetters[kv.SlotData],
		kv.ValidatorEffectiveBalance: types.KeyValueGetters[kv.ValidatorEffectiveBalance],
	}
	snapshots := dumpDutiesSnapshots(t, dirs, types, 1000, 1000, logger)
	require.Equal(t, uint64(999), snapshots.BlocksAvailable())
	handler.caplinStateSnapshots = snapshots
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		return tx.Delete(kv.ActiveValidatorIndicies, base_encoding.Encode64ToBytes4(64))
	}))

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
		require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
			if err := tx.Delete(kv.SlotData, base_encoding.Encode64ToBytes4(96)); err != nil {
				return err
			}
			return state_accessors.SetStateProcessingProgress(tx, 95)
		}))
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

func TestHistoricalAttesterDutiesSnapshotStartsAfterRequestedSlot(t *testing.T) {
	logger := log.New()
	db, _, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, logger, true)
	lowest := uint64(33)
	fcu.LowestAvailableSlotVal = &lowest
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		return state_accessors.SetStateProcessingProgress(tx, 31)
	}))

	dirs := datadir.New(t.TempDir())
	types := snapshotsync.SnapshotTypes{
		KeyValueGetters: map[string]snapshotsync.KeyValueGetter{
			kv.ActiveValidatorIndicies: func(uint64) ([]byte, []byte, error) { return nil, nil, nil },
		},
	}
	snapshots := dumpDutiesSnapshots(t, dirs, types, 100_000, 50_000, logger)
	files := snapshots.SegFileNames(50_000, 100_000)
	require.Len(t, files, 1)
	require.NoError(t, snapshots.OpenList([]string{filepath.Base(files[0])}, false))
	require.Equal(t, uint64(99_999), snapshots.BlocksAvailable())
	handler.caplinStateSnapshots = snapshots

	status, body := dutiesSpecStatusRequest(t, handler, http.MethodPost, "/eth/v1/validator/duties/attester/1", `["0"]`)
	require.Equal(t, http.StatusServiceUnavailable, status, body)
}

func TestHistoricalProposerDutiesValidatorTableRewind(t *testing.T) {
	db, _, _, preState, _, handler, _, _, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	validatorTable := state_accessors.NewStaticValidatorTable()
	tx, err := db.BeginRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, state_accessors.ReadValidatorsTable(tx, validatorTable))
	require.GreaterOrEqual(t, validatorTable.Slot(), uint64(96))
	handler.stateReader = historical_states_reader.NewHistoricalStatesReader(handler.beaconChainCfg, nil, validatorTable, preState, nil, handler.syncedData)

	getter := func(table string, key []byte) ([]byte, error) {
		if table == kv.SlotData {
			validatorTable.SetSlot(95)
		}
		return tx.GetOne(table, key)
	}
	duties, err := handler.getHistoricalProposerDuties(t.Context(), tx, getter, 3, 96)
	require.Equal(t, uint64(95), validatorTable.Slot())
	require.Error(t, err, "returned %d proposer duties after rewind", len(duties))
	require.Equal(t, http.StatusServiceUnavailable, beaconhttp.WrapEndpointError(err).Code)
}

func TestHistoricalAttesterRandaoPendingAtWriteSlot(t *testing.T) {
	logger := log.New()
	db, _, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, logger, true)
	lowest := uint64(129)
	fcu.LowestAvailableSlotVal = &lowest
	var active []byte
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		indices, err := tx.GetOne(kv.ActiveValidatorIndicies, base_encoding.Encode64ToBytes4(64))
		if err != nil {
			return err
		}
		active = append([]byte(nil), indices...)
		if err := tx.Delete(kv.RandaoMixes, base_encoding.Encode64ToBytes4(64)); err != nil {
			return err
		}
		return state_accessors.SetStateProcessingProgress(tx, 64)
	}))
	dirs := datadir.New(t.TempDir())
	types := snapshotsync.SnapshotTypes{KeyValueGetters: map[string]snapshotsync.KeyValueGetter{
		kv.BlockRoot: func(uint64) ([]byte, []byte, error) { return nil, make([]byte, 32), nil },
		kv.ActiveValidatorIndicies: func(slot uint64) ([]byte, []byte, error) {
			if slot == 128 {
				return nil, active, nil
			}
			return nil, nil, nil
		},
	}}
	snapshots := dumpDutiesSnapshots(t, dirs, types, 1000, 1000, logger)
	handler.caplinStateSnapshots = snapshots
	status, body := dutiesSpecStatusRequest(t, handler, http.MethodPost, "/eth/v1/validator/duties/attester/4", `["0"]`)
	require.Equal(t, http.StatusServiceUnavailable, status, body)
}

func TestHistoricalAttesterCanonicalRootBeforeSnapshot(t *testing.T) {
	for _, tc := range []struct {
		name          string
		canonicalSlot uint64
		progress      uint64
		status        int
	}{
		{"exact canonical root", 31, 0, http.StatusOK},
		{"earlier canonical root while indexing is pending", 30, 0, http.StatusServiceUnavailable},
		{"block root row written at the next slot", 30, 31, http.StatusServiceUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logger := log.New()
			db, _, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, logger, true)
			lowest := uint64(65)
			fcu.LowestAvailableSlotVal = &lowest
			root := common.Hash{1}
			require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
				if err := beacon_indicies.MarkRootCanonical(tx, tc.canonicalSlot, root); err != nil {
					return err
				}
				if err := tx.Delete(kv.BlockRoot, base_encoding.Encode64ToBytes4(31)); err != nil {
					return err
				}
				return state_accessors.SetStateProcessingProgress(tx, tc.progress)
			}))
			dirs := datadir.New(t.TempDir())
			getter := func(uint64) ([]byte, []byte, error) { return nil, nil, nil }
			active := dumpDutiesSnapshots(t, dirs, snapshotsync.SnapshotTypes{KeyValueGetters: map[string]snapshotsync.KeyValueGetter{kv.ActiveValidatorIndicies: getter}}, 100_000, 50_000, logger)
			activeFiles := active.SegFileNames(0, 50_000)
			require.Len(t, activeFiles, 1)
			rootGetter := func(uint64) ([]byte, []byte, error) { return nil, root[:], nil }
			roots := dumpDutiesSnapshots(t, dirs, snapshotsync.SnapshotTypes{KeyValueGetters: map[string]snapshotsync.KeyValueGetter{kv.BlockRoot: rootGetter}}, 100_000, 50_000, logger)
			rootFiles := roots.SegFileNames(50_000, 100_000)
			require.Len(t, rootFiles, 1)
			types := snapshotsync.SnapshotTypes{KeyValueGetters: map[string]snapshotsync.KeyValueGetter{kv.ActiveValidatorIndicies: getter, kv.BlockRoot: getter}}
			snapshots := snapshotsync.NewCaplinStateSnapshots(ethconfig.BlocksFreezing{}, dirs, types, logger)
			defer snapshots.Close()
			require.NoError(t, snapshots.OpenList([]string{filepath.Base(activeFiles[0]), filepath.Base(rootFiles[0])}, false))
			handler.caplinStateSnapshots = snapshots
			status, body := dutiesSpecStatusRequest(t, handler, http.MethodPost, "/eth/v1/validator/duties/attester/2", `[]`)
			require.Equal(t, tc.status, status, body)
		})
	}
}

func TestHistoricalProposerBalancesPendingBeforeRoot(t *testing.T) {
	logger := log.New()
	db, _, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, logger, true)
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
		return state_accessors.SetStateProcessingProgress(tx, 95)
	}))
	dirs := datadir.New(t.TempDir())
	types := snapshotsync.MakeCaplinStateSnapshotsTypes(db, handler.beaconChainCfg)
	types.KeyValueGetters = map[string]snapshotsync.KeyValueGetter{kv.SlotData: types.KeyValueGetters[kv.SlotData]}
	snapshots := dumpDutiesSnapshots(t, dirs, types, 1000, 1000, logger)
	handler.caplinStateSnapshots = snapshots
	require.True(t, handler.stateReader.ValidatorTableCovers(96))
	status, body := dutiesSpecStatusRequest(t, handler, http.MethodGet, "/eth/v1/validator/duties/proposer/3", "")
	require.Equal(t, http.StatusServiceUnavailable, status, body)
}

func TestHistoricalProposerRandaoPendingAtWriteSlot(t *testing.T) {
	logger := log.New()
	db, _, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, logger, true)
	fcu.FinalizedSlotVal = 160
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		data, err := tx.GetOne(kv.SlotData, base_encoding.Encode64ToBytes4(96))
		if err != nil {
			return err
		}
		if err := tx.Put(kv.SlotData, base_encoding.Encode64ToBytes4(128), data); err != nil {
			return err
		}
		if err := tx.Put(kv.BlockRoot, base_encoding.Encode64ToBytes4(127), make([]byte, 32)); err != nil {
			return err
		}
		if err := tx.Delete(kv.RandaoMixes, base_encoding.Encode64ToBytes4(64)); err != nil {
			return err
		}
		return state_accessors.SetStateProcessingProgress(tx, 64)
	}))
	dirs := datadir.New(t.TempDir())
	types := snapshotsync.MakeCaplinStateSnapshotsTypes(db, handler.beaconChainCfg)
	delete(types.KeyValueGetters, kv.RandaoMixes)
	snapshots := dumpDutiesSnapshots(t, dirs, types, 1000, 1000, logger)
	handler.caplinStateSnapshots = snapshots
	require.True(t, handler.stateReader.ValidatorTableCovers(128))
	status, body := dutiesSpecStatusRequest(t, handler, http.MethodGet, "/eth/v1/validator/duties/proposer/4", "")
	require.Equal(t, http.StatusServiceUnavailable, status, body)
}

func TestHistoricalReadinessRangeFindsGapBeforeSnapshot(t *testing.T) {
	logger := log.New()
	dirs := datadir.New(t.TempDir())
	getter := func(uint64) ([]byte, []byte, error) { return nil, []byte{1}, nil }
	types := snapshotsync.SnapshotTypes{KeyValueGetters: map[string]snapshotsync.KeyValueGetter{kv.ValidatorEffectiveBalance: getter}}
	dumped := dumpDutiesSnapshots(t, dirs, types, 100_000, 50_000, logger)
	files := dumped.SegFileNames(50_000, 100_000)
	require.Len(t, files, 1)
	snapshots := snapshotsync.NewCaplinStateSnapshots(ethconfig.BlocksFreezing{}, dirs, types, logger)
	defer snapshots.Close()
	require.NoError(t, snapshots.OpenList([]string{filepath.Base(files[0])}, false))
	view := snapshots.View()
	defer view.Close()
	readiness := historicalReadiness{progress: 49_983, reconstructed: true, view: view}

	require.NoError(t, readiness.checkRange(49_153, 49_983, kv.ValidatorEffectiveBalance))
	require.NoError(t, readiness.checkRange(50_000, 50_016, kv.ValidatorEffectiveBalance))
	err := readiness.checkRange(49_153, 50_016, kv.ValidatorEffectiveBalance)
	require.Error(t, err)
	require.Equal(t, http.StatusServiceUnavailable, beaconhttp.WrapEndpointError(err).Code)
}

func TestHistoricalProposerDutiesAtBalanceDumpBoundary(t *testing.T) {
	for _, tc := range []struct {
		name      string
		dumpReady bool
		want      int
	}{
		{"dump ready", true, http.StatusOK},
		{"dump pending", false, http.StatusServiceUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logger := log.New()
			db, _, _, preState, postState, handler, _, syncedData, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, logger, true)
			slot := uint64(clparams.SlotsPerDump)
			fcu.FinalizedSlotVal = slot + 32
			head, err := postState.Copy()
			require.NoError(t, err)
			require.NoError(t, head.SetSlot(slot+32))
			manager := syncedData.(*synced_data.SyncedDataManager)
			root, err := postState.GetBlockRootAtSlot(postState.Slot() - 1)
			require.NoError(t, err)
			require.NoError(t, manager.OnHeadStateWithBlockRoot(head, root))
			require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
				for _, row := range []struct {
					table    string
					from, to uint64
				}{
					{kv.SlotData, 96, slot},
					{kv.EffectiveBalancesDump, 0, slot},
					{kv.RandaoMixes, 64, slot - 64},
				} {
					data, err := tx.GetOne(row.table, base_encoding.Encode64ToBytes4(row.from))
					if err != nil {
						return err
					}
					if len(data) == 0 {
						t.Fatalf("missing %s at %d", row.table, row.from)
					}
					if err := tx.Put(row.table, base_encoding.Encode64ToBytes4(row.to), data); err != nil {
						return err
					}
				}
				if err := beacon_indicies.MarkRootCanonical(tx, slot-1, common.Hash{1}); err != nil {
					return err
				}
				if !tc.dumpReady {
					if err := tx.Delete(kv.EffectiveBalancesDump, base_encoding.Encode64ToBytes4(slot)); err != nil {
						return err
					}
				}
				return state_accessors.SetStateProcessingProgress(tx, slot-1)
			}))
			dirs := datadir.New(t.TempDir())
			types := snapshotsync.MakeCaplinStateSnapshotsTypes(db, handler.beaconChainCfg)
			getters := map[string]snapshotsync.KeyValueGetter{kv.SlotData: types.KeyValueGetters[kv.SlotData]}
			if tc.dumpReady {
				getters[kv.EffectiveBalancesDump] = types.KeyValueGetters[kv.EffectiveBalancesDump]
			}
			types.KeyValueGetters = getters
			snapshots := dumpDutiesSnapshots(t, dirs, types, 2000, 2000, logger)
			handler.caplinStateSnapshots = snapshots
			view := snapshots.View()
			_, slotCovered := view.VisibleSegment(slot, kv.SlotData)
			_, dumpCovered := view.VisibleSegment(slot, kv.EffectiveBalancesDump)
			_, diffCovered := view.VisibleSegment(slot, kv.ValidatorEffectiveBalance)
			view.Close()
			require.True(t, slotCovered)
			require.Equal(t, tc.dumpReady, dumpCovered)
			require.False(t, diffCovered)
			validatorTable := state_accessors.NewStaticValidatorTable()
			require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error { return state_accessors.ReadValidatorsTable(tx, validatorTable) }))
			validatorTable.SetSlot(slot)
			handler.stateReader = historical_states_reader.NewHistoricalStatesReader(handler.beaconChainCfg, nil, validatorTable, preState, snapshots, handler.syncedData)
			status, body := dutiesSpecStatusRequest(t, handler, http.MethodGet, "/eth/v1/validator/duties/proposer/48", "")
			require.Equal(t, tc.want, status, body)
		})
	}
}
