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
	"context"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/beacon/beaconhttp"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/clparams/initial_state"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/persistence/base_encoding"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	state_accessors "github.com/erigontech/erigon/cl/persistence/state"
	"github.com/erigontech/erigon/cl/persistence/state/historical_states_reader"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/transition"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
)

func TestGetDutiesProposerHistoricalEpochIgnoresHeadEffectiveBalance(t *testing.T) {
	_, blocks, _, _, postState, handler, _, syncedData, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)

	headRoot, err := blocks[len(blocks)-1].Block.HashSSZ()
	require.NoError(t, err)
	fcu.HeadVal = headRoot
	fcu.HeadSlotVal = postState.Slot()

	epoch := uint64(3)
	expectedSlot := epoch * handler.beaconChainCfg.SlotsPerEpoch
	fcu.FinalizedCheckpointVal = solid.Checkpoint{Epoch: epoch, Root: headRoot}
	fcu.FinalizedSlotVal = (epoch+1)*handler.beaconChainCfg.SlotsPerEpoch - 1
	fcu.LowestAvailableSlotVal = &expectedSlot

	before := getProposerDutiesForEpoch(t, handler, epoch)
	require.True(t, before.Finalized)
	require.NotEmpty(t, before.DependentRoot)

	headWithChangedBalances, err := postState.Copy()
	require.NoError(t, err)
	activeIndices := headWithChangedBalances.GetActiveValidatorsIndices(epoch)
	require.NotEmpty(t, activeIndices)
	chosenIndex := activeIndices[0]
	for _, validatorIndex := range activeIndices {
		require.NoError(t, headWithChangedBalances.SetEffectiveBalanceForValidatorAtIndex(int(validatorIndex), 0))
	}
	require.NoError(t, headWithChangedBalances.SetEffectiveBalanceForValidatorAtIndex(int(chosenIndex), handler.beaconChainCfg.MaxEffectiveBalanceForVersion(headWithChangedBalances.Version())))

	manager, ok := syncedData.(*synced_data.SyncedDataManager)
	require.True(t, ok)
	require.NoError(t, manager.OnHeadStateWithBlockRoot(headWithChangedBalances, headRoot))

	after := getProposerDutiesForEpoch(t, handler, epoch)
	require.Equal(t, before, after)
}

func TestGetDutiesProposerMissingRandaoMixReturns500(t *testing.T) {
	db, _, _, _, post, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	epoch := uint64(2)
	fcu.FinalizedSlotVal = (epoch+1)*handler.beaconChainCfg.SlotsPerEpoch - 1
	handler.beaconChainCfg.MinSeedLookahead = handler.beaconChainCfg.EpochsPerHistoricalVector - 1
	key := base_encoding.Encode64ToBytes4(epoch * handler.beaconChainCfg.SlotsPerEpoch)
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		root, err := post.GetBlockRootAtSlot(epoch*handler.beaconChainCfg.SlotsPerEpoch - 1)
		if err != nil {
			return err
		}
		if err := beacon_indicies.MarkRootCanonical(tx, epoch*handler.beaconChainCfg.SlotsPerEpoch-1, root); err != nil {
			return err
		}
		return tx.Delete(kv.IntraRandaoMixes, key)
	}))
	recorder := httptest.NewRecorder()
	path := "/eth/v1/validator/duties/proposer/" + strconv.FormatUint(epoch, 10)
	handler.mux.ServeHTTP(recorder, httptest.NewRequestWithContext(t.Context(), http.MethodGet, path, http.NoBody))
	require.Equal(t, http.StatusInternalServerError, recorder.Code, recorder.Body.String())
	require.Contains(t, recorder.Body.String(), "randao mix")
}

func TestGetAttesterDutiesMissingRandaoMixReturns500(t *testing.T) {
	db, blocks, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	epoch := uint64(1)
	handler.beaconChainCfg.MinSeedLookahead = handler.beaconChainCfg.EpochsPerHistoricalVector - 1
	epochSlot := epoch * handler.beaconChainCfg.SlotsPerEpoch
	lowestAvailableSlot := epochSlot + 1
	fcu.LowestAvailableSlotVal = &lowestAvailableSlot
	key := base_encoding.Encode64ToBytes4(epochSlot)
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		root, err := blocks[0].Block.HashSSZ()
		if err != nil {
			return err
		}
		if err := beacon_indicies.MarkRootCanonical(tx, 0, root); err != nil {
			return err
		}
		return tx.Delete(kv.IntraRandaoMixes, key)
	}))
	path := "/eth/v1/validator/duties/attester/1"
	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, path, strings.NewReader(`["0"]`))
	recorder := httptest.NewRecorder()
	handler.mux.ServeHTTP(recorder, request)
	require.Equal(t, http.StatusInternalServerError, recorder.Code, recorder.Body.String())
}

func TestGetHistoricalProposerDependentRootEpochZeroReturnsGenesisRoot(t *testing.T) {
	db, _, _, preState, _, handler, _, _, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)

	genesisRoot, err := preState.GetBlockRootAtSlot(0)
	require.NoError(t, err)
	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, beacon_indicies.MarkRootCanonical(tx, 0, genesisRoot))
	require.NoError(t, tx.Commit())

	roTx, err := db.BeginRo(context.Background())
	require.NoError(t, err)
	defer roTx.Rollback()
	dependentRoot, err := handler.getHistoricalProposerDependentRoot(roTx, state_accessors.GetValFnTxAndSnapshot(roTx, nil), 0, false)
	require.NoError(t, err)
	require.Equal(t, genesisRoot, dependentRoot)
}

func TestGetDutiesProposerEpochZeroReturnsGenesisRootAndDuties(t *testing.T) {
	db, _, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.Phase0Version, log.Root(), true)

	genesisState, err := initial_state.GetGenesisState(t.Context(), chainspec.MainnetChainID)
	require.NoError(t, err)
	handler.stateReader = historical_states_reader.NewHistoricalStatesReader(handler.beaconChainCfg, nil, state_accessors.NewStaticValidatorTable(), genesisState, nil, handler.syncedData)

	genesisRoot := common.Hash{1, 2, 3}
	tx, err := db.BeginRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, beacon_indicies.MarkRootCanonical(tx, 0, genesisRoot))
	require.NoError(t, tx.Commit())

	fcu.FinalizedCheckpointVal = solid.Checkpoint{Epoch: 0, Root: genesisRoot}
	fcu.FinalizedSlotVal = handler.beaconChainCfg.SlotsPerEpoch - 1

	expectedProposers, err := genesisState.GetBeaconProposerIndices(0)
	require.NoError(t, err)

	response := getProposerDutiesForEpoch(t, handler, 0)
	require.True(t, response.Finalized)
	require.Equal(t, genesisRoot.String(), response.DependentRoot)
	require.Len(t, response.Data, int(handler.beaconChainCfg.SlotsPerEpoch))
	for i, duty := range response.Data {
		require.Equal(t, uint64(i), duty.Slot)
		require.Equal(t, expectedProposers[i], duty.ValidatorIndex)
	}
}

func TestGetDutiesProposerAdvancesHeadCopyAcrossTargetFork(t *testing.T) {
	_, blocks, _, _, postState, handler, _, syncedData, fcu, _ := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), true)

	headRoot, err := blocks[len(blocks)-1].Block.HashSSZ()
	require.NoError(t, err)

	headEpoch := postState.Slot() / handler.beaconChainCfg.SlotsPerEpoch
	targetEpoch := headEpoch + 1
	handler.beaconChainCfg.FuluForkEpoch = headEpoch
	handler.beaconChainCfg.GloasForkEpoch = targetEpoch
	handler.beaconChainCfg.InitializeForkSchedule()

	stateCfg := *postState.BeaconConfig()
	stateCfg.FuluForkEpoch = headEpoch
	stateCfg.GloasForkEpoch = targetEpoch
	stateCfg.InitializeForkSchedule()
	encodedPostState, err := postState.EncodeSSZ(nil)
	require.NoError(t, err)
	isolatedPostState := state.New(&stateCfg)
	require.NoError(t, isolatedPostState.DecodeSSZ(encodedPostState, int(clparams.ElectraVersion)))

	fuluHead, err := isolatedPostState.Copy()
	require.NoError(t, err)
	require.NoError(t, fuluHead.UpgradeToFulu())
	require.Equal(t, clparams.FuluVersion, fuluHead.Version())

	fcu.HeadVal = headRoot
	fcu.HeadSlotVal = fuluHead.Slot()

	manager, ok := syncedData.(*synced_data.SyncedDataManager)
	require.True(t, ok)
	require.NoError(t, manager.OnHeadStateWithBlockRoot(fuluHead, headRoot))

	expectedState, err := fuluHead.Copy()
	require.NoError(t, err)
	expectedSlot := targetEpoch * handler.beaconChainCfg.SlotsPerEpoch
	require.NoError(t, transition.DefaultMachine.ProcessSlots(expectedState, expectedSlot))
	require.Equal(t, clparams.GloasVersion, expectedState.Version())
	expectedProposers, err := expectedState.GetBeaconProposerIndices(targetEpoch)
	require.NoError(t, err)

	response := getProposerDutiesForEpoch(t, handler, targetEpoch)
	require.False(t, response.Finalized)
	require.Equal(t, clparams.GloasVersion.String(), response.Version)
	require.NoError(t, handler.syncedData.ViewHeadState(func(headState *state.CachingBeaconState) error {
		require.Equal(t, clparams.FuluVersion, headState.Version())
		return nil
	}))
	for i, duty := range response.Data {
		require.Equal(t, expectedSlot+uint64(i), duty.Slot)
		require.Equal(t, expectedProposers[i], duty.ValidatorIndex)
	}
	path := "/eth/v1/validator/duties/proposer/" + strconv.FormatUint(headEpoch+3, 10)
	recorder := httptest.NewRecorder()
	handler.mux.ServeHTTP(recorder, httptest.NewRequestWithContext(t.Context(), http.MethodGet, path, http.NoBody))
	require.Equal(t, http.StatusServiceUnavailable, recorder.Code)
}

func TestGetDutiesProposerFuluEpochBounds(t *testing.T) {
	_, _, _, _, postState, handler, _, manager, _, _ := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), true)
	headRoot := manager.HeadRoot()
	cfg := handler.beaconChainCfg
	headEpoch := postState.Slot() / cfg.SlotsPerEpoch
	targetEpoch := headEpoch + 2
	cfg.FuluForkEpoch = headEpoch
	cfg.InitializeForkSchedule()

	fuluHead, err := handler.copyHeadStateForDuties(postState)
	require.NoError(t, err)
	require.NoError(t, fuluHead.UpgradeToFulu())
	activeIndices := fuluHead.GetActiveValidatorsIndices(targetEpoch)
	require.Greater(t, len(activeIndices), 1)
	for _, index := range activeIndices[1:] {
		require.NoError(t, fuluHead.SetEffectiveBalanceForValidatorAtIndex(int(index), 0))
	}
	require.NoError(t, manager.OnHeadStateWithBlockRoot(fuluHead, headRoot))

	t.Run("beyond_seed", func(t *testing.T) {
		path := "/eth/v1/validator/duties/proposer/" + strconv.FormatUint(headEpoch+3, 10)
		recorder := httptest.NewRecorder()
		handler.mux.ServeHTTP(recorder, httptest.NewRequestWithContext(t.Context(), http.MethodGet, path, http.NoBody))
		require.Equal(t, http.StatusServiceUnavailable, recorder.Code)
	})
	t.Run("at_seed_boundary", func(t *testing.T) {
		getProposerDutiesForEpoch(t, handler, targetEpoch)
	})
	t.Run("advance_uses_state_epoch", func(t *testing.T) {
		targetSlot := targetEpoch * cfg.SlotsPerEpoch
		handler.syncedData = proposerHeadSlotOverride{SyncedData: manager, slot: targetSlot}

		advancedState, err := fuluHead.Copy()
		require.NoError(t, err)
		require.NoError(t, transition.DefaultMachine.ProcessSlots(advancedState, targetSlot))
		expected, err := advancedState.GetBeaconProposerIndices(targetEpoch)
		require.NoError(t, err)
		response := getProposerDutiesForEpoch(t, handler, targetEpoch)
		require.Len(t, response.Data, len(expected))
		for i, duty := range response.Data {
			require.Equal(t, expected[i], duty.ValidatorIndex, "slot %d", targetSlot+uint64(i))
		}
	})
}

type proposerHeadSlotOverride struct {
	synced_data.SyncedData
	slot uint64
}

func (s proposerHeadSlotOverride) HeadSlot() uint64 {
	return s.slot
}

func TestGetDutiesProposerUsesTargetForkBeforeHeadFork(t *testing.T) {
	_, blocks, _, _, postState, handler, _, syncedData, fcu, _ := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), true)

	headRoot, err := blocks[len(blocks)-1].Block.HashSSZ()
	require.NoError(t, err)

	headEpoch := postState.Slot() / handler.beaconChainCfg.SlotsPerEpoch
	targetEpoch := headEpoch - 1
	handler.beaconChainCfg.ElectraForkEpoch = headEpoch
	handler.beaconChainCfg.FuluForkEpoch = headEpoch
	handler.beaconChainCfg.GloasForkEpoch = headEpoch
	handler.beaconChainCfg.InitializeForkSchedule()

	stateCfg := *postState.BeaconConfig()
	stateCfg.ElectraForkEpoch = headEpoch
	stateCfg.FuluForkEpoch = headEpoch
	stateCfg.GloasForkEpoch = headEpoch
	stateCfg.InitializeForkSchedule()
	encodedPostState, err := postState.EncodeSSZ(nil)
	require.NoError(t, err)
	isolatedPostState := state.New(&stateCfg)
	require.NoError(t, isolatedPostState.DecodeSSZ(encodedPostState, int(clparams.ElectraVersion)))

	gloasHead, err := isolatedPostState.Copy()
	require.NoError(t, err)
	require.NoError(t, gloasHead.UpgradeToFulu())
	require.NoError(t, gloasHead.UpgradeToGloas())
	require.Equal(t, clparams.GloasVersion, gloasHead.Version())

	fcu.HeadVal = headRoot
	fcu.HeadSlotVal = gloasHead.Slot()
	fcu.FinalizedSlotVal = targetEpoch*handler.beaconChainCfg.SlotsPerEpoch - 1

	manager, ok := syncedData.(*synced_data.SyncedDataManager)
	require.True(t, ok)
	require.NoError(t, manager.OnHeadStateWithBlockRoot(gloasHead, headRoot))

	expectedState, err := gloasHead.Copy()
	require.NoError(t, err)
	expectedState.SetVersion(clparams.DenebVersion)
	expectedProposers, err := expectedState.GetBeaconProposerIndices(targetEpoch)
	require.NoError(t, err)
	gloasProposers, err := gloasHead.GetBeaconProposerIndices(targetEpoch)
	require.NoError(t, err)
	require.NotEqual(t, gloasProposers, expectedProposers)

	response := getProposerDutiesForEpoch(t, handler, targetEpoch)
	require.False(t, response.Finalized)
	require.Equal(t, clparams.DenebVersion.String(), response.Version)
	require.NoError(t, handler.syncedData.ViewHeadState(func(headState *state.CachingBeaconState) error {
		require.Equal(t, clparams.GloasVersion, headState.Version())
		return nil
	}))
	expectedSlot := targetEpoch * handler.beaconChainCfg.SlotsPerEpoch
	for i, duty := range response.Data {
		require.Equal(t, expectedSlot+uint64(i), duty.Slot)
		require.Equal(t, expectedProposers[i], duty.ValidatorIndex)
	}
}

func TestGetDutiesProposerUsesHeadStateWhenFinalizedSlotIsStillAvailable(t *testing.T) {
	db, blocks, _, _, postState, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)

	headRoot, err := blocks[len(blocks)-1].Block.HashSSZ()
	require.NoError(t, err)
	fcu.HeadVal = headRoot
	fcu.HeadSlotVal = postState.Slot()

	epoch := postState.Slot() / handler.beaconChainCfg.SlotsPerEpoch
	expectedSlot := epoch * handler.beaconChainCfg.SlotsPerEpoch
	fcu.FinalizedCheckpointVal = solid.Checkpoint{Epoch: epoch, Root: headRoot}
	fcu.FinalizedSlotVal = expectedSlot + handler.beaconChainCfg.SlotsPerEpoch - 1
	fcu.LowestAvailableSlotVal = &expectedSlot

	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, state_accessors.SetStateProcessingProgress(tx, expectedSlot-1))
	require.NoError(t, tx.Commit())

	response := getProposerDutiesForEpoch(t, handler, epoch)
	require.True(t, response.Finalized)
	require.Equal(t, handler.beaconChainCfg.GetCurrentStateVersion(epoch).String(), response.Version)
	for i, duty := range response.Data {
		require.Equal(t, expectedSlot+uint64(i), duty.Slot)
	}
}

func TestGetDutiesProposerFutureEpochUsesHeadStateBeforeLowestAvailableSlot(t *testing.T) {
	_, blocks, _, _, postState, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)

	headRoot, err := blocks[len(blocks)-1].Block.HashSSZ()
	require.NoError(t, err)
	fcu.HeadVal = headRoot
	fcu.HeadSlotVal = postState.Slot()

	headEpoch := postState.Slot() / handler.beaconChainCfg.SlotsPerEpoch
	epoch := headEpoch + 1
	expectedSlot := epoch * handler.beaconChainCfg.SlotsPerEpoch
	lowestAvailableSlot := expectedSlot + handler.beaconChainCfg.SlotsPerEpoch
	fcu.LowestAvailableSlotVal = &lowestAvailableSlot

	response := getProposerDutiesForEpoch(t, handler, epoch)
	require.False(t, response.Finalized)
	require.Equal(t, handler.beaconChainCfg.GetCurrentStateVersion(epoch).String(), response.Version)
	for i, duty := range response.Data {
		require.Equal(t, expectedSlot+uint64(i), duty.Slot)
	}
}

func TestGetDutiesProposerFutureEpochBounds(t *testing.T) {
	_, _, _, _, postState, handler, _, _, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)

	headEpoch := postState.Slot() / handler.beaconChainCfg.SlotsPerEpoch
	for _, tc := range []struct {
		epoch uint64
		code  int
	}{
		{headEpoch + 2, http.StatusOK},
		{headEpoch + 3, http.StatusServiceUnavailable},
		{headEpoch + maxEpochsLookaheadForDuties + 1, http.StatusBadRequest},
	} {
		t.Run(strconv.FormatUint(tc.epoch, 10), func(t *testing.T) {
			request := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/eth/v1/validator/duties/proposer/"+strconv.FormatUint(tc.epoch, 10), http.NoBody)
			recorder := httptest.NewRecorder()
			handler.mux.ServeHTTP(recorder, request)
			require.Equal(t, tc.code, recorder.Code, recorder.Body.String())
		})
	}
}

type dutyViewHooks struct {
	synced_data.SyncedData
	beforeView, afterView func()
}

func (s *dutyViewHooks) ViewHeadState(fn synced_data.ViewHeadStateFn) error {
	return s.ViewHeadStateWithIdentity(func(head *state.CachingBeaconState, _ common.Hash, _ uint64) error {
		return fn(head)
	})
}

func (s *dutyViewHooks) ViewHeadStateWithIdentity(fn synced_data.ViewHeadStateWithIdentityFn) error {
	if beforeView := s.beforeView; beforeView != nil {
		s.beforeView = nil
		beforeView()
	}
	if err := s.SyncedData.ViewHeadStateWithIdentity(fn); err != nil {
		return err
	}
	if afterView := s.afterView; afterView != nil {
		s.afterView = nil
		afterView()
	}
	return nil
}

type dutyOptimisticStore struct {
	forkchoice.ForkChoiceStorage
	head           synced_data.SyncedData
	optimisticRoot common.Hash
}

func (f dutyOptimisticStore) IsRootOptimistic(root common.Hash) bool {
	return root == f.optimisticRoot
}

func (f dutyOptimisticStore) IsHeadOptimistic() bool {
	return f.IsRootOptimistic(f.head.HeadRoot())
}

func TestDutiesUseSameHeadAsDependentRoot(t *testing.T) {
	_, _, _, _, head, h, _, manager, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	epoch := state.Epoch(head) + 2
	setDutiesClockEpoch(h, epoch-1)
	newHead, err := head.Copy()
	require.NoError(t, err)
	cfg := head.BeaconConfig()
	mixIndex := (epoch + cfg.EpochsPerHistoricalVector - cfg.MinSeedLookahead - 1) % cfg.EpochsPerHistoricalVector
	require.NoError(t, newHead.SetRandaoMixAt(int(mixIndex), common.Hash{1}))
	oldRoot, newRoot := common.Hash{2}, common.Hash{3}
	h.forkchoiceStore = dutyOptimisticStore{ForkChoiceStorage: h.forkchoiceStore, head: manager, optimisticRoot: oldRoot}

	for _, tc := range []struct{ name, method, body string }{
		{"attester", http.MethodPost, `["0","1","2"]`},
		{"proposer", http.MethodGet, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			type response struct {
				Data          json.RawMessage `json:"data"`
				DependentRoot common.Hash     `json:"dependent_root"`
				Optimistic    *bool           `json:"execution_optimistic"`
			}
			getDuties := func() response {
				path := "/eth/v1/validator/duties/" + tc.name + "/" + strconv.FormatUint(epoch, 10)
				req := httptest.NewRequestWithContext(t.Context(), tc.method, path, strings.NewReader(tc.body))
				recorder := httptest.NewRecorder()
				h.mux.ServeHTTP(recorder, req)
				require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
				var body response
				require.NoError(t, json.NewDecoder(recorder.Body).Decode(&body))
				return body
			}

			require.NoError(t, manager.OnHeadStateWithBlockRoot(head, oldRoot))
			before := getDuties()
			require.Equal(t, oldRoot, before.DependentRoot)
			h.syncedData = &dutyViewHooks{SyncedData: manager, afterView: func() {
				require.NoError(t, manager.OnHeadStateWithBlockRoot(newHead, newRoot))
			}}
			during := getDuties()
			h.syncedData = manager
			after := getDuties()
			require.Equal(t, newRoot, after.DependentRoot)
			require.NotEqual(t, before.Data, after.Data)
			require.NotNil(t, before.Optimistic)
			require.True(t, *before.Optimistic)
			require.NotNil(t, after.Optimistic)
			require.False(t, *after.Optimistic)
			require.Equal(t, oldRoot, during.DependentRoot)
			require.Equal(t, string(before.Data), string(during.Data), "duties must match the head that supplied dependent_root")
			require.NotNil(t, during.Optimistic)
			require.True(t, *during.Optimistic, "execution_optimistic must match the pinned head")
		})
	}
}

func TestGetDutiesProposerUsesReplacementHeadBeforeView(t *testing.T) {
	db, _, _, _, replacement, h, _, manager, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		return state_accessors.SetStateProcessingProgress(tx, 0)
	}))

	cfg := h.beaconChainCfg
	epoch := state.Epoch(replacement)
	epochSlot := epoch * cfg.SlotsPerEpoch
	oldHead, err := replacement.Copy()
	require.NoError(t, err)
	require.NoError(t, oldHead.SetSlot(epochSlot+cfg.SlotsPerEpoch))
	require.NoError(t, manager.OnHeadStateWithBlockRoot(oldHead, common.Hash{0x44}))
	oldDuties := getProposerDutiesForEpoch(t, h, epoch)
	lowestAvailableSlot := oldHead.Slot()
	fcu.LowestAvailableSlotVal = &lowestAvailableSlot

	require.NoError(t, replacement.SetSlot(oldHead.Slot()-1))
	mixIndex := (epoch + cfg.EpochsPerHistoricalVector - cfg.MinSeedLookahead - 1) % cfg.EpochsPerHistoricalVector
	require.NoError(t, replacement.SetRandaoMixAt(int(mixIndex), common.Hash{0x41}))
	dependentRoot := common.Hash{0x42}
	require.NoError(t, replacement.SetBlockRootAt(int((epochSlot-1)%cfg.SlotsPerHistoricalRoot), dependentRoot))
	require.NoError(t, manager.OnHeadStateWithBlockRoot(replacement, common.Hash{0x43}))
	expected := getProposerDutiesForEpoch(t, h, epoch)
	require.Equal(t, dependentRoot.String(), expected.DependentRoot)
	require.NotEqual(t, oldDuties.Data, expected.Data)

	require.NoError(t, manager.OnHeadStateWithBlockRoot(oldHead, common.Hash{0x44}))
	h.syncedData = &dutyViewHooks{SyncedData: manager, beforeView: func() {
		require.NoError(t, manager.OnHeadStateWithBlockRoot(replacement, common.Hash{0x43}))
	}}
	actual := getProposerDutiesForEpoch(t, h, epoch)
	require.Equal(t, expected.DependentRoot, actual.DependentRoot)
	require.Equal(t, expected.Data, actual.Data)
}

func TestGetDutiesProposerLogsAdvanceFailureAfterView(t *testing.T) {
	_, _, _, _, head, h, _, manager, _, _ := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), true)
	epoch := state.Epoch(head)
	h.beaconChainCfg.FuluForkEpoch = epoch
	h.beaconChainCfg.InitializeForkSchedule()

	var released, warned, warnedAfterView atomic.Bool
	h.syncedData = &dutyViewHooks{SyncedData: manager, afterView: func() {
		released.Store(true)
	}}
	logger := log.Root()
	previous := logger.GetHandler()
	t.Cleanup(func() { logger.SetHandler(previous) })
	logger.SetHandler(log.FilterHandler(func(r *log.Record) bool {
		if r.Msg == "getDutiesProposer: failed to advance state to target fork" {
			warnedAfterView.Store(released.Load())
			warned.Store(true)
		}
		return true
	}, previous))

	path := "/eth/v1/validator/duties/proposer/" + strconv.FormatUint(epoch, 10)
	recorder := httptest.NewRecorder()
	h.mux.ServeHTTP(recorder, httptest.NewRequestWithContext(t.Context(), http.MethodGet, path, http.NoBody))
	require.Equal(t, http.StatusInternalServerError, recorder.Code, recorder.Body.String())
	require.Contains(t, recorder.Body.String(), "not greater than current slot")
	require.True(t, warned.Load())
	require.True(t, warnedAfterView.Load(), "the slot advance warning must follow the head view")
}

func TestGetDependentRootChecksOutOfWindowSlotOnce(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	h := &ApiHandler{beaconChainCfg: &cfg}
	slot := cfg.SlotsPerEpoch - 1
	headSlot := slot + cfg.SlotsPerHistoricalRoot + 1
	head := state.New(&cfg)
	require.NoError(t, head.SetSlot(headSlot))

	var rootLookups []uint64
	_, err := h.getDependentRoot(func(slot uint64) (common.Hash, error) {
		rootLookups = append(rootLookups, slot)
		return head.GetBlockRootAtSlot(slot)
	}, common.Hash{}, headSlot, 2, true)
	require.Equal(t, http.StatusNotFound, beaconhttp.WrapEndpointError(err).Code)
	require.Equal(t, []uint64{slot}, rootLookups)
}

type proposerDutiesResponse struct {
	Data          []proposerDuties `json:"data"`
	Finalized     bool             `json:"finalized"`
	DependentRoot string           `json:"dependent_root"`
	Version       string           `json:"version"`
}

func TestEpochSlotOverflows(t *testing.T) {
	require.False(t, epochSlotOverflows(0, 32))
	require.False(t, epochSlotOverflows(1000, 32))
	require.False(t, epochSlotOverflows(math.MaxUint64/32-1, 32))
	require.True(t, epochSlotOverflows(math.MaxUint64/32, 32))
	require.True(t, epochSlotOverflows(math.MaxUint64, 32))
	require.False(t, epochSlotOverflows(0, 0))
}

func TestGetDutiesProposerEpochOverflowReturnsBadRequest(t *testing.T) {
	_, _, _, _, _, handler, _, _, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)

	epoch := uint64(math.MaxUint64)
	request := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/eth/v1/validator/duties/proposer/"+strconv.FormatUint(epoch, 10), http.NoBody)
	recorder := httptest.NewRecorder()
	handler.mux.ServeHTTP(recorder, request)
	require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
	require.Contains(t, recorder.Body.String(), "overflows")
}

func TestGetDutiesProposerV2UsesFuluDependentRoot(t *testing.T) {
	_, blocks, _, _, postState, handler, _, syncedData, fcu, _ := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), true)

	headRoot, err := blocks[len(blocks)-1].Block.HashSSZ()
	require.NoError(t, err)

	headEpoch := postState.Slot() / handler.beaconChainCfg.SlotsPerEpoch
	require.Greater(t, headEpoch, uint64(1))
	targetEpoch := headEpoch
	handler.beaconChainCfg.FuluForkEpoch = headEpoch
	handler.beaconChainCfg.InitializeForkSchedule()

	stateCfg := *postState.BeaconConfig()
	stateCfg.FuluForkEpoch = headEpoch
	stateCfg.InitializeForkSchedule()
	encodedPostState, err := postState.EncodeSSZ(nil)
	require.NoError(t, err)
	isolatedPostState := state.New(&stateCfg)
	require.NoError(t, isolatedPostState.DecodeSSZ(encodedPostState, int(clparams.ElectraVersion)))

	fuluHead, err := isolatedPostState.Copy()
	require.NoError(t, err)
	require.NoError(t, fuluHead.UpgradeToFulu())
	require.Equal(t, clparams.FuluVersion, fuluHead.Version())

	fcu.HeadVal = headRoot
	fcu.HeadSlotVal = fuluHead.Slot()
	manager, ok := syncedData.(*synced_data.SyncedDataManager)
	require.True(t, ok)
	require.NoError(t, manager.OnHeadStateWithBlockRoot(fuluHead, headRoot))

	v1 := getProposerDutiesForPath(t, handler, "/eth/v1/validator/duties/proposer/"+strconv.FormatUint(targetEpoch, 10))
	v2 := getProposerDutiesForPath(t, handler, "/eth/v2/validator/duties/proposer/"+strconv.FormatUint(targetEpoch, 10))
	expectedV2Root, err := fuluHead.GetBlockRootAtSlot((targetEpoch-1)*handler.beaconChainCfg.SlotsPerEpoch - 1)
	require.NoError(t, err)

	require.NotEqual(t, v1.DependentRoot, v2.DependentRoot)
	require.Equal(t, expectedV2Root.String(), v2.DependentRoot)
	require.Equal(t, v1.Data, v2.Data)
}

func TestComputeDependentRootSlotNoUnderflow(t *testing.T) {
	slotsPerEpoch := uint64(32)
	tests := []struct {
		name     string
		epoch    uint64
		attester bool
		wantSlot uint64
	}{
		{"proposer_epoch0", 0, false, 0},
		{"attester_epoch0", 0, true, 0},
		{"attester_epoch1", 1, true, 0},
		{"proposer_epoch1", 1, false, 31},
		{"attester_epoch2", 2, true, 31},
		{"proposer_epoch2", 2, false, 63},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			slot := computeDependentRootSlot(tc.epoch, slotsPerEpoch, tc.attester)
			require.Equal(t, tc.wantSlot, slot)
		})
	}
}

func TestGetAttesterDutiesEpochOverflowReturnsBadRequest(t *testing.T) {
	_, _, _, _, _, handler, _, _, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)

	epoch := uint64(math.MaxUint64)
	body := strings.NewReader(`["0"]`)
	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/validator/duties/attester/"+strconv.FormatUint(epoch, 10), body)
	recorder := httptest.NewRecorder()
	handler.mux.ServeHTTP(recorder, request)
	require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
	require.Contains(t, recorder.Body.String(), "overflows")
}

func TestPostPtcDutiesEpochOverflowReturnsBadRequest(t *testing.T) {
	_, _, _, _, _, handler, _, _, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)

	epoch := uint64(math.MaxUint64)
	body := strings.NewReader(`["0"]`)
	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/validator/duties/ptc/"+strconv.FormatUint(epoch, 10), body)
	recorder := httptest.NewRecorder()
	handler.mux.ServeHTTP(recorder, request)
	require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
	require.Contains(t, recorder.Body.String(), "overflows")
}

func getProposerDutiesForEpoch(t *testing.T, handler *ApiHandler, epoch uint64) proposerDutiesResponse {
	t.Helper()

	return getProposerDutiesForPath(t, handler, "/eth/v1/validator/duties/proposer/"+strconv.FormatUint(epoch, 10))
}

func getProposerDutiesForPath(t *testing.T, handler *ApiHandler, path string) proposerDutiesResponse {
	t.Helper()

	request := httptest.NewRequestWithContext(t.Context(), http.MethodGet, path, http.NoBody)
	recorder := httptest.NewRecorder()
	handler.mux.ServeHTTP(recorder, request)
	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())

	var body proposerDutiesResponse
	require.NoError(t, json.NewDecoder(recorder.Body).Decode(&body))
	require.NotEmpty(t, body.Data)
	return body
}
