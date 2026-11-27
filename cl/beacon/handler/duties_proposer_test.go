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
	"context"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/clparams/initial_state"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/persistence/base_encoding"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	state_accessors "github.com/erigontech/erigon/cl/persistence/state"
	"github.com/erigontech/erigon/cl/persistence/state/historical_states_reader"
	"github.com/erigontech/erigon/cl/phase1/core/state"
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

func TestGetDutiesProposerHistoricalFuluForkEpochUsesRecomputed(t *testing.T) {
	db, _, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	cfg := handler.beaconChainCfg
	epoch := uint64(3)
	slot := epoch * cfg.SlotsPerEpoch
	fcu.FinalizedSlotVal = slot

	var archivedState *state.CachingBeaconState
	require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
		var err error
		archivedState, err = handler.stateReader.ReadHistoricalState(t.Context(), tx, slot)
		return err
	}))
	require.NotNil(t, archivedState)
	cfg.CapellaForkEpoch, cfg.DenebForkEpoch, cfg.ElectraForkEpoch, cfg.FuluForkEpoch = epoch, epoch, epoch, epoch
	cfg.InitializeForkSchedule()
	archivedState.SetVersion(clparams.ElectraVersion)
	var encoded bytes.Buffer
	require.NoError(t, state_accessors.EpochDataFromBeaconState(archivedState).Serialize(&encoded))
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		return tx.Put(kv.EpochData, base_encoding.Encode64ToBytes4(slot), encoded.Bytes())
	}))
	require.NoError(t, archivedState.UpgradeToFulu())
	expected := firstEpochProposers(archivedState)
	recomputed, err := archivedState.GetBeaconProposerIndices(epoch)
	require.NoError(t, err)
	require.Equal(t, recomputed, expected)

	response := getProposerDutiesForEpoch(t, handler, epoch)
	require.Equal(t, clparams.FuluVersion.String(), response.Version)
	requireProposerDuties(t, response.Data, archivedState, slot, expected)
}

func TestGetDutiesProposerHistoricalFuluAndGloasForkEpochUsesRecomputed(t *testing.T) {
	db, _, _, preState, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	cfg := handler.beaconChainCfg
	epoch := uint64(3)
	slot := epoch * cfg.SlotsPerEpoch
	fcu.FinalizedSlotVal = slot

	var archivedState *state.CachingBeaconState
	require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
		var err error
		archivedState, err = handler.stateReader.ReadHistoricalState(t.Context(), tx, slot)
		return err
	}))
	require.NotNil(t, archivedState)
	cfg.CapellaForkEpoch, cfg.DenebForkEpoch, cfg.ElectraForkEpoch, cfg.FuluForkEpoch, cfg.GloasForkEpoch = epoch, epoch, epoch, epoch, epoch
	cfg.InitializeForkSchedule()
	archivedState.SetVersion(clparams.FuluVersion)
	recomputed, err := archivedState.GetBeaconProposerIndices(epoch)
	require.NoError(t, err)
	slashed := recomputed[0]
	require.True(t, archivedState.ValidatorSet().Get(int(slashed)).Active(epoch))
	require.NoError(t, archivedState.SetValidatorSlashed(int(slashed), true))
	archivedState.SetVersion(clparams.ElectraVersion)
	var encoded bytes.Buffer
	require.NoError(t, state_accessors.EpochDataFromBeaconState(archivedState).Serialize(&encoded))
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		return tx.Put(kv.EpochData, base_encoding.Encode64ToBytes4(slot), encoded.Bytes())
	}))
	validators := state_accessors.NewStaticValidatorTable()
	archivedState.ValidatorSet().Range(func(index int, validator solid.Validator, _ int) bool {
		require.NoError(t, validators.AddValidator(validator, uint64(index), 0))
		return true
	})
	handler.stateReader = historical_states_reader.NewHistoricalStatesReader(cfg, nil, validators, preState, nil, handler.syncedData)
	require.NoError(t, archivedState.UpgradeToFulu())
	expected := firstEpochProposers(archivedState)
	require.Equal(t, slashed, expected[0])
	require.NoError(t, archivedState.UpgradeToGloas())
	require.Equal(t, expected, firstEpochProposers(archivedState))
	gloasRecomputed, err := archivedState.GetBeaconProposerIndices(epoch)
	require.NoError(t, err)
	require.NotEqual(t, expected, gloasRecomputed)

	response := getProposerDutiesForEpoch(t, handler, epoch)
	require.Equal(t, clparams.GloasVersion.String(), response.Version)
	requireProposerDuties(t, response.Data, archivedState, slot, expected)
}

func TestGetDutiesProposerHistoricalFuluMissingEpochData(t *testing.T) {
	db, _, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	cfg := handler.beaconChainCfg
	epoch := uint64(3)
	slot := epoch * cfg.SlotsPerEpoch
	fcu.FinalizedSlotVal = slot
	cfg.CapellaForkEpoch, cfg.DenebForkEpoch, cfg.ElectraForkEpoch, cfg.FuluForkEpoch = epoch-1, epoch-1, epoch-1, epoch-1
	cfg.InitializeForkSchedule()
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		return tx.Delete(kv.EpochData, base_encoding.Encode64ToBytes4(slot))
	}))
	path := "/eth/v1/validator/duties/proposer/" + strconv.FormatUint(epoch, 10)
	recorder := httptest.NewRecorder()
	handler.mux.ServeHTTP(recorder, httptest.NewRequestWithContext(t.Context(), http.MethodGet, path, http.NoBody))
	require.Equal(t, http.StatusNotFound, recorder.Code, recorder.Body.String())
	require.Contains(t, recorder.Body.String(), "epoch data not found")
}

func TestGetDutiesProposerHistoricalFuluUsesArchivedLookahead(t *testing.T) {
	db, _, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	cfg := handler.beaconChainCfg
	epoch := uint64(3)
	slot := epoch * cfg.SlotsPerEpoch
	fcu.FinalizedSlotVal = slot

	var archivedState *state.CachingBeaconState
	require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
		var err error
		archivedState, err = handler.stateReader.ReadHistoricalState(t.Context(), tx, slot)
		return err
	}))
	require.NotNil(t, archivedState)

	cfg.CapellaForkEpoch, cfg.DenebForkEpoch, cfg.ElectraForkEpoch, cfg.FuluForkEpoch = epoch-1, epoch-1, epoch-1, epoch-1
	cfg.InitializeForkSchedule()
	archivedState.SetVersion(clparams.FuluVersion)
	recomputed, err := archivedState.GetBeaconProposerIndices(epoch)
	require.NoError(t, err)
	active := archivedState.GetActiveValidatorsIndices(epoch)
	require.Greater(t, len(active), 1)
	require.NoError(t, archivedState.SetEffectiveBalanceForValidatorAtIndex(int(active[0]), cfg.MaxEffectiveBalanceElectra))
	require.NoError(t, archivedState.UpgradeToFulu())
	expected := firstEpochProposers(archivedState)
	require.NotEqual(t, recomputed, expected)

	var encoded bytes.Buffer
	require.NoError(t, state_accessors.EpochDataFromBeaconState(archivedState).Serialize(&encoded))
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		if err := tx.Put(kv.EpochData, base_encoding.Encode64ToBytes4(slot), encoded.Bytes()); err != nil {
			return err
		}
		return tx.Delete(kv.SlotData, base_encoding.Encode64ToBytes4(slot))
	}))

	response := getProposerDutiesForEpoch(t, handler, epoch)
	require.True(t, response.Finalized)
	require.Equal(t, clparams.FuluVersion.String(), response.Version)
	requireProposerDuties(t, response.Data, archivedState, slot, expected)
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

func TestGetDutiesProposerRecentFuluUsesEpochLookahead(t *testing.T) {
	_, _, _, _, postState, handler, _, manager, fcu, _ := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), true)
	cfg := handler.beaconChainCfg
	epoch := state.Epoch(postState)
	slot := epoch * cfg.SlotsPerEpoch
	cfg.FuluForkEpoch = epoch
	cfg.InitializeForkSchedule()

	epochState, err := handler.copyHeadStateForDuties(postState)
	require.NoError(t, err)
	target := increaseProposerTestBalance(t, epochState)
	require.NoError(t, epochState.UpgradeToFulu())
	expected := firstEpochProposers(epochState)
	balanceBefore, err := epochState.ValidatorEffectiveBalance(int(target))
	require.NoError(t, err)

	head, err := epochState.Copy()
	require.NoError(t, err)
	require.NoError(t, transition.DefaultMachine.ProcessSlots(head, slot+cfg.SlotsPerEpoch))
	balanceAfter, err := head.ValidatorEffectiveBalance(int(target))
	require.NoError(t, err)
	require.Greater(t, balanceAfter, balanceBefore)
	recomputed, err := head.GetBeaconProposerIndices(epoch)
	require.NoError(t, err)
	require.NotEqual(t, recomputed, expected)

	epochRoot, err := head.GetBlockRootAtSlot(head.Slot() - 1)
	require.NoError(t, err)
	require.NotEmpty(t, epochRoot)
	fcu.StateAtBlockRootVal[epochRoot] = epochState
	fcu.FinalizedSlotVal = slot - 1
	fcu.LowestAvailableSlotVal = &slot
	require.NoError(t, manager.OnHeadStateWithBlockRoot(head, common.Hash{1}))

	response := getProposerDutiesForEpoch(t, handler, epoch)
	require.False(t, response.Finalized)
	require.Equal(t, clparams.FuluVersion.String(), response.Version)
	requireProposerDuties(t, response.Data, epochState, slot, expected)
}

func TestGetDutiesProposerRecentPreFuluUsesEpochState(t *testing.T) {
	for _, lastBlockInEpoch := range []bool{true, false} {
		t.Run(strconv.FormatBool(lastBlockInEpoch), func(t *testing.T) {
			_, _, _, _, postState, handler, _, manager, fcu, _ := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), true)
			cfg := handler.beaconChainCfg
			epoch := state.Epoch(postState) + 1
			slot := epoch * cfg.SlotsPerEpoch

			previousState, err := handler.copyHeadStateForDuties(postState)
			require.NoError(t, err)
			increaseProposerTestBalance(t, previousState)
			active := previousState.GetActiveValidatorsIndices(epoch)
			require.Greater(t, len(active), 3)
			source, target := active[2], active[3]
			require.NoError(t, previousState.SetExitEpochForValidatorAtIndex(int(source), epoch))
			require.NoError(t, previousState.SetWithdrawableEpochForValidatorAtIndex(int(source), epoch+1))
			for _, index := range []uint64{source, target} {
				require.NoError(t, previousState.SetWithdrawalCredentialForValidatorAtIndex(int(index), common.Hash{byte(cfg.CompoundingWithdrawalPrefix)}))
			}
			require.NoError(t, previousState.SetValidatorBalance(int(source), cfg.MaxEffectiveBalanceElectra))
			require.NoError(t, previousState.SetEffectiveBalanceForValidatorAtIndex(int(source), cfg.MaxEffectiveBalanceElectra))
			previousState.AppendPendingConsolidation(&solid.PendingConsolidation{SourceIndex: source, TargetIndex: target})

			epochState, err := previousState.Copy()
			require.NoError(t, err)
			require.NoError(t, transition.DefaultMachine.ProcessSlots(epochState, slot))
			expected, err := epochState.GetBeaconProposerIndices(epoch)
			require.NoError(t, err)
			if !lastBlockInEpoch {
				previousProposers, err := previousState.GetBeaconProposerIndices(epoch)
				require.NoError(t, err)
				require.NotEqual(t, expected, previousProposers)
			}

			blockState := previousState
			if lastBlockInEpoch {
				blockState = epochState
			}
			header := blockState.LatestBlockHeader()
			header.Slot = blockState.Slot()
			header.Root = common.Hash{}
			blockState.SetLatestBlockHeader(&header)
			head, err := blockState.Copy()
			require.NoError(t, err)
			require.NoError(t, transition.DefaultMachine.ProcessSlots(head, slot+cfg.SlotsPerEpoch))
			balanceBefore, err := epochState.ValidatorEffectiveBalance(int(target))
			require.NoError(t, err)
			balanceAfter, err := head.ValidatorEffectiveBalance(int(target))
			require.NoError(t, err)
			require.Greater(t, balanceAfter, balanceBefore)
			recomputed, err := head.GetBeaconProposerIndices(epoch)
			require.NoError(t, err)
			require.NotEqual(t, expected, recomputed)

			epochRoot, err := head.GetBlockRootAtSlot(slot + cfg.SlotsPerEpoch - 1)
			require.NoError(t, err)
			require.NotEmpty(t, epochRoot)
			fcu.StateAtBlockRootVal[epochRoot] = blockState
			fcu.FinalizedSlotVal = slot - 1
			fcu.LowestAvailableSlotVal = &slot
			require.NoError(t, manager.OnHeadStateWithBlockRoot(head, common.Hash{1}))

			for _, version := range []string{"v1", "v2"} {
				t.Run(version, func(t *testing.T) {
					path := "/eth/" + version + "/validator/duties/proposer/" + strconv.FormatUint(epoch, 10)
					response := getProposerDutiesForPath(t, handler, path)
					require.False(t, response.Finalized)
					requireProposerDuties(t, response.Data, epochState, slot, expected)
				})
			}
		})
	}
}

func TestGetDutiesProposerAdvancesHeadCopyAcrossTargetFork(t *testing.T) {
	_, _, _, _, postState, handler, _, manager, _, _ := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), true)

	cfg := handler.beaconChainCfg
	headEpoch := state.Epoch(postState)
	targetEpoch := headEpoch + 1
	cfg.FuluForkEpoch = targetEpoch
	cfg.InitializeForkSchedule()

	head, err := handler.copyHeadStateForDuties(postState)
	require.NoError(t, err)
	require.Equal(t, clparams.ElectraVersion, head.Version())
	target := increaseProposerTestBalance(t, head)
	balanceBefore, err := head.ValidatorEffectiveBalance(int(target))
	require.NoError(t, err)
	recomputed, err := head.GetBeaconProposerIndices(targetEpoch)
	require.NoError(t, err)
	require.NoError(t, manager.OnHeadStateWithBlockRoot(head, manager.HeadRoot()))

	expectedState, err := head.Copy()
	require.NoError(t, err)
	expectedSlot := targetEpoch * cfg.SlotsPerEpoch
	require.NoError(t, transition.DefaultMachine.ProcessSlots(expectedState, expectedSlot))
	require.Equal(t, clparams.FuluVersion, expectedState.Version())
	balanceAfter, err := expectedState.ValidatorEffectiveBalance(int(target))
	require.NoError(t, err)
	require.Greater(t, balanceAfter, balanceBefore)
	expected := firstEpochProposers(expectedState)
	require.NotEqual(t, recomputed, expected)

	response := getProposerDutiesForEpoch(t, handler, targetEpoch)
	require.False(t, response.Finalized)
	require.Equal(t, clparams.FuluVersion.String(), response.Version)
	require.NoError(t, manager.ViewHeadState(func(headState *state.CachingBeaconState) error {
		require.Equal(t, clparams.ElectraVersion, headState.Version())
		return nil
	}))
	requireProposerDuties(t, response.Data, expectedState, expectedSlot, expected)
	path := "/eth/v1/validator/duties/proposer/" + strconv.FormatUint(headEpoch+3, 10)
	recorder := httptest.NewRecorder()
	handler.mux.ServeHTTP(recorder, httptest.NewRequestWithContext(t.Context(), http.MethodGet, path, http.NoBody))
	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
}

func TestGetDutiesProposerAdvancesFuluHeadCopyAcrossTargetFork(t *testing.T) {
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
	expectedProposers := firstEpochProposers(expectedState)

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
	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
}

func increaseProposerTestBalance(t *testing.T, s *state.CachingBeaconState) uint64 {
	t.Helper()
	cfg := s.BeaconConfig()
	active := s.GetActiveValidatorsIndices(state.Epoch(s) + 1)
	require.Greater(t, len(active), 1)
	target := active[1]
	require.NoError(t, s.SetWithdrawalCredentialForValidatorAtIndex(int(target), common.Hash{byte(cfg.CompoundingWithdrawalPrefix)}))
	require.NoError(t, s.SetValidatorBalance(int(target), cfg.MaxEffectiveBalanceElectra))
	return target
}

func firstEpochProposers(s *state.CachingBeaconState) []uint64 {
	proposers := make([]uint64, s.BeaconConfig().SlotsPerEpoch)
	lookahead := s.GetProposerLookahead()
	for i := range proposers {
		proposers[i] = lookahead.Get(i)
	}
	return proposers
}

func requireProposerDuties(t *testing.T, duties []proposerDuties, s *state.CachingBeaconState, startSlot uint64, expected []uint64) {
	t.Helper()
	require.Len(t, duties, len(expected))
	for i, duty := range duties {
		slot := startSlot + uint64(i)
		require.Equal(t, slot, duty.Slot)
		require.Equal(t, expected[i], duty.ValidatorIndex, "slot %d", slot)
		pubkey, err := s.ValidatorPublicKey(int(expected[i]))
		require.NoError(t, err)
		require.Equal(t, pubkey, duty.Pubkey, "slot %d", slot)
	}
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
	activeIndices := fuluHead.GetActiveValidatorsIndices(targetEpoch)
	require.Greater(t, len(activeIndices), 2)
	source, target := activeIndices[0], activeIndices[1]
	require.NoError(t, fuluHead.SetExitEpochForValidatorAtIndex(int(source), headEpoch))
	require.NoError(t, fuluHead.SetWithdrawableEpochForValidatorAtIndex(int(source), targetEpoch))
	for _, index := range []uint64{source, target} {
		require.NoError(t, fuluHead.SetWithdrawalCredentialForValidatorAtIndex(int(index), common.Hash{byte(cfg.CompoundingWithdrawalPrefix)}))
	}
	require.NoError(t, fuluHead.SetValidatorBalance(int(source), cfg.MaxEffectiveBalanceElectra))
	require.NoError(t, fuluHead.SetEffectiveBalanceForValidatorAtIndex(int(source), cfg.MaxEffectiveBalanceElectra))
	fuluHead.AppendPendingConsolidation(&solid.PendingConsolidation{SourceIndex: source, TargetIndex: target})
	require.NoError(t, fuluHead.UpgradeToFulu())
	require.NoError(t, manager.OnHeadStateWithBlockRoot(fuluHead, headRoot))

	t.Run("beyond_seed", func(t *testing.T) {
		for _, offset := range []uint64{3, 4} {
			epoch := headEpoch + offset
			t.Run(strconv.FormatUint(epoch, 10), func(t *testing.T) {
				setDutiesClockEpoch(handler, epoch)
				targetSlot := epoch * cfg.SlotsPerEpoch
				advancedState, err := fuluHead.Copy()
				require.NoError(t, err)
				require.NoError(t, transition.DefaultMachine.ProcessSlots(advancedState, targetSlot))
				expected := firstEpochProposers(advancedState)
				for _, version := range []string{"v1", "v2"} {
					t.Run(version, func(t *testing.T) {
						path := "/eth/" + version + "/validator/duties/proposer/" + strconv.FormatUint(epoch, 10)
						response := getProposerDutiesForPath(t, handler, path)
						require.Len(t, response.Data, len(expected))
						for i, duty := range response.Data {
							require.Equal(t, targetSlot+uint64(i), duty.Slot)
							require.Equal(t, expected[i], duty.ValidatorIndex)
							pubkey, err := advancedState.ValidatorPublicKey(int(expected[i]))
							require.NoError(t, err)
							require.Equal(t, pubkey, duty.Pubkey)
						}
					})
				}
			})
		}
	})
	t.Run("at_seed_boundary", func(t *testing.T) {
		getProposerDutiesForEpoch(t, handler, targetEpoch)
	})
	t.Run("advance_uses_state_epoch", func(t *testing.T) {
		targetSlot := targetEpoch * cfg.SlotsPerEpoch
		handler.syncedData = proposerHeadSlotOverride{SyncedData: manager, slot: targetSlot}

		advancedState, err := fuluHead.Copy()
		require.NoError(t, err)
		require.NoError(t, transition.DefaultMachine.ProcessSlots(advancedState, targetSlot-cfg.SlotsPerEpoch))
		balanceBefore, err := advancedState.ValidatorEffectiveBalance(int(target))
		require.NoError(t, err)
		require.NoError(t, transition.DefaultMachine.ProcessSlots(advancedState, targetSlot))
		balanceAfter, err := advancedState.ValidatorEffectiveBalance(int(target))
		require.NoError(t, err)
		require.Greater(t, balanceAfter, balanceBefore)
		recomputed, err := advancedState.GetBeaconProposerIndices(targetEpoch)
		require.NoError(t, err)
		expected := firstEpochProposers(advancedState)
		require.NotEqual(t, expected, recomputed)
		response := getProposerDutiesForEpoch(t, handler, targetEpoch)
		requireProposerDuties(t, response.Data, advancedState, targetSlot, expected)
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
	expectedSlot := targetEpoch * handler.beaconChainCfg.SlotsPerEpoch
	require.NoError(t, expectedState.SetSlot(expectedSlot+handler.beaconChainCfg.SlotsPerEpoch-1))
	epochRoot, err := gloasHead.GetBlockRootAtSlot(expectedState.Slot())
	require.NoError(t, err)
	fcu.StateAtBlockRootVal[epochRoot] = expectedState
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

// Processing the empty slots after the head would carry the head root into their block roots.
func TestGetDependentRootUsesHeadRootForSlotsAfterHead(t *testing.T) {
	_, _, _, _, _, handler, _, syncedData, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)

	headEpoch := syncedData.HeadSlot() / handler.beaconChainCfg.SlotsPerEpoch
	for _, attester := range []bool{false, true} {
		root, err := handler.getDependentRoot(headEpoch+2, attester)
		require.NoError(t, err)
		require.Equal(t, syncedData.HeadRoot(), root)
	}
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
