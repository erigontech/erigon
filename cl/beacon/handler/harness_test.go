// Copyright 2024 The Erigon Authors
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
	_ "embed"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/beacon/beacontest"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/persistence/base_encoding"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
)

func TestHarnessPhase0(t *testing.T) {
	beacontest.Execute(
		append(
			defaultHarnessOpts(harnessConfig{t: t, v: clparams.Phase0Version}),
			beacontest.WithTestFromFs(Harnesses, "blocks"),
			beacontest.WithTestFromFs(Harnesses, "config"),
			beacontest.WithTestFromFs(Harnesses, "headers"),
			beacontest.WithTestFromFs(Harnesses, "committees"),
			beacontest.WithTestFromFs(Harnesses, "duties_attester"),
			beacontest.WithTestFromFs(Harnesses, "duties_proposer"),
		)...,
	)
}

func TestHarnessPhase0Finalized(t *testing.T) {
	beacontest.Execute(
		append(
			defaultHarnessOpts(harnessConfig{t: t, v: clparams.Phase0Version, finalized: true}),
			beacontest.WithTestFromFs(Harnesses, "liveness"),
			beacontest.WithTestFromFs(Harnesses, "duties_attester_f"),
			beacontest.WithTestFromFs(Harnesses, "committees_f"),
			beacontest.WithTestFromFs(Harnesses, "attestation_rewards_phase0_inclusion"),
		)...,
	)
}

func TestHarnessBellatrix(t *testing.T) {
	beacontest.Execute(
		append(
			defaultHarnessOpts(harnessConfig{t: t, v: clparams.BellatrixVersion, finalized: true}),
			beacontest.WithTestFromFs(Harnesses, "attestation_rewards_bellatrix"),
			beacontest.WithTestFromFs(Harnesses, "duties_sync_bellatrix"),
			beacontest.WithTestFromFs(Harnesses, "lightclient"),
			beacontest.WithTestFromFs(Harnesses, "validators"),
			beacontest.WithTestFromFs(Harnesses, "lighthouse"),
			beacontest.WithTestFromFs(Harnesses, "blob_sidecars"),
		)...,
	)
}

func TestHarnessCapella(t *testing.T) {
	beacontest.Execute(
		append(
			defaultHarnessOpts(harnessConfig{t: t, v: clparams.CapellaVersion, finalized: true}),
			beacontest.WithTestFromFs(Harnesses, "expected_withdrawals"),
		)...,
	)
}

func TestHarnessForkChoice(t *testing.T) {
	beacontest.Execute(
		append(
			defaultHarnessOpts(harnessConfig{t: t, v: clparams.BellatrixVersion, forkmode: 1}),
			beacontest.WithTestFromFs(Harnesses, "fork_choice"),
		)...,
	)
}

func TestAttestationRewardsAltairRulesOmitInclusionDelay(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		epoch                string
		stateEpoch           uint64
		version              clparams.StateVersion
		seedCalculationEpoch bool
	}{
		{name: "last phase0 requested epoch", epoch: "4", stateEpoch: 4, version: clparams.Phase0Version},
		{name: "first Altair requested epoch", epoch: "5", stateEpoch: 5, version: clparams.BellatrixVersion, seedCalculationEpoch: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, blocks, _, _, _, h, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.New(), true)
			h.beaconChainCfg.AltairForkEpoch = 5
			h.beaconChainCfg.BellatrixForkEpoch = 5
			if tc.seedCalculationEpoch {
				blockRoot, err := blocks[len(blocks)-1].Block.HashSSZ()
				require.NoError(t, err)
				slot := 6 * h.beaconChainCfg.SlotsPerEpoch
				tx, err := db.BeginRw(t.Context())
				require.NoError(t, err)
				defer tx.Rollback()
				epochData, err := tx.GetOne(kv.EpochData, base_encoding.Encode64ToBytes4(slot-h.beaconChainCfg.SlotsPerEpoch))
				require.NoError(t, err)
				require.NotEmpty(t, epochData)
				require.NoError(t, tx.Put(kv.EpochData, base_encoding.Encode64ToBytes4(slot), epochData))
				require.NoError(t, beacon_indicies.MarkRootCanonical(tx, slot, blockRoot))
				require.NoError(t, tx.Commit())
			}
			fcu.HeadVal, _ = blocks[len(blocks)-1].Block.HashSSZ()
			fcu.HeadSlotVal = blocks[len(blocks)-1].Block.Slot
			if headSlot := (tc.stateEpoch+2)*h.beaconChainCfg.SlotsPerEpoch - 1; fcu.HeadSlotVal < headSlot {
				fcu.HeadSlotVal = headSlot
			}
			fcu.FinalizedSlotVal = math.MaxUint64
			require.Equal(t, tc.version, h.beaconChainCfg.GetCurrentStateVersion(tc.stateEpoch))

			r := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/beacon/rewards/attestations/"+tc.epoch, nil)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			require.Equal(t, http.StatusOK, w.Code, w.Body.String())
			var response struct {
				Data map[string][]map[string]string `json:"data"`
			}
			require.NoError(t, json.Unmarshal(w.Body.Bytes(), &response))
			for _, key := range []string{"ideal_rewards", "total_rewards"} {
				require.NotEmpty(t, response.Data[key])
				for i, reward := range response.Data[key] {
					require.NotContains(t, reward, "inclusion_delay", "%s[%d]: Altair rules must omit inclusion_delay; got %v", key, i, reward)
				}
			}
		})
	}
}
