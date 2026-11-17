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

package cltypes

import (
	"encoding/json"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
)

func TestIndexedAttestationJSONIndexLimit(t *testing.T) {
	cfg := clparams.BeaconChainConfig{MaxValidatorsPerCommittee: 1, MaxCommitteesPerSlot: 2}

	t.Run("at limit", func(t *testing.T) {
		att := NewIndexedAttestationWithConfig(clparams.ElectraVersion, &cfg)
		require.NoError(t, json.Unmarshal([]byte(`{"attesting_indices":["1","2"]}`), att))
		require.Equal(t, 2, att.AttestingIndices.Length())
		require.Equal(t, 2, att.AttestingIndices.Cap())
	})

	t.Run("over limit", func(t *testing.T) {
		att := NewIndexedAttestationWithConfig(clparams.ElectraVersion, &cfg)
		att.AttestingIndices.Append(4)
		require.ErrorContains(t, json.Unmarshal([]byte(`{"attesting_indices":["1","2","3"]}`), att), "attesting indices exceed limit")
		require.Equal(t, 1, att.AttestingIndices.Length())
		require.Equal(t, uint64(4), att.AttestingIndices.Get(0))
	})
}

// An oversized list must be rejected without decoding every index first.
func TestIndexedAttestationJSONStopsAtIndexLimit(t *testing.T) {
	cfg := clparams.BeaconChainConfig{MaxValidatorsPerCommittee: 1, MaxCommitteesPerSlot: 2}
	buf := []byte(`{"attesting_indices":["123"` + strings.Repeat(`,"123"`, 100_000) + `]}`)
	att := NewIndexedAttestationWithConfig(clparams.ElectraVersion, &cfg)
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	err := json.Unmarshal(buf, att)
	runtime.ReadMemStats(&after)
	require.ErrorContains(t, err, "attesting indices exceed limit")
	require.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(3*len(buf)))
}

func TestBeaconBodyJSONUsesElectraIndexedAttestationLimit(t *testing.T) {
	slashing := NewAttesterSlashing(clparams.ElectraVersion)
	for i := range attestingIndicesLimit + 1 {
		slashing.Attestation_1.AttestingIndices.Append(uint64(i))
	}
	data, err := json.Marshal(map[string]any{"attester_slashings": []*AttesterSlashing{slashing}})
	require.NoError(t, err)

	body := BeaconBody{Version: clparams.ElectraVersion, beaconCfg: &clparams.MainnetBeaconConfig}
	require.NoError(t, json.Unmarshal(data, &body))
	indices := body.AttesterSlashings.Get(0).Attestation_1.AttestingIndices
	require.Equal(t, attestingIndicesLimit+1, indices.Length())
	require.Equal(t, attestingIndicesLimitElectra, indices.Cap())
}
