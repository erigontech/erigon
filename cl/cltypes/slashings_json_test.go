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
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
)

var indexedAttestationJSON = `{"attesting_indices":["1"],"data":{"slot":"1","index":"0",` +
	`"beacon_block_root":"0x0000000000000000000000000000000000000000000000000000000000000001",` +
	`"source":{"epoch":"0","root":"0x0000000000000000000000000000000000000000000000000000000000000000"},` +
	`"target":{"epoch":"0","root":"0x0000000000000000000000000000000000000000000000000000000000000000"}},` +
	`"signature":"0x` + strings.Repeat("0", 192) + `"}`

func TestBeaconBodyJSONRejectsIncompleteAttesterSlashing(t *testing.T) {
	for _, tc := range []struct {
		name  string
		input string
	}{
		{name: "missing attestation_1", input: `{"attestation_2":` + indexedAttestationJSON + `}`},
		{name: "missing attestation_2", input: `{"attestation_1":` + indexedAttestationJSON + `}`},
		{name: "null attestation_1", input: `{"attestation_1":null,"attestation_2":` + indexedAttestationJSON + `}`},
		{name: "null attestation_2", input: `{"attestation_1":` + indexedAttestationJSON + `,"attestation_2":null}`},
		{name: "both missing", input: `{}`},
		{name: "both null", input: `{"attestation_1":null,"attestation_2":null}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := NewBeaconBody(&clparams.MainnetBeaconConfig, clparams.FuluVersion)

			require.Error(t, json.Unmarshal([]byte(`{"attester_slashings":[`+tc.input+`]}`), body))
		})
	}
}

func TestBeaconBodyJSONKeepsValidAttesterSlashingForkLimits(t *testing.T) {
	for _, tc := range []struct {
		version clparams.StateVersion
		limit   int
	}{
		{version: clparams.DenebVersion, limit: attestingIndicesLimit},
		{version: clparams.ElectraVersion, limit: attestingIndicesLimitElectra},
		{version: clparams.FuluVersion, limit: attestingIndicesLimitElectra},
	} {
		t.Run(clparams.ClVersionToString(tc.version), func(t *testing.T) {
			body := NewBeaconBody(&clparams.MainnetBeaconConfig, tc.version)
			input := `{"attester_slashings":[{"attestation_1":` + indexedAttestationJSON +
				`,"attestation_2":` + indexedAttestationJSON + `}]}`

			require.NoError(t, json.Unmarshal([]byte(input), body))
			require.Equal(t, 1, body.AttesterSlashings.Len())

			slashing := body.AttesterSlashings.Get(0)
			require.Equal(t, 1, slashing.Attestation_1.AttestingIndices.Length())
			require.Equal(t, 1, slashing.Attestation_2.AttestingIndices.Length())
			require.Equal(t, tc.limit, slashing.Attestation_1.AttestingIndices.Cap())
			require.Equal(t, tc.limit, slashing.Attestation_2.AttestingIndices.Cap())
		})
	}
}

func TestIndexedAttestationJSONRejectsNullData(t *testing.T) {
	att := NewIndexedAttestation(clparams.FuluVersion)

	require.ErrorContains(t, json.Unmarshal([]byte(`{"data":null}`), att), "indexed attestation data is null")
}

func TestProposerSlashingJSONRejectsIncompleteHeaders(t *testing.T) {
	zeroRoot := `"0x` + strings.Repeat("0", 64) + `"`
	signature := `"signature":"0x` + strings.Repeat("0", 192) + `"`
	header := `{"message":{"slot":"1","proposer_index":"0","parent_root":` + zeroRoot +
		`,"state_root":` + zeroRoot + `,"body_root":` + zeroRoot + `},` + signature + `}`
	for _, tc := range []struct {
		name  string
		input string
	}{
		{name: "missing signed_header_1", input: `{"signed_header_2":` + header + `}`},
		{name: "missing signed_header_2", input: `{"signed_header_1":` + header + `}`},
		{name: "null signed_header_1", input: `{"signed_header_1":null,"signed_header_2":` + header + `}`},
		{name: "null signed_header_2", input: `{"signed_header_1":` + header + `,"signed_header_2":null}`},
		{name: "missing message", input: `{"signed_header_1":{` + signature + `},"signed_header_2":` + header + `}`},
		{name: "null message", input: `{"signed_header_1":` + header + `,"signed_header_2":{"message":null,` + signature + `}}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var slashing ProposerSlashing

			require.Error(t, json.Unmarshal([]byte(tc.input), &slashing))
		})
	}
}

func TestBeaconBodyJSONRejectsOversizedAttesterSlashings(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.MaxValidatorsPerCommittee = 2
	cfg.MaxCommitteesPerSlot = 2
	for _, tc := range []struct {
		version clparams.StateVersion
		count   int
	}{
		{version: clparams.DenebVersion, count: attestingIndicesLimit + 1},
		{version: clparams.ElectraVersion, count: 5},
		{version: clparams.FuluVersion, count: 5},
	} {
		for _, field := range []string{"attestation_1", "attestation_2"} {
			t.Run(tc.version.String()+"/"+field, func(t *testing.T) {
				block := NewSignedBeaconBlock(&cfg, tc.version)
				slashing := NewAttesterSlashing(tc.version)
				att := slashing.Attestation_1
				if field == "attestation_2" {
					att = slashing.Attestation_2
				}
				for i := range tc.count {
					att.AttestingIndices.Append(uint64(i))
				}
				block.Block.Body.AttesterSlashings.Append(slashing)
				data, err := json.Marshal(block)
				require.NoError(t, err)

				decoded := NewSignedBeaconBlock(&cfg, tc.version)
				require.ErrorContains(t, json.Unmarshal(data, decoded), "attesting indices exceed limit")
			})
		}
	}
}
