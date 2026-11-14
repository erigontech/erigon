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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
)

func TestBlindedBeaconBlockJSONAttesterSlashingLimits(t *testing.T) {
	customCfg := clparams.MainnetBeaconConfig
	customCfg.MaxValidatorsPerCommittee = 2
	customCfg.MaxCommitteesPerSlot = 2
	for _, tc := range []struct {
		name    string
		cfg     *clparams.BeaconChainConfig
		version clparams.StateVersion
		count   int
		limit   int
	}{
		{name: "Deneb", cfg: &clparams.MainnetBeaconConfig, version: clparams.DenebVersion, count: attestingIndicesLimit, limit: attestingIndicesLimit},
		{name: "Electra", cfg: &clparams.MainnetBeaconConfig, version: clparams.ElectraVersion, count: attestingIndicesLimit + 1, limit: attestingIndicesLimitElectra},
		{name: "Fulu", cfg: &clparams.MainnetBeaconConfig, version: clparams.FuluVersion, count: attestingIndicesLimit + 1, limit: attestingIndicesLimitElectra},
		{name: "custom Electra", cfg: &customCfg, version: clparams.ElectraVersion, count: 4, limit: 4},
		{name: "custom Fulu", cfg: &customCfg, version: clparams.FuluVersion, count: 4, limit: 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			block := NewSignedBlindedBeaconBlock(tc.cfg, tc.version)
			block.Block.Slot = 123
			slashing := NewAttesterSlashing(tc.version)
			slashing.SetVersionWithConfig(tc.version, tc.cfg)
			for i := range tc.count {
				slashing.Attestation_1.AttestingIndices.Append(uint64(i))
				slashing.Attestation_2.AttestingIndices.Append(uint64(i))
			}
			block.Block.Body.AttesterSlashings.Append(slashing)
			data, err := json.Marshal(block)
			require.NoError(t, err)

			decoded := NewSignedBlindedBeaconBlock(tc.cfg, tc.version)
			require.NoError(t, json.Unmarshal(data, decoded))
			got := decoded.Block.Body.AttesterSlashings.Get(0)
			for _, att := range []*IndexedAttestation{got.Attestation_1, got.Attestation_2} {
				require.Equal(t, tc.limit, att.AttestingIndices.Cap())
				require.Equal(t, tc.count, att.AttestingIndices.Length())
				require.Equal(t, tc.version, att.version)
			}
			wantRoot, err := block.HashSSZ()
			require.NoError(t, err)
			gotRoot, err := decoded.HashSSZ()
			require.NoError(t, err)
			require.Equal(t, wantRoot, gotRoot)
		})
	}
}

func TestBlindedBeaconBlockJSONRejectsOversizedAttesterSlashings(t *testing.T) {
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
				block := NewSignedBlindedBeaconBlock(&cfg, tc.version)
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

				decoded := NewSignedBlindedBeaconBlock(&cfg, tc.version)
				require.ErrorContains(t, json.Unmarshal(data, decoded), "attesting indices exceed limit")
			})
		}
	}
}

func TestBlindedBeaconBodyJSONRejectsIncompleteAttesterSlashings(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
	}{
		{name: "missing both", body: `{"attester_slashings":[{}]}`},
		{name: "missing first", body: `{"attester_slashings":[{"attestation_2":{}}]}`},
		{name: "missing second", body: `{"attester_slashings":[{"attestation_1":{}}]}`},
		{name: "null first", body: `{"attester_slashings":[{"attestation_1":null,"attestation_2":{}}]}`},
		{name: "null second", body: `{"attester_slashings":[{"attestation_1":{},"attestation_2":null}]}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := NewBlindedBeaconBody(&clparams.MainnetBeaconConfig, clparams.ElectraVersion)
			var err error
			require.NotPanics(t, func() {
				err = json.Unmarshal([]byte(tc.body), body)
			})
			require.ErrorContains(t, err, "attestation_1 and attestation_2 are required")
		})
	}
}

// EncodingSizeSSZ must grow by exactly the encoded delta when a field's
// content grows, for every field gated by a fork version.
func TestBlindedBeaconBodyEncodingSizeTracksEncoding(t *testing.T) {
	for _, v := range []clparams.StateVersion{clparams.DenebVersion, clparams.ElectraVersion} {
		t.Run(v.String(), func(t *testing.T) {
			body := NewBlindedBeaconBody(&clparams.MainnetBeaconConfig, v)
			require.Equal(t, v, body.Version, "constructor must honor the version argument")

			enc0, err := body.EncodeSSZ(nil)
			require.NoError(t, err)
			size0 := body.EncodingSizeSSZ()

			body.BlobKzgCommitments.Append(&KZGCommitment{})
			enc1, err := body.EncodeSSZ(nil)
			require.NoError(t, err)
			size1 := body.EncodingSizeSSZ()
			require.Equal(t, len(enc1)-len(enc0), size1-size0, "BlobKzgCommitments not reflected in EncodingSizeSSZ")

			body.ExecutionChanges.Append(&SignedBLSToExecutionChange{Message: &BLSToExecutionChange{}})
			enc2, err := body.EncodeSSZ(nil)
			require.NoError(t, err)
			size2 := body.EncodingSizeSSZ()
			require.Equal(t, len(enc2)-len(enc1), size2-size1, "ExecutionChanges over-counted in EncodingSizeSSZ")
		})
	}
}
