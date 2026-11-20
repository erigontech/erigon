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
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/common/ssz"

	"github.com/stretchr/testify/require"
)

func configuredSlashing(version clparams.StateVersion, cfg *clparams.BeaconChainConfig, count int) *AttesterSlashing {
	a := NewAttesterSlashing(version)
	for _, att := range []*IndexedAttestation{a.Attestation_1, a.Attestation_2} {
		att.SetVersionWithConfig(version, cfg)
		for i := range count {
			att.AttestingIndices.Append(uint64(i))
		}
	}
	return a
}

func TestAttesterSlashingConfiguredLimits(t *testing.T) {
	mainnet := clparams.MainnetBeaconConfig
	minimal := mainnet
	clparams.ApplyMinimalPreset(&minimal)
	small, largePhase0, largeElectra := mainnet, mainnet, mainnet
	small.MaxValidatorsPerCommittee, small.MaxCommitteesPerSlot = 8, 4
	largePhase0.MaxValidatorsPerCommittee = 4096
	largeElectra.MaxCommitteesPerSlot = 128
	for _, tc := range []struct {
		name string
		cfg  *clparams.BeaconChainConfig
	}{
		{"default", nil}, {"mainnet", &mainnet}, {"minimal", &minimal}, {"small", &small}, {"large_phase0", &largePhase0}, {"large_electra", &largeElectra},
	} {
		for _, version := range []clparams.StateVersion{clparams.Phase0Version, clparams.ElectraVersion, clparams.GloasVersion} {
			t.Run(fmt.Sprintf("%s/%d", tc.name, version), func(t *testing.T) {
				bodyCfg := tc.cfg
				if bodyCfg == nil {
					bodyCfg = &mainnet
				} // Execution payload decoding requires a chain config.
				limit := 2048
				if tc.cfg != nil {
					limit = int(tc.cfg.MaxValidatorsPerCommittee)
				}
				if version >= clparams.ElectraVersion {
					committees := 64
					if tc.cfg != nil {
						committees = int(tc.cfg.MaxCommitteesPerSlot)
					}
					limit *= committees
				}
				require.Equal(t, limit, NewIndexedAttestationWithConfig(version, tc.cfg).AttestingIndices.Cap())
				for _, count := range []int{0, limit, limit + 1} {
					t.Run(fmt.Sprint(count), func(t *testing.T) {
						a := configuredSlashing(version, tc.cfg, count)
						if count > limit {
							a.Attestation_1.AttestingIndices.Clear()
						} // Verify the second nested list is also checked.
						encoded, err := a.EncodeSSZ(nil)
						require.NoError(t, err)
						body := NewBeaconBody(bodyCfg, version)
						body.AttesterSlashings.Append(a)
						bodyBytes, err := body.EncodeSSZ(nil)
						require.NoError(t, err)
						for _, strict := range []bool{false, true} {
							t.Run(fmt.Sprint(strict), func(t *testing.T) {
								decoded := new(AttesterSlashing)
								decode := decoded.DecodeSSZWithConfig
								if strict {
									decode = decoded.DecodeSSZStrictWithConfig
								}
								err := decode(encoded, int(version), tc.cfg)
								decodedBody := NewBeaconBody(bodyCfg, version)
								decodeBody := decodedBody.DecodeSSZ
								if strict {
									decodeBody = decodedBody.DecodeSSZStrict
								}
								bodyErr := decodeBody(bodyBytes, int(version))
								var blindedBody *BlindedBeaconBody
								var blindedErr error
								if version < clparams.GloasVersion && version >= clparams.BellatrixVersion {
									blinded, err := body.Blinded()
									require.NoError(t, err)
									blindedBytes, err := blinded.EncodeSSZ(nil)
									require.NoError(t, err)
									blindedBody = NewBlindedBeaconBody(bodyCfg, version)
									blindedErr = blindedBody.DecodeSSZ(blindedBytes, int(version))
								}
								if count > limit {
									require.Error(t, err)
									require.Error(t, bodyErr)
									if blindedBody != nil {
										require.Error(t, blindedErr)
									}
									return
								}
								require.NoError(t, err)
								require.NoError(t, bodyErr)
								expectedRoot, err := a.HashSSZ()
								require.NoError(t, err)
								for _, result := range []*AttesterSlashing{decoded, decodedBody.AttesterSlashings.Get(0)} {
									require.Equal(t, limit, result.Attestation_1.AttestingIndices.Cap())
									require.Equal(t, limit, result.Attestation_2.AttestingIndices.Cap())
									require.Equal(t, count, result.Attestation_1.AttestingIndices.Length())
									require.Equal(t, count, result.Attestation_2.AttestingIndices.Length())
									root, err := result.HashSSZ()
									require.NoError(t, err)
									require.Equal(t, expectedRoot, root)
								}
								expectedBodyRoot, err := body.HashSSZ()
								require.NoError(t, err)
								root, err := decodedBody.HashSSZ()
								require.NoError(t, err)
								require.Equal(t, expectedBodyRoot, root)
								if blindedBody != nil {
									require.NoError(t, blindedErr)
									root, err := blindedBody.AttesterSlashings.Get(0).HashSSZ()
									require.NoError(t, err)
									require.Equal(t, expectedRoot, root)
								}
							})
						}
					})
				}
			})
		}
	}
}

// The blinded-block API decodes JSON and then calls SetVersion; the slashings must hash with the configured cap, as
// after SSZ decoding.
func TestBlindedBodyJSONUsesConfiguredSlashingLimit(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.MaxValidatorsPerCommittee = 4096
	version := clparams.BellatrixVersion
	body := NewBeaconBody(&cfg, version)
	body.AttesterSlashings.Append(configuredSlashing(version, &cfg, 1))
	blinded, err := body.Blinded()
	require.NoError(t, err)
	blinded.ExecutionPayload.Extra = solid.NewExtraData()
	want, err := blinded.HashSSZ()
	require.NoError(t, err)
	encoded, err := json.Marshal(blinded)
	require.NoError(t, err)

	decoded := NewBlindedBeaconBody(&cfg, version)
	decoded.ExecutionPayload = nil
	require.NoError(t, json.Unmarshal(encoded, decoded))
	decoded.SetVersion(version)
	root, err := decoded.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, want, root)
}

func TestBlindedBodySetVersionInvalidatesSlashingRoot(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.MaxValidatorsPerCommittee = 4096
	version := clparams.BellatrixVersion
	body := NewBlindedBeaconBody(&cfg, version)
	body.ExecutionPayload.Extra = solid.NewExtraData()
	body.AttesterSlashings.Append(configuredSlashing(version, nil, 1))
	before, err := body.HashSSZ()
	require.NoError(t, err)

	body.SetVersion(version)
	encoded, err := body.EncodeSSZ(nil)
	require.NoError(t, err)
	decoded := NewBlindedBeaconBody(&cfg, version)
	require.NoError(t, decoded.DecodeSSZ(encoded, int(version)))
	want, err := decoded.HashSSZ()
	require.NoError(t, err)
	require.NotEqual(t, before, want)
	root, err := body.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, want, root)
}

func TestAttesterSlashingConfiguredStrictOffsets(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.MaxValidatorsPerCommittee, cfg.MaxCommitteesPerSlot = 8, 4
	version := clparams.ElectraVersion
	for _, offset := range []int{0, 8} { // slashing container and first indexed attestation
		t.Run(fmt.Sprint(offset), func(t *testing.T) {
			a := configuredSlashing(version, &cfg, 1)
			encoded, err := a.EncodeSSZ(nil)
			require.NoError(t, err)
			const gap = 8
			if offset == 0 {
				first := int(binary.LittleEndian.Uint32(encoded))
				second := int(binary.LittleEndian.Uint32(encoded[4:]))
				copy(encoded[first+gap:second], encoded[first:second-gap])
			}
			binary.LittleEndian.PutUint32(encoded[offset:], binary.LittleEndian.Uint32(encoded[offset:])+gap)
			require.NoError(t, new(AttesterSlashing).DecodeSSZWithConfig(encoded, int(version), &cfg))
			require.ErrorIs(t, new(AttesterSlashing).DecodeSSZStrictWithConfig(encoded, int(version), &cfg), ssz.ErrBadOffset)
			body := NewBeaconBody(&cfg, version)
			body.AttesterSlashings.Append(a)
			bodyBytes, err := body.EncodeSSZ(nil)
			require.NoError(t, err)
			original, err := a.EncodeSSZ(nil)
			require.NoError(t, err)
			pos := bytes.Index(bodyBytes, original)
			require.Greater(t, pos, 0)
			copy(bodyBytes[pos:], encoded)
			require.NoError(t, NewBeaconBody(&cfg, version).DecodeSSZ(bodyBytes, int(version)))
			require.ErrorIs(t, NewBeaconBody(&cfg, version).DecodeSSZStrict(bodyBytes, int(version)), ssz.ErrBadOffset)
		})
	}
}
