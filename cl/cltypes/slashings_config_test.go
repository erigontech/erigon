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
	"fmt"
	"testing"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/common/ssz"

	"github.com/stretchr/testify/require"
)

func configuredSlashing(version clparams.StateVersion, cfg *clparams.BeaconChainConfig, count int) *AttesterSlashing {
	a := NewAttesterSlashing(version)
	for _, att := range []*IndexedAttestation{a.Attestation_1, a.Attestation_2} {
		att.SetVersionWithConfig(version, cfg)
		for i := 0; i < count; i++ {
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
								if count > limit {
									require.Error(t, err)
									require.Error(t, bodyErr)
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
								if version < clparams.GloasVersion && version >= clparams.BellatrixVersion {
									blinded, err := body.Blinded()
									require.NoError(t, err)
									encoded, err := blinded.EncodeSSZ(nil)
									require.NoError(t, err)
									target := NewBlindedBeaconBody(bodyCfg, version)
									require.NoError(t, target.DecodeSSZ(encoded, int(version)))
									root, err := target.AttesterSlashings.Get(0).HashSSZ()
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

func TestAttesterSlashingConfiguredStrictOffsets(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.MaxValidatorsPerCommittee, cfg.MaxCommitteesPerSlot = 8, 4
	version := clparams.ElectraVersion
	for _, offset := range []int{0, 8} { // slashing container and first indexed attestation
		t.Run(fmt.Sprint(offset), func(t *testing.T) {
			a := configuredSlashing(version, &cfg, 1)
			encoded, err := a.EncodeSSZ(nil)
			require.NoError(t, err)
			binary.LittleEndian.PutUint32(encoded[offset:], uint32(len(encoded)+1))
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
			require.ErrorIs(t, NewBeaconBody(&cfg, version).DecodeSSZStrict(bodyBytes, int(version)), ssz.ErrBadOffset)
		})
	}
}
