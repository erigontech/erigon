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

package raw

import (
	"encoding/binary"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
)

func layoutTestState(t *testing.T, version clparams.StateVersion, count int) *BeaconState {
	t.Helper()
	state := newTestState(clparams.GloasVersion)
	state.SetVersion(version)
	for i := range count {
		validator := solid.NewValidator()
		binary.LittleEndian.PutUint64(validator.PublicKeyBytes(), uint64(i)+1)
		validator.SetEffectiveBalance(uint64(i) * 1_000)
		require.NoError(t, state.AddValidator(validator, 32_000_000_000))
	}
	return state
}

func mutateLayoutTestState(t *testing.T, state *BeaconState) {
	t.Helper()
	if n := state.ValidatorLength(); n > 0 {
		require.NoError(t, state.SetEffectiveBalanceForValidatorAtIndex(n/2, 7))
		require.NoError(t, state.SetValidatorSlashed(n-1, true))
	}
	validator := solid.NewValidator()
	validator.SetEffectiveBalance(99)
	require.NoError(t, state.AddValidator(validator, 1))
}

var layoutTestCounts = []int{0, 1, 84, 85, 86, 255, 256, 257, 341, 700}

func TestSetVersionAcrossGloasKeepsValidatorRoots(t *testing.T) {
	for _, tc := range []struct {
		name     string
		from, to clparams.StateVersion
	}{
		{"fulu to gloas", clparams.FuluVersion, clparams.GloasVersion},
		{"gloas to fulu", clparams.GloasVersion, clparams.FuluVersion},
	} {
		for _, count := range layoutTestCounts {
			t.Run(fmt.Sprintf("%s/%d", tc.name, count), func(t *testing.T) {
				state := layoutTestState(t, tc.from, count)
				requireStateRootMatchesEncoding(t, state)

				state.SetVersion(tc.to)
				requireStateRootMatchesEncoding(t, state)

				mutateLayoutTestState(t, state)
				requireStateRootMatchesEncoding(t, state)

				state.SetVersion(tc.from)
				requireStateRootMatchesEncoding(t, state)
			})
		}
	}
}

func TestDirectGloasDecodeHashesValidators(t *testing.T) {
	for _, count := range layoutTestCounts {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			source := layoutTestState(t, clparams.GloasVersion, count)
			want, err := source.HashSSZ()
			require.NoError(t, err)
			encoded, err := source.EncodeSSZ(nil)
			require.NoError(t, err)

			decoded := New(source.BeaconConfig())
			require.NoError(t, decoded.DecodeSSZ(encoded, int(clparams.GloasVersion)))
			got, err := decoded.HashSSZ()
			require.NoError(t, err)
			require.Equal(t, want, got)
		})
	}
}

func TestCopyIntoReusedSpareAcrossLayouts(t *testing.T) {
	for _, tc := range []struct {
		name             string
		source, spare    clparams.StateVersion
		sourceN, spareN  int
		hashSourceBefore bool
	}{
		{"fulu source into gloas spare", clparams.FuluVersion, clparams.GloasVersion, 700, 300, true},
		{"gloas source into fulu spare", clparams.GloasVersion, clparams.FuluVersion, 700, 300, true},
		{"fulu source into longer gloas spare", clparams.FuluVersion, clparams.GloasVersion, 90, 700, false},
		{"gloas source into longer fulu spare", clparams.GloasVersion, clparams.FuluVersion, 90, 700, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := layoutTestState(t, tc.source, tc.sourceN)
			spare := layoutTestState(t, tc.spare, tc.spareN)
			if tc.hashSourceBefore {
				_, err := source.HashSSZ()
				require.NoError(t, err)
			}
			_, err := spare.HashSSZ()
			require.NoError(t, err)

			require.NoError(t, source.CopyInto(spare))
			requireStateRootMatchesEncoding(t, spare)
			requireStateRootMatchesEncoding(t, source)

			mutateLayoutTestState(t, spare)
			requireStateRootMatchesEncoding(t, spare)
			requireStateRootMatchesEncoding(t, source)

			mutateLayoutTestState(t, source)
			requireStateRootMatchesEncoding(t, source)
			requireStateRootMatchesEncoding(t, spare)
		})
	}
}

func TestInstallingValidatorSetNormalizesLayout(t *testing.T) {
	for _, version := range []clparams.StateVersion{clparams.FuluVersion, clparams.GloasVersion} {
		for _, install := range []struct {
			name string
			fn   func(*BeaconState, *solid.ValidatorSet)
		}{
			{"SetValidators", (*BeaconState).SetValidators},
			{"SetValidatorSet", (*BeaconState).SetValidatorSet},
		} {
			for _, wrongLayout := range []bool{false, true} {
				t.Run(fmt.Sprintf("%d/%s/progressive=%v", version, install.name, wrongLayout), func(t *testing.T) {
					state := layoutTestState(t, version, 0)
					set := solid.NewValidatorSet(int(state.BeaconConfig().ValidatorRegistryLimit))
					set.SetProgressiveHashing(wrongLayout)
					for i := range 300 {
						validator := solid.NewValidator()
						validator.SetEffectiveBalance(uint64(i))
						set.Append(validator)
					}
					install.fn(state, set)
					requireStateRootMatchesEncoding(t, state)
				})
			}
		}
	}
}

func TestSiblingStatesHashAndMutateConcurrently(t *testing.T) {
	for _, version := range []clparams.StateVersion{clparams.FuluVersion, clparams.GloasVersion} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			source := layoutTestState(t, version, 5_000)
			_, err := source.HashSSZ()
			require.NoError(t, err)
			siblings := make([]*BeaconState, 6)
			for i := range siblings {
				siblings[i] = New(source.BeaconConfig())
			}
			var wg sync.WaitGroup
			for i, sibling := range siblings {
				wg.Go(func() {
					if err := source.CopyInto(sibling); err != nil {
						t.Error(err)
						return
					}
					if i%2 == 0 {
						if _, err := sibling.HashSSZ(); err != nil {
							t.Error(err)
						}
						return
					}
					for j := range 50 {
						_ = sibling.SetEffectiveBalanceForValidatorAtIndex((i*997+j*31)%5_000, uint64(j))
					}
				})
			}
			wg.Wait()
			requireStateRootMatchesEncoding(t, source)
			for _, sibling := range siblings {
				requireStateRootMatchesEncoding(t, sibling)
			}
		})
	}
}
