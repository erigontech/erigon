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

package state

import (
	"reflect"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
)

const shareTestValidators = 1_500

func viewOf(ref *CachingBeaconState) func(func(*CachingBeaconState)) {
	return func(use func(*CachingBeaconState)) { use(ref) }
}

func noReference(func(*CachingBeaconState)) {}

func decodeRaw(t testing.TB, cfg *clparams.BeaconChainConfig, encoded []byte, version clparams.StateVersion) *CachingBeaconState {
	t.Helper()
	decoded := New(cfg)
	require.NoError(t, decoded.BeaconState.DecodeSSZ(encoded, int(version)))
	return decoded
}

func decodePlain(t testing.TB, cfg *clparams.BeaconChainConfig, encoded []byte, version clparams.StateVersion) *CachingBeaconState {
	t.Helper()
	decoded := New(cfg)
	require.NoError(t, decoded.DecodeSSZ(encoded, int(version)))
	return decoded
}

func touchValidators(t testing.TB, st *CachingBeaconState, indexes ...int) {
	t.Helper()
	for _, i := range indexes {
		require.NoError(t, st.SetEffectiveBalanceForValidatorAtIndex(i, 31_000_000_000+uint64(i)))
	}
}

func appendValidators(t testing.TB, st *CachingBeaconState, from, count uint64) {
	t.Helper()
	for i := from; i < from+count; i++ {
		require.NoError(t, st.AddValidator(activeValidatorWithIndex(i), 32_000_000_000))
	}
}

func mustCopy(t testing.TB, st *CachingBeaconState) *CachingBeaconState {
	t.Helper()
	copied, err := st.Copy()
	require.NoError(t, err)
	return copied
}

func requireSameLookups(t testing.TB, want, got *CachingBeaconState, probes ...*CachingBeaconState) {
	t.Helper()
	keys := [][48]byte{activeValidatorWithIndex(1 << 40).PublicKey()}
	for _, st := range append([]*CachingBeaconState{want, got}, probes...) {
		for _, validator := range st.ValidatorSet().All(0, st.ValidatorLength()) {
			keys = append(keys, validator.PublicKey())
		}
	}
	for _, key := range keys {
		wantIndex, wantOK := want.ValidatorIndexByPubkey(key)
		gotIndex, gotOK := got.ValidatorIndexByPubkey(key)
		require.Equal(t, wantOK, gotOK)
		require.Equal(t, wantIndex, gotIndex)
	}
}

func requireSameState(t testing.TB, want, got *CachingBeaconState, probes ...*CachingBeaconState) {
	t.Helper()
	wantRoot, err := want.HashSSZ()
	require.NoError(t, err)
	gotRoot, err := got.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, wantRoot, gotRoot)
	require.Equal(t, want.GetTotalActiveBalance(), got.GetTotalActiveBalance())
	requireSameLookups(t, want, got, probes...)
}

type shareScenario struct {
	name      string
	decoded   *CachingBeaconState
	ref       *CachingBeaconState
	baseShare bool
}

func shareScenarios(t *testing.T, version clparams.StateVersion) []shareScenario {
	base := benchmarkBeaconStateWithSize(t, version, shareTestValidators)
	last := base.ValidatorLength() - 1
	var scenarios []shareScenario

	scenarios = append(scenarios, shareScenario{name: "same state", decoded: base, ref: mustCopy(t, base), baseShare: true})

	ahead := mustCopy(t, base)
	touchValidators(t, ahead, 0, 300, 1300, last)
	appendValidators(t, ahead, 50_000, 16)
	scenarios = append(scenarios, shareScenario{name: "reference ahead", decoded: base, ref: ahead, baseShare: true})

	longer := mustCopy(t, base)
	appendValidators(t, longer, 60_000, 16)
	scenarios = append(scenarios, shareScenario{name: "decoded longer than reference", decoded: longer, ref: mustCopy(t, base), baseShare: true})

	fork := mustCopy(t, base)
	fork.SetValidatorAtIndex(last, activeValidatorWithIndex(70_000))
	scenarios = append(scenarios, shareScenario{name: "fork replaces the last validator", decoded: base, ref: fork})

	forkLonger := mustCopy(t, fork)
	appendValidators(t, forkLonger, 71_000, 5)
	scenarios = append(scenarios, shareScenario{name: "longer fork replaces the last validator", decoded: base, ref: forkLonger})

	forkMiddle := mustCopy(t, base)
	forkMiddle.SetValidatorAtIndex(1300, activeValidatorWithIndex(72_000))
	scenarios = append(scenarios, shareScenario{name: "fork replaces a middle validator", decoded: base, ref: forkMiddle})

	extended := mustCopy(t, base)
	appendValidators(t, extended, 80_000, 20)
	extendedEncoded, err := extended.EncodeSSZ(nil)
	require.NoError(t, err)
	hidden := decodePlain(t, base.BeaconConfig(), extendedEncoded, version)
	scenarios = append(scenarios, shareScenario{name: "reference base longer than decoded", decoded: base, ref: hidden, baseShare: true})

	return scenarios
}

func TestInitBeaconStateSharingMatchesDecodeSSZ(t *testing.T) {
	for _, version := range benchmarkVersions {
		for _, scenario := range shareScenarios(t, version.version) {
			t.Run(version.name+"/"+scenario.name, func(t *testing.T) {
				encoded, err := scenario.decoded.EncodeSSZ(nil)
				require.NoError(t, err)
				cfg := scenario.decoded.BeaconConfig()
				refRoot, err := scenario.ref.HashSSZ()
				require.NoError(t, err)
				refEncoded, err := scenario.ref.EncodeSSZ(nil)
				require.NoError(t, err)
				plain := decodePlain(t, cfg, encoded, version.version)
				shared := decodeRaw(t, cfg, encoded, version.version)

				chunks, err := shared.InitBeaconStateSharing(viewOf(scenario.ref))

				require.NoError(t, err)
				require.Positive(t, chunks)
				require.Equal(t, scenario.baseShare, shared.publicKeyIndices.base == scenario.ref.publicKeyIndices.base)
				requireSameState(t, plain, shared, scenario.ref)

				appendValidators(t, plain, 90_000, 3)
				appendValidators(t, shared, 90_000, 3)
				touchValidators(t, plain, 5, 1300)
				touchValidators(t, shared, 5, 1300)
				requireSameState(t, plain, shared, scenario.ref)

				gotRefRoot, err := scenario.ref.HashSSZ()
				require.NoError(t, err)
				require.Equal(t, refRoot, gotRefRoot, "the reference changed")
				gotRefEncoded, err := scenario.ref.EncodeSSZ(nil)
				require.NoError(t, err)
				require.Equal(t, refEncoded, gotRefEncoded, "the reference changed")
			})
		}
	}
}

func TestInitBeaconStateSharingWithoutReferenceMatchesDecodeSSZ(t *testing.T) {
	for _, version := range benchmarkVersions {
		t.Run(version.name, func(t *testing.T) {
			base := benchmarkBeaconStateWithSize(t, version.version, shareTestValidators)
			encoded, err := base.EncodeSSZ(nil)
			require.NoError(t, err)
			plain := decodePlain(t, base.BeaconConfig(), encoded, version.version)
			shared := decodeRaw(t, base.BeaconConfig(), encoded, version.version)

			chunks, err := shared.InitBeaconStateSharing(noReference)

			require.NoError(t, err)
			require.Zero(t, chunks)
			requireSameState(t, plain, shared)
			appendValidators(t, plain, 90_000, 3)
			appendValidators(t, shared, 90_000, 3)
			requireSameState(t, plain, shared)
		})
	}
}

func TestInitBeaconStateSharingConcurrentWithReferenceUse(t *testing.T) {
	for _, version := range benchmarkVersions {
		t.Run(version.name, func(t *testing.T) {
			base := benchmarkBeaconStateWithSize(t, version.version, shareTestValidators)
			encoded, err := base.EncodeSSZ(nil)
			require.NoError(t, err)
			cfg := base.BeaconConfig()
			wantRoot, err := decodePlain(t, cfg, encoded, version.version).HashSSZ()
			require.NoError(t, err)

			ref := mustCopy(t, base)
			var refMu sync.RWMutex
			view := func(use func(*CachingBeaconState)) {
				refMu.RLock()
				defer refMu.RUnlock()
				use(ref)
			}
			const rounds = 6
			var wg sync.WaitGroup
			for range 2 {
				wg.Go(func() {
					for round := range rounds {
						decoded := decodeRaw(t, cfg, encoded, version.version)
						if _, err := decoded.InitBeaconStateSharing(view); err != nil {
							t.Error(err)
							return
						}
						root, err := decoded.HashSSZ()
						if err != nil || root != wantRoot {
							t.Errorf("round %d: root %x, err %v, want %x", round, root, err, wantRoot)
							return
						}
						if err := decoded.SetEffectiveBalanceForValidatorAtIndex(round*100, 1); err != nil {
							t.Error(err)
							return
						}
						if _, err := decoded.HashSSZ(); err != nil {
							t.Error(err)
							return
						}
					}
				})
				wg.Go(func() {
					for round := range rounds {
						refMu.RLock()
						copied, err := ref.Copy()
						refMu.RUnlock()
						if err != nil {
							t.Error(err)
							return
						}
						if err := copied.SetEffectiveBalanceForValidatorAtIndex(round*50, 2); err != nil {
							t.Error(err)
							return
						}
						if _, err := copied.HashSSZ(); err != nil {
							t.Error(err)
							return
						}
					}
				})
			}
			wg.Go(func() {
				for range rounds {
					refMu.RLock()
					_, err := ref.HashSSZ()
					refMu.RUnlock()
					if err != nil {
						t.Error(err)
						return
					}
				}
			})
			wg.Go(func() {
				for round := range rounds {
					refMu.Lock()
					err := ref.SetEffectiveBalanceForValidatorAtIndex(round*70, 3)
					if err == nil {
						err = ref.AddValidator(activeValidatorWithIndex(uint64(100_000+round)), 32_000_000_000)
					}
					refMu.Unlock()
					if err != nil {
						t.Error(err)
						return
					}
				}
			})
			wg.Wait()
			if t.Failed() {
				return
			}
			decoded := decodeRaw(t, cfg, encoded, version.version)
			_, err = decoded.InitBeaconStateSharing(view)
			require.NoError(t, err)
			root, err := decoded.HashSSZ()
			require.NoError(t, err)
			require.Equal(t, wantRoot, root)
		})
	}
}

func validatorDataPointer(set *solid.ValidatorSet, i int) uintptr {
	return reflect.ValueOf(set.Get(i)).Field(0).Pointer()
}

func countAliasedValidators(a, b *solid.ValidatorSet) int {
	aliased := 0
	for i := range min(a.Length(), b.Length()) {
		if validatorDataPointer(a, i) == validatorDataPointer(b, i) {
			aliased++
		}
	}
	return aliased
}

func TestInitBeaconStateSharingDoesNoSharingWorkInsideTheView(t *testing.T) {
	for _, version := range benchmarkVersions {
		t.Run(version.name, func(t *testing.T) {
			base := benchmarkBeaconStateWithSize(t, version.version, shareTestValidators)
			encoded, err := base.EncodeSSZ(nil)
			require.NoError(t, err)
			ref := mustCopy(t, base)
			touchValidators(t, ref, 0)
			shared := decodeRaw(t, base.BeaconConfig(), encoded, version.version)

			inView := false
			view := func(use func(*CachingBeaconState)) {
				use(ref)
				inView = true
				require.NotSame(t, ref.publicKeyIndices.base, shared.publicKeyIndices.base)
				require.Zero(t, shared.publicKeyIndices.limit)
				require.Nil(t, shared.publicKeyIndices.delta)
				require.Zero(t, countAliasedValidators(shared.ValidatorSet(), ref.ValidatorSet()))
			}
			chunks, err := shared.InitBeaconStateSharing(view)

			require.NoError(t, err)
			require.True(t, inView)
			require.Positive(t, chunks)
			aliased := countAliasedValidators(shared.ValidatorSet(), ref.ValidatorSet())
			require.Positive(t, aliased)
			require.Less(t, aliased, shared.ValidatorLength())
			require.Same(t, ref.publicKeyIndices.base, shared.publicKeyIndices.base)
		})
	}
}

func TestInitBeaconStateSharingSurvivesTheReferenceChangingAfterTheCallback(t *testing.T) {
	for _, version := range benchmarkVersions {
		t.Run(version.name, func(t *testing.T) {
			base := benchmarkBeaconStateWithSize(t, version.version, shareTestValidators)
			encoded, err := base.EncodeSSZ(nil)
			require.NoError(t, err)
			cfg := base.BeaconConfig()
			plain := decodePlain(t, cfg, encoded, version.version)
			shared := decodeRaw(t, cfg, encoded, version.version)
			ref := mustCopy(t, base)
			unrelated := mustCopy(t, base)
			for i := range unrelated.ValidatorLength() {
				unrelated.SetValidatorAtIndex(i, activeValidatorWithIndex(uint64(200_000+i)))
			}
			appendValidators(t, unrelated, 300_000, 8)

			view := func(use func(*CachingBeaconState)) {
				use(ref)
				require.NoError(t, unrelated.CopyInto(ref))
				appendValidators(t, ref, 310_000, 8)
			}
			chunks, err := shared.InitBeaconStateSharing(view)

			require.NoError(t, err)
			require.Positive(t, chunks)
			requireSameState(t, plain, shared, unrelated)
		})
	}
}
