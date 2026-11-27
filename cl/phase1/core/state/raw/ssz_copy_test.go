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

package raw_test

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/antiquary/tests"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/core/state/raw"
	"github.com/erigontech/erigon/common"
)

func copyTestState(t *testing.T, st *state.CachingBeaconState) *state.CachingBeaconState {
	t.Helper()
	out, err := st.Copy()
	require.NoError(t, err)
	return out
}

// copyTestStates returns one state per fork, reaching forks without a fixture through the upgrade chain.
func copyTestStates(t *testing.T) map[clparams.StateVersion]*state.CachingBeaconState {
	t.Helper()
	_, phase0, _ := tests.GetPhase0Random()
	altair := copyTestState(t, phase0)
	require.NoError(t, altair.UpgradeToAltair())
	bellatrix := copyTestState(t, altair)
	require.NoError(t, bellatrix.UpgradeToBellatrix())
	_, capella, _ := tests.GetCapellaRandom()
	deneb := copyTestState(t, capella)
	require.NoError(t, deneb.UpgradeToDeneb())
	_, electra, _ := tests.GetElectraRandom()
	fulu := copyTestState(t, electra)
	require.NoError(t, fulu.UpgradeToFulu())
	gloas := copyTestState(t, fulu)
	require.NoError(t, gloas.UpgradeToGloas())
	return map[clparams.StateVersion]*state.CachingBeaconState{
		clparams.Phase0Version:    phase0,
		clparams.AltairVersion:    altair,
		clparams.BellatrixVersion: bellatrix,
		clparams.CapellaVersion:   capella,
		clparams.DenebVersion:     deneb,
		clparams.ElectraVersion:   electra,
		clparams.FuluVersion:      fulu,
		clparams.GloasVersion:     gloas,
	}
}

func pendingAttestation(seed byte) *solid.PendingAttestation {
	bits := solid.NewBitList(0, 2048)
	bits.Append(seed)
	bits.Append(seed + 1)
	data := &solid.AttestationData{Slot: uint64(seed), Target: solid.Checkpoint{Epoch: uint64(seed)}}
	return &solid.PendingAttestation{AggregationBits: bits, Data: data, InclusionDelay: uint64(seed), ProposerIndex: uint64(seed)}
}

// populateDynamicLists makes every dynamic list of the state's schema non-empty, with different content per element.
func populateDynamicLists(t *testing.T, st *raw.BeaconState) {
	t.Helper()
	version := st.Version()
	for i := byte(1); i <= 3; i++ {
		st.AddHistoricalRoot(common.Hash{i, 0xa0})
		require.NoError(t, st.AddEth1DataVote(&cltypes.Eth1Data{DepositCount: uint64(i), BlockHash: common.Hash{i, 0xa1}}))
		if version == clparams.Phase0Version {
			st.AddPreviousEpochAttestation(pendingAttestation(i))
			st.AddCurrentEpochAtteastation(pendingAttestation(i + 10))
		}
		if version >= clparams.CapellaVersion {
			st.AddHistoricalSummary(&cltypes.HistoricalSummary{BlockSummaryRoot: common.Hash{i, 0xa2}, StateSummaryRoot: common.Hash{i, 0xa3}})
		}
		if version >= clparams.ElectraVersion {
			st.AppendPendingDeposit(&solid.PendingDeposit{Amount: uint64(i), Slot: uint64(i)})
			st.AppendPendingPartialWithdrawal(&solid.PendingPartialWithdrawal{ValidatorIndex: uint64(i), Amount: uint64(i)})
			st.AppendPendingConsolidation(&solid.PendingConsolidation{SourceIndex: uint64(i), TargetIndex: uint64(i) + 1})
		}
		if version >= clparams.GloasVersion {
			st.GetBuilders().Append(&cltypes.Builder{Balance: uint64(i), DepositEpoch: uint64(i)})
			st.GetBuilderPendingWithdrawals().Append(&cltypes.BuilderPendingWithdrawal{Amount: uint64(i), BuilderIndex: uint64(i)})
			st.GetPayloadExpectedWithdrawals().Append(&cltypes.Withdrawal{Index: uint64(i), Amount: uint64(i)})
		}
	}
	if version >= clparams.BellatrixVersion && version < clparams.GloasVersion {
		st.LatestExecutionPayloadHeader().Extra.SetBytes([]byte{0xe1, 0xe2, 0xe3})
	}
	if version >= clparams.GloasVersion {
		commitments := &st.GetLatestExecutionPayloadBid().BlobKzgCommitments
		for i := byte(1); i <= 3; i++ {
			commitments.Append(&cltypes.KZGCommitment{i, 0xb0})
		}
	}
	if version >= clparams.AltairVersion {
		require.NotZero(t, st.PreviousEpochParticipation().Length())
		require.NotZero(t, st.CurrentEpochParticipation().Length())
		require.NotZero(t, st.InactivityScores().Length())
	}
}

// TestDecodeSSZCopiesEveryDynamicField overwrites the input after decoding: a field that aliases it, or a scratch
// slice that the streaming decoder reuses, would change the state.
func TestDecodeSSZCopiesEveryDynamicField(t *testing.T) {
	for version, source := range copyTestStates(t) {
		t.Run(version.String(), func(t *testing.T) {
			populateDynamicLists(t, source.BeaconState)
			want, err := source.EncodeSSZ(nil)
			require.NoError(t, err)
			wantRoot, err := source.HashSSZ()
			require.NoError(t, err)

			decoders := map[string]func(buf []byte, dst *raw.BeaconState) error{
				"DecodeSSZ": func(buf []byte, dst *raw.BeaconState) error { return dst.DecodeSSZ(buf, int(version)) },
				"DecodeCanonicalSSZFrom": func(buf []byte, dst *raw.BeaconState) error {
					return dst.DecodeCanonicalSSZFrom(bytes.NewReader(buf), len(buf), int(version))
				},
			}
			for name, decode := range decoders {
				t.Run(name, func(t *testing.T) {
					buf := bytes.Clone(want)
					decoded := raw.New(&clparams.MainnetBeaconConfig)
					require.NoError(t, decode(buf, decoded))
					for i := range buf {
						buf[i] = 0xff
					}
					got, err := decoded.EncodeSSZ(nil)
					require.NoError(t, err)
					require.True(t, bytes.Equal(want, got), "decoded state changed after the input was overwritten")
					gotRoot, err := decoded.HashSSZ()
					require.NoError(t, err)
					require.Equal(t, wantRoot, gotRoot)
				})
			}
		})
	}
}
