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
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	state_mock "github.com/erigontech/erigon/cl/abstract/mock_services"
	"github.com/erigontech/erigon/cl/aggregation"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/pool"
	"github.com/erigontech/erigon/cl/utils/bls"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
)

const packingCommitteeSize = 8

// packingFixture builds Electra candidates from real BLS signatures: gossip aggregates in the
// operations pool and single attestations aggregated by a real aggregation pool.
type packingFixture struct {
	t       *testing.T
	cfg     *clparams.BeaconChainConfig
	keys    map[int][]*bls.PrivateKey // committee -> members
	data    *solid.AttestationData
	msg     []byte
	handler *ApiHandler
	state   *state_mock.MockBeaconState
	// The state is in epoch 1; votes of each target epoch need the matching justified checkpoint.
	currentJustified, previousJustified solid.Checkpoint
}

func newPackingFixture(t *testing.T) *packingFixture {
	cfg := clparams.MainnetBeaconConfig
	cfg.AltairForkEpoch, cfg.BellatrixForkEpoch, cfg.CapellaForkEpoch, cfg.DenebForkEpoch, cfg.ElectraForkEpoch = 0, 0, 0, 0, 0
	ctrl := gomock.NewController(t)
	clock := eth_clock.NewMockEthereumClock(ctrl)
	clock.EXPECT().GetEpochAtSlot(gomock.Any()).DoAndReturn(func(slot uint64) uint64 { return slot / cfg.SlotsPerEpoch }).AnyTimes()
	clock.EXPECT().StateVersionByEpoch(gomock.Any()).Return(clparams.ElectraVersion).AnyTimes()

	currentJustified := solid.Checkpoint{Epoch: 0, Root: [32]byte{1}}
	previousJustified := solid.Checkpoint{Epoch: 0, Root: [32]byte{8}}
	data := &solid.AttestationData{
		Slot:            cfg.SlotsPerEpoch + 1,
		BeaconBlockRoot: [32]byte{2},
		Source:          currentJustified,
		Target:          solid.Checkpoint{Epoch: 1, Root: [32]byte{3}},
	}
	f := &packingFixture{
		t:                 t,
		cfg:               &cfg,
		keys:              map[int][]*bls.PrivateKey{},
		data:              data,
		msg:               []byte("attestation data signing root"),
		state:             state_mock.NewMockBeaconState(ctrl),
		currentJustified:  currentJustified,
		previousJustified: previousJustified,
	}
	for committee := range 2 {
		for range packingCommitteeSize {
			key, err := bls.GenerateKey()
			require.NoError(t, err)
			f.keys[committee] = append(f.keys[committee], key)
		}
	}
	f.state.EXPECT().Slot().Return(data.Slot + 1).AnyTimes()
	f.state.EXPECT().BeaconConfig().Return(&cfg).AnyTimes()
	f.state.EXPECT().Version().Return(clparams.ElectraVersion).AnyTimes()
	f.state.EXPECT().CommitteeCount(gomock.Any()).Return(uint64(2)).AnyTimes()
	f.state.EXPECT().CurrentJustifiedCheckpoint().Return(currentJustified).AnyTimes()
	f.state.EXPECT().PreviousJustifiedCheckpoint().Return(previousJustified).AnyTimes()
	f.handler = &ApiHandler{
		beaconChainCfg: &cfg,
		operationsPool: pool.NewOperationsPool(&cfg),
		aggregatePool:  aggregation.NewAggregationPool(t.Context(), &cfg, &clparams.NetworkConfig{AttestationPropagationSlotRange: 32}, clock),
	}
	return f
}

// attestation signs data with the given members of committee and aggregates the signatures.
func (f *packingFixture) attestation(data *solid.AttestationData, committee int, members ...int) *solid.Attestation {
	raw := make([]byte, packingCommitteeSize/8+1)
	sigs := make([][]byte, 0, len(members))
	for _, m := range members {
		raw[m/8] |= 1 << (m % 8)
		sigs = append(sigs, f.keys[committee][m].Sign(f.msg).Bytes())
	}
	raw[packingCommitteeSize/8] |= 1 << (packingCommitteeSize % 8)
	sig, err := bls.AggregateSignatures(sigs)
	require.NoError(f.t, err)
	committeeBits := solid.NewBitVector(int(f.cfg.MaxCommitteesPerSlot))
	require.NoError(f.t, committeeBits.SetBitAt(committee, true))
	att := &solid.Attestation{
		AggregationBits: solid.BitlistFromBytes(raw, int(f.cfg.MaxValidatorsPerCommittee*f.cfg.MaxCommitteesPerSlot)),
		Data:            data,
		Signature:       [96]byte(sig),
		CommitteeBits:   committeeBits,
	}
	att.SetVersion(clparams.ElectraVersion)
	return att
}

func (f *packingFixture) gossip(committee int, members ...int) {
	f.gossipData(f.data, committee, members...)
}

func (f *packingFixture) gossipData(data *solid.AttestationData, committee int, members ...int) {
	att := f.attestation(data, committee, members...)
	f.handler.operationsPool.AttestationsPool.Insert(att.Signature, att)
}

func (f *packingFixture) singles(committee int, members ...int) {
	for _, m := range members {
		require.NoError(f.t, f.handler.aggregatePool.AddAttestation(f.attestation(f.data, committee, m)))
	}
}

// packed verifies every merged candidate's signature against the members its bits name, and
// returns, per candidate, the members it covers in each committee.
func (f *packingFixture) packed() []map[int][]int {
	merged, err := f.handler.electraMergedAttestationCandidates(f.state)
	require.NoError(f.t, err)
	var out []map[int][]int
	for _, candidates := range merged {
		for _, att := range candidates {
			covered := map[int][]int{}
			var pubKeys [][]byte
			offset := 0
			for _, committee := range att.CommitteeBits.GetOnIndices() {
				for m := range packingCommitteeSize {
					if att.AggregationBits.GetBitAt(offset + m) {
						covered[committee] = append(covered[committee], m)
						pubKeys = append(pubKeys, bls.CompressPublicKey(f.keys[committee][m].PublicKey()))
					}
				}
				offset += packingCommitteeSize
			}
			valid, err := bls.VerifyAggregate(att.Signature[:], f.msg, pubKeys)
			require.NoError(f.t, err)
			require.True(f.t, valid, "packed signature must match the members its bits name: %v", covered)
			out = append(out, covered)
		}
	}
	return out
}

func TestPackedLocalAggregateAlone(t *testing.T) {
	f := newPackingFixture(t)
	f.singles(0, 1, 2, 5)
	require.Equal(t, []map[int][]int{{0: {1, 2, 5}}}, f.packed())
}

func TestPackedLocalAggregateDisjointFromGossip(t *testing.T) {
	f := newPackingFixture(t)
	f.gossip(0, 0, 1)
	f.singles(0, 4, 6)
	require.Equal(t, []map[int][]int{{0: {0, 1, 4, 6}}}, f.packed())
}

func TestPackedLocalAggregateOverlappingGossip(t *testing.T) {
	f := newPackingFixture(t)
	f.gossip(0, 0, 1, 2)
	f.singles(0, 2, 3, 7)
	require.ElementsMatch(t, []map[int][]int{{0: {0, 1, 2}}, {0: {2, 3, 7}}}, f.packed())
}

func TestPackedLocalAggregatesAcrossCommittees(t *testing.T) {
	f := newPackingFixture(t)
	f.gossip(0, 0, 1)
	f.singles(1, 3, 5)
	require.Equal(t, []map[int][]int{{0: {0, 1}, 1: {3, 5}}}, f.packed())
}

// process_attestation asserts the source; a candidate with another source can never be included.
func TestPackingDropsCandidatesWithForeignSource(t *testing.T) {
	f := newPackingFixture(t)
	foreign := *f.data
	foreign.Source = solid.Checkpoint{Epoch: 0, Root: [32]byte{9}}
	att := f.attestation(&foreign, 0, 4)
	f.handler.operationsPool.AttestationsPool.Insert(att.Signature, att)
	require.NoError(t, f.handler.aggregatePool.AddAttestation(f.attestation(&foreign, 1, 5)))
	f.gossip(0, 0, 1)

	require.Equal(t, []map[int][]int{{0: {0, 1}}}, f.packed())
}

// A vote of the current epoch needs the current justified checkpoint as its source, a vote of the
// previous epoch the previous one.
func TestPackingUsesJustifiedCheckpointOfTargetEpoch(t *testing.T) {
	f := newPackingFixture(t)
	previousEpoch := &solid.AttestationData{
		Slot:            5,
		BeaconBlockRoot: [32]byte{2},
		Source:          f.previousJustified,
		Target:          solid.Checkpoint{Epoch: 0, Root: [32]byte{4}},
	}
	currentWithPreviousSource, previousWithCurrentSource := *f.data, *previousEpoch
	currentWithPreviousSource.Source, previousWithCurrentSource.Source = f.previousJustified, f.currentJustified
	f.gossip(0, 0)
	f.gossipData(&currentWithPreviousSource, 0, 1)
	f.gossipData(previousEpoch, 1, 2)
	f.gossipData(&previousWithCurrentSource, 1, 3)

	require.ElementsMatch(t, []map[int][]int{{0: {0}}, {1: {2}}}, f.packed())
}

func TestDenebPackingDropsCandidatesWithForeignSource(t *testing.T) {
	f := newPackingFixture(t)
	f.cfg.ElectraForkEpoch = 100
	foreign := *f.data
	foreign.Source = solid.Checkpoint{Epoch: 0, Root: [32]byte{9}}
	f.gossipData(&foreign, 0, 4)
	f.gossip(0, 0, 1)

	merged, err := f.handler.denebMergedAttestationCandidates(f.state)
	require.NoError(t, err)
	dataRoot, err := f.data.HashSSZ()
	require.NoError(t, err)
	require.Len(t, merged, 1)
	require.Len(t, merged[dataRoot], 1)
}

func TestPackedLocalAggregateReplacesGossipSubset(t *testing.T) {
	f := newPackingFixture(t)
	f.gossip(0, 1, 2)
	f.singles(0, 1, 2, 3)
	require.Equal(t, []map[int][]int{{0: {1, 2, 3}}}, f.packed())
}

func TestPackedGossipSupersetCoversLocalAggregate(t *testing.T) {
	f := newPackingFixture(t)
	f.gossip(0, 1, 2, 3)
	f.singles(0, 1, 2)
	require.Equal(t, []map[int][]int{{0: {1, 2, 3}}}, f.packed())
}
