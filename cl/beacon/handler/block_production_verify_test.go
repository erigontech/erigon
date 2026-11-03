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

	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/fork"
	"github.com/erigontech/erigon/cl/utils/bls"
)

// After a reorg that changes the shuffling, the proposal state maps an aggregate's bits to other
// validators than the ones that signed it. Packing it would make the block invalid, so production
// verifies the selected attestations against the proposal state and drops the ones that fail.
func TestFindBestAttestationsDropsAttestationsThatFailAgainstTheProposalState(t *testing.T) {
	f := newVerifyingPackingFixture(t)
	otherHead := *f.data
	otherHead.BeaconBlockRoot = [32]byte{4}

	f.signedGossip(f.data, 0, 0, 1, 2)
	f.signedGossip(&otherHead, 1, 0, 1, 2)
	packed := f.handler.findBestAttestationsForBlockProduction(f.state)

	require.Equal(t, 1, packed.Len())
	require.Equal(t, []int{0}, packed.Get(0).CommitteeBits.GetOnIndices())
	require.Equal(t, f.data.BeaconBlockRoot, packed.Get(0).Data.BeaconBlockRoot)
}

// Aggregates of one data from several committees are merged into one attestation, so a committee
// that fails takes the merged attestation with it. The block stays valid.
func TestFindBestAttestationsDropsAMergedAttestationWithAFailingCommittee(t *testing.T) {
	f := newVerifyingPackingFixture(t)

	f.signedGossip(f.data, 0, 0, 1, 2)
	f.signedGossip(f.data, 1, 0, 1, 2)
	packed := f.handler.findBestAttestationsForBlockProduction(f.state)

	require.Zero(t, packed.Len())
}

// Every attestation that the proposal state maps to its signers is kept.
func TestFindBestAttestationsKeepsAttestationsThatVerify(t *testing.T) {
	f := newVerifyingPackingFixture(t)

	f.signedGossip(f.data, 0, 0, 1, 2)
	f.signedGossip(f.data, 0, 3, 4)
	packed := f.handler.findBestAttestationsForBlockProduction(f.state)

	require.Equal(t, 1, packed.Len(), "the two disjoint aggregates are merged")
	require.Equal(t, []int{0}, packed.Get(0).CommitteeBits.GetOnIndices())
}

type verifyingPackingFixture struct {
	*packingFixture
	domain []byte
}

// newVerifyingPackingFixture backs the packing fixture's mocked proposal state with real signing
// roots and validator keys. Committee 1's members are rotated in that state, so its bits name
// other validators than the ones that signed, as after a reorg that changes the shuffling.
func newVerifyingPackingFixture(t *testing.T) *verifyingPackingFixture {
	f := &verifyingPackingFixture{packingFixture: newPackingFixture(t), domain: make([]byte, 32)}
	f.domain[0] = 0x01
	validatorOf := func(committee, member int) uint64 {
		if committee == 1 {
			member = (member + 1) % packingCommitteeSize
		}
		return uint64(committee*packingCommitteeSize + member)
	}
	f.state.EXPECT().GetAttestingIndicies(gomock.Any(), true).DoAndReturn(func(att *solid.Attestation, _ bool) ([]uint64, error) {
		var indices []uint64
		offset := 0
		for _, committee := range att.CommitteeBits.GetOnIndices() {
			for m := range packingCommitteeSize {
				if att.AggregationBits.GetBitAt(offset + m) {
					indices = append(indices, validatorOf(committee, m))
				}
			}
			offset += packingCommitteeSize
		}
		return indices, nil
	}).AnyTimes()
	f.state.EXPECT().ValidatorForValidatorIndex(gomock.Any()).DoAndReturn(func(index int) (solid.Validator, error) {
		validator := solid.NewValidator()
		validator.SetPublicKey([48]byte(bls.CompressPublicKey(f.keys[index/packingCommitteeSize][index%packingCommitteeSize].PublicKey())))
		return validator, nil
	}).AnyTimes()
	f.state.EXPECT().GetDomain(f.cfg.DomainBeaconAttester, gomock.Any()).Return(f.domain, nil).AnyTimes()
	f.state.EXPECT().BaseRewardPerIncrement().Return(uint64(1000)).AnyTimes()
	f.state.EXPECT().GetAttestationParticipationFlagIndicies(gomock.Any(), gomock.Any(), gomock.Any(), false).Return([]uint8{0, 1, 2}, nil).AnyTimes()
	f.state.EXPECT().EpochParticipationForValidatorIndex(gomock.Any(), gomock.Any()).Return(cltypes.ParticipationFlags(0)).AnyTimes()
	f.state.EXPECT().ValidatorEffectiveBalance(gomock.Any()).Return(f.cfg.MaxEffectiveBalance, nil).AnyTimes()
	return f
}

// signedGossip adds a gossip aggregate of data signed over its real signing root.
func (f *verifyingPackingFixture) signedGossip(data *solid.AttestationData, committee int, members ...int) {
	signingRoot, err := fork.ComputeSigningRoot(data, f.domain)
	require.NoError(f.t, err)
	f.msg = signingRoot[:]
	f.gossipData(data, committee, members...)
}
