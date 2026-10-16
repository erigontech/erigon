// Copyright 2024 The Erigon Authors
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

package aggregation

import (
	"bytes"
	"context"
	"errors"
	"log"
	"maps"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/cl/utils/bls"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
)

var (
	attData1 = &solid.AttestationData{
		Source: solid.Checkpoint{
			Epoch: 1,
			Root:  [32]byte{0, 4, 2, 6},
		},
		Target: solid.Checkpoint{
			Epoch: 1,
			Root:  [32]byte{0, 4, 2, 6},
		},
	}
	att1_1 = &solid.Attestation{
		AggregationBits: solid.BitlistFromBytes([]byte{0b01000001}, 2048),
		Data:            attData1,
		Signature:       [96]byte{'a', 'b', 'c', 'd', 'e', 'f'},
	}
	att1_2 = &solid.Attestation{
		AggregationBits: solid.BitlistFromBytes([]byte{0b01000001}, 2048),
		Data:            attData1,
		Signature:       [96]byte{'d', 'e', 'f', 'g', 'h', 'i'},
	}
	att1_3 = &solid.Attestation{
		AggregationBits: solid.BitlistFromBytes([]byte{0b01000100}, 2048),
		Data:            attData1,
		Signature:       [96]byte{'g', 'h', 'i', 'j', 'k', 'l'},
	}
	att1_4 = &solid.Attestation{
		AggregationBits: solid.BitlistFromBytes([]byte{0b01100000}, 2048),
		Data:            attData1,
		Signature:       [96]byte{'m', 'n', 'o', 'p', 'q', 'r'},
	}
	attData1Root, _ = attData1.HashSSZ()

	attData2 = &solid.AttestationData{
		Source: solid.Checkpoint{
			Epoch: 3,
			Root:  [32]byte{5, 5, 6, 6},
		},
		Target: solid.Checkpoint{
			Epoch: 1,
			Root:  [32]byte{0, 4, 2, 6},
		},
	}
	att2_1 = &solid.Attestation{
		AggregationBits: solid.BitlistFromBytes([]byte{0b00000001}, 2048),
		Data:            attData2,
		Signature:       [96]byte{'t', 'e', 's', 't', 'i', 'n'},
	}

	mockAggrResult = [96]byte{'m', 'o', 'c', 'k'}
)

type PoolTestSuite struct {
	mockEthClock     *eth_clock.MockEthereumClock
	mockBeaconConfig *clparams.BeaconChainConfig
	ctrl             *gomock.Controller
	suite.Suite
}

type fakeSignatureAggregator struct {
	sigs     [][]byte
	delay    time.Duration
	failNext error
	failAll  error
}

func (f *fakeSignatureAggregator) AddValidated(sig []byte) error {
	time.Sleep(f.delay)
	if f.failAll != nil {
		return f.failAll
	}
	if len(f.sigs) > 0 && f.failNext != nil {
		return f.failNext
	}
	f.sigs = append(f.sigs, sig)
	return nil
}

func (f *fakeSignatureAggregator) Compressed() [96]byte {
	if len(f.sigs) == 1 {
		return [96]byte(f.sigs[0])
	}
	return mockAggrResult
}

func (t *PoolTestSuite) SetupTest() {
	newSignatureAggregator = func() signatureAggregator { return &fakeSignatureAggregator{} }
	blsAggregate = func(sigs [][]byte) ([]byte, error) {
		ret := make([]byte, 96)
		copy(ret, mockAggrResult[:])
		return ret, nil
	}
	t.ctrl = gomock.NewController(t.T())
	t.mockEthClock = eth_clock.NewMockEthereumClock(t.ctrl)
	t.mockBeaconConfig = &clparams.BeaconChainConfig{
		SlotsPerEpoch:             32,
		MaxCommitteesPerSlot:      64,
		MaxValidatorsPerCommittee: 2048,
	}
}

func (t *PoolTestSuite) TearDownTest() {
	t.ctrl.Finish()
	blsAggregate = bls.AggregateSignatures
	newSignatureAggregator = func() signatureAggregator { return &bls.SignatureAggregator{} }
}

func (t *PoolTestSuite) TestAddAttestationElectra() {
	cBits1 := solid.NewBitVector(64)
	t.Require().NoError(cBits1.SetBitAt(10, true))
	cBits2 := solid.NewBitVector(64)
	t.Require().NoError(cBits2.SetBitAt(10, true))
	expectedCommitteeBits := solid.NewBitVector(64)
	t.Require().NoError(expectedCommitteeBits.SetBitAt(10, true))
	expected := &solid.Attestation{
		AggregationBits: solid.BitlistFromBytes([]byte{0b00001101}, 2048*64),
		Data:            attData1,
		Signature:       mockAggrResult,
		CommitteeBits:   expectedCommitteeBits,
	}
	expected.SetVersion(clparams.ElectraVersion)

	att1 := &solid.Attestation{
		AggregationBits: solid.BitlistFromBytes([]byte{0b00001001}, 2048*64),
		Data:            attData1,
		Signature:       [96]byte{'a', 'b', 'c', 'd', 'e', 'f'},
		CommitteeBits:   cBits1,
	}
	att2 := &solid.Attestation{
		AggregationBits: solid.BitlistFromBytes([]byte{0b00001100}, 2048*64),
		Data:            attData1,
		Signature:       [96]byte{'d', 'e', 'f', 'g', 'h', 'i'},
		CommitteeBits:   cBits2,
	}
	testcases := []struct {
		name     string
		atts     []*solid.Attestation
		hashRoot [32]byte
		mockFunc func()
		expect   *solid.Attestation
	}{
		{
			name: "electra case",
			atts: []*solid.Attestation{
				att1,
				att2,
			},
			hashRoot: attData1Root,
			mockFunc: func() {
				t.mockEthClock.EXPECT().GetEpochAtSlot(gomock.Any()).Return(uint64(1)).Times(2)
				t.mockEthClock.EXPECT().StateVersionByEpoch(gomock.Any()).Return(clparams.ElectraVersion).Times(2)
			},
			expect: expected,
		},
	}

	for _, tc := range testcases {
		func() {
			log.Printf("test case: %s", tc.name)
			if tc.mockFunc != nil {
				tc.mockFunc()
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			pool := NewAggregationPool(ctx, t.mockBeaconConfig, nil, t.mockEthClock)
			for i := range tc.atts {
				t.Require().NoError(pool.AddAttestation(tc.atts[i]))
			}
			att := pool.GetAggregatationByRootAndCommittee(tc.hashRoot, 1, 10)
			t.Equal(tc.expect, att, tc.name)
		}()
	}
}

func (t *PoolTestSuite) TestMergedGloasAttestationUsesProgressiveHash() {
	committeeBits := solid.NewBitVector(64)
	t.Require().NoError(committeeBits.SetBitAt(10, true))
	att1 := &solid.Attestation{
		AggregationBits: solid.BitlistFromBytes([]byte{0b00001001}, 2048*64),
		Data:            attData1,
		Signature:       [96]byte{'a'},
		CommitteeBits:   committeeBits,
	}
	att2 := att1.Copy()
	att2.AggregationBits = solid.BitlistFromBytes([]byte{0b00001100}, 2048*64)
	att2.Signature = [96]byte{'b'}
	t.mockEthClock.EXPECT().GetEpochAtSlot(gomock.Any()).Return(uint64(1)).Times(2)
	t.mockEthClock.EXPECT().StateVersionByEpoch(gomock.Any()).Return(clparams.GloasVersion).Times(2)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pool := NewAggregationPool(ctx, t.mockBeaconConfig, nil, t.mockEthClock)

	t.Require().NoError(pool.AddAttestation(att1))
	t.Require().NoError(pool.AddAttestation(att2))
	merged := pool.GetAggregatationByRootAndCommittee(attData1Root, 1, 10)
	got, err := merged.HashSSZ()
	t.Require().NoError(err)
	want, err := merged.HashSSZProgressive()
	t.Require().NoError(err)
	t.Equal(want, got)
}

func (t *PoolTestSuite) TestFirstGloasAttestationIsCopiedAndVersioned() {
	committeeBits := solid.NewBitVector(64)
	t.Require().NoError(committeeBits.SetBitAt(10, true))
	att := &solid.Attestation{
		AggregationBits: solid.BitlistFromBytes([]byte{0b00001001}, 2048*64),
		Data:            attData1,
		Signature:       [96]byte{'a'},
		CommitteeBits:   committeeBits,
	}
	callerRoot, err := att.HashSSZ()
	t.Require().NoError(err)
	t.mockEthClock.EXPECT().GetEpochAtSlot(gomock.Any()).Return(uint64(1))
	t.mockEthClock.EXPECT().StateVersionByEpoch(gomock.Any()).Return(clparams.GloasVersion)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pool := NewAggregationPool(ctx, t.mockBeaconConfig, nil, t.mockEthClock)

	t.Require().NoError(pool.AddAttestation(att))
	afterAddRoot, err := att.HashSSZ()
	t.Require().NoError(err)
	t.Equal(callerRoot, afterAddRoot)
	cached := pool.GetAggregatationByRootAndCommittee(attData1Root, 1, 10)
	t.NotSame(att, cached)
	att.SetVersion(clparams.ElectraVersion)
	got, err := cached.HashSSZ()
	t.Require().NoError(err)
	want, err := cached.HashSSZProgressive()
	t.Require().NoError(err)
	t.Equal(want, got)
}

func (t *PoolTestSuite) TestElectraAggregationErrorIsReturned() {
	committeeBits := solid.NewBitVector(64)
	t.Require().NoError(committeeBits.SetBitAt(10, true))
	att := &solid.Attestation{
		AggregationBits: solid.BitlistFromBytes([]byte{0b00001001}, 2048*64),
		Data:            attData1,
		Signature:       [96]byte{'a'},
		CommitteeBits:   committeeBits,
	}
	t.mockEthClock.EXPECT().GetEpochAtSlot(gomock.Any()).Return(uint64(1)).Times(2)
	t.mockEthClock.EXPECT().StateVersionByEpoch(gomock.Any()).Return(clparams.ElectraVersion).Times(2)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pool := NewAggregationPool(ctx, t.mockBeaconConfig, nil, t.mockEthClock)

	t.Require().NoError(pool.AddAttestation(att))
	t.ErrorIs(pool.AddAttestation(att.Copy()), ErrIsSuperset)
}

func (t *PoolTestSuite) TestSlotIsStale() {
	netConfig := &clparams.NetworkConfig{AttestationPropagationSlotRange: 32}
	const curSlot = uint64(100)

	t.mockEthClock.EXPECT().GetCurrentSlot().Return(curSlot).AnyTimes()
	pool := &aggregationPoolImpl{ethClock: t.mockEthClock, netConfig: netConfig}

	testcases := []struct {
		name       string
		targetSlot uint64
		expect     bool
	}{
		{name: "future slot (target > current)", targetSlot: 101, expect: false},
		{name: "current slot", targetSlot: 100, expect: false},
		{name: "one slot behind", targetSlot: 99, expect: false},
		{name: "at range boundary", targetSlot: 68, expect: false},
		{name: "past range boundary", targetSlot: 67, expect: true},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func() {
			t.Equal(tc.expect, pool.slotIsStale(tc.targetSlot))
		})
	}
}

func (t *PoolTestSuite) TestAddAttestation() {
	testcases := []struct {
		name     string
		atts     []*solid.Attestation
		hashRoot [32]byte
		mockFunc func()
		expect   *solid.Attestation
	}{
		{
			name: "simple, different hashRoot",
			atts: []*solid.Attestation{
				att1_1,
				att2_1,
			},
			hashRoot: attData1Root,
			mockFunc: func() {
				t.mockEthClock.EXPECT().GetEpochAtSlot(gomock.Any()).Return(uint64(1)).AnyTimes()
				t.mockEthClock.EXPECT().StateVersionByEpoch(gomock.Any()).Return(clparams.DenebVersion).AnyTimes()
			},
			expect: att1_1,
		},
		{
			name: "att1_2 is a super set of att1_1. skip att1_1",
			atts: []*solid.Attestation{
				att1_2,
				att1_1,
				att2_1, // none of its business
			},
			hashRoot: attData1Root,
			mockFunc: func() {
				t.mockEthClock.EXPECT().GetEpochAtSlot(gomock.Any()).Return(uint64(1)).AnyTimes()
				t.mockEthClock.EXPECT().StateVersionByEpoch(gomock.Any()).Return(clparams.DenebVersion).AnyTimes()
			},
			expect: att1_2,
		},
		{
			name: "merge att1_2, att1_3, att1_4",
			atts: []*solid.Attestation{
				att1_2,
				att1_3,
				att1_4,
			},
			hashRoot: attData1Root,
			mockFunc: func() {
				t.mockEthClock.EXPECT().GetEpochAtSlot(gomock.Any()).Return(uint64(1)).AnyTimes()
				t.mockEthClock.EXPECT().StateVersionByEpoch(gomock.Any()).Return(clparams.DenebVersion).AnyTimes()
			},
			expect: &solid.Attestation{
				AggregationBits: solid.BitlistFromBytes([]byte{0b01100101}, 2048),
				Data:            attData1,
				Signature:       mockAggrResult,
			},
		},
	}

	for _, tc := range testcases {
		func() {
			log.Printf("test case: %s", tc.name)
			if tc.mockFunc != nil {
				tc.mockFunc()
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			pool := NewAggregationPool(ctx, t.mockBeaconConfig, nil, t.mockEthClock)
			for i := range tc.atts {
				// Subset attestations are rejected by design; anything else is a failure.
				if err := pool.AddAttestation(tc.atts[i]); err != nil {
					t.Require().ErrorIs(err, ErrIsSuperset)
				}
			}
			att := pool.GetAggregatationByRoot(tc.hashRoot)
			expected := tc.expect.Copy()
			expected.SetVersion(clparams.DenebVersion)
			t.Equal(expected, att, tc.name)
		}()
	}
}

func (t *PoolTestSuite) TestAggregatesReturnsEveryCommitteeAggregate() {
	t.mockEthClock.EXPECT().GetEpochAtSlot(gomock.Any()).Return(uint64(1)).AnyTimes()
	t.mockEthClock.EXPECT().StateVersionByEpoch(gomock.Any()).Return(clparams.ElectraVersion).AnyTimes()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pool := NewAggregationPool(ctx, t.mockBeaconConfig, nil, t.mockEthClock)
	t.Require().NoError(pool.AddAttestation(t.electraAttestation(5, 10, 0b00001001)))
	t.Require().NoError(pool.AddAttestation(t.electraAttestation(5, 10, 0b00001100)))
	t.Require().NoError(pool.AddAttestation(t.electraAttestation(5, 11, 0b00000011)))

	committees := map[int]byte{}
	for _, att := range pool.Aggregates() {
		indices := att.CommitteeBits.GetOnIndices()
		t.Require().Len(indices, 1)
		committees[indices[0]] = att.AggregationBits.Bytes()[0]
	}
	t.Equal(map[int]byte{10: 0b00001101, 11: 0b00000011}, committees)
}

func (t *PoolTestSuite) electraAttestation(slot uint64, committee int, bits byte) *solid.Attestation {
	cBits := solid.NewBitVector(64)
	t.Require().NoError(cBits.SetBitAt(committee, true))
	data := &solid.AttestationData{Slot: slot, Source: attData1.Source, Target: attData1.Target}
	return &solid.Attestation{
		AggregationBits: solid.BitlistFromBytes([]byte{bits}, 2048*64),
		Data:            data,
		Signature:       [96]byte{bits},
		CommitteeBits:   cBits,
	}
}

// Since Deneb an attestation stays includable until the end of the epoch after its own.
func (t *PoolTestSuite) TestPruneStaleKeepsInclusionWindow() {
	t.mockEthClock.EXPECT().GetEpochAtSlot(gomock.Any()).DoAndReturn(func(slot uint64) uint64 { return slot / 32 }).AnyTimes()
	t.mockEthClock.EXPECT().StateVersionByEpoch(gomock.Any()).Return(clparams.ElectraVersion).AnyTimes()
	t.mockEthClock.EXPECT().GetCurrentSlot().Return(uint64(100)).AnyTimes()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pool := NewAggregationPool(ctx, t.mockBeaconConfig, &clparams.NetworkConfig{AttestationPropagationSlotRange: 32}, t.mockEthClock).(*aggregationPoolImpl)
	for committee, slot := range []uint64{1, 63, 64, 100, 101} {
		t.Require().NoError(pool.AddAttestation(t.electraAttestation(slot, committee, 0b11)))
	}

	pool.pruneStale()

	slots := []uint64{}
	for _, att := range pool.Aggregates() {
		slots = append(slots, att.Data.Slot)
	}
	t.ElementsMatch([]uint64{64, 100, 101}, slots)
}

func (t *PoolTestSuite) TestAddAttestationElectraKeepsConcurrentVotes() {
	t.mockEthClock.EXPECT().GetEpochAtSlot(gomock.Any()).Return(uint64(1)).AnyTimes()
	t.mockEthClock.EXPECT().StateVersionByEpoch(gomock.Any()).Return(clparams.ElectraVersion).AnyTimes()
	newSignatureAggregator = func() signatureAggregator {
		return &fakeSignatureAggregator{delay: time.Millisecond} // widen the window between reading and storing the aggregate
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pool := NewAggregationPool(ctx, t.mockBeaconConfig, nil, t.mockEthClock)
	var wg sync.WaitGroup
	for bit := range 7 {
		wg.Go(func() {
			t.NoError(pool.AddAttestation(t.electraAttestation(5, 4, byte(1<<bit)|0x80)))
		})
	}
	wg.Wait()

	aggregates := pool.Aggregates()
	t.Require().Len(aggregates, 1)
	t.Equal(byte(0xff), aggregates[0].AggregationBits.Bytes()[0])
}

func (t *PoolTestSuite) TestAddAttestationElectraKeepsAggregateOnSignatureError() {
	t.mockEthClock.EXPECT().GetEpochAtSlot(gomock.Any()).Return(uint64(1)).AnyTimes()
	t.mockEthClock.EXPECT().StateVersionByEpoch(gomock.Any()).Return(clparams.ElectraVersion).AnyTimes()
	errBadSignature := errors.New("bad signature")
	newSignatureAggregator = func() signatureAggregator { return &fakeSignatureAggregator{failNext: errBadSignature} }
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pool := NewAggregationPool(ctx, t.mockBeaconConfig, nil, t.mockEthClock)
	t.Require().NoError(pool.AddAttestation(t.electraAttestation(5, 1, 0b00001001)))

	t.ErrorIs(pool.AddAttestation(t.electraAttestation(5, 1, 0b00001100)), errBadSignature)

	aggregates := pool.Aggregates()
	t.Require().Len(aggregates, 1)
	t.Equal(byte(0b00001001), aggregates[0].AggregationBits.Bytes()[0])
}

// electraAttestationWithSource gives each source root its own AttestationData, as an attester
// alternating sources does.
func (t *PoolTestSuite) electraAttestationWithSource(slot uint64, committee int, source byte) *solid.Attestation {
	att := t.electraAttestation(slot, committee, 0b11)
	att.Data.Source.Root = [32]byte{source}
	return att
}

func (t *PoolTestSuite) TestAddAttestationElectraLimitsDataPerCommittee() {
	t.mockEthClock.EXPECT().GetEpochAtSlot(gomock.Any()).DoAndReturn(func(slot uint64) uint64 { return slot / 32 }).AnyTimes()
	t.mockEthClock.EXPECT().StateVersionByEpoch(gomock.Any()).Return(clparams.ElectraVersion).AnyTimes()
	t.mockEthClock.EXPECT().GetCurrentSlot().Return(uint64(1000)).AnyTimes()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pool := NewAggregationPool(ctx, t.mockBeaconConfig, &clparams.NetworkConfig{AttestationPropagationSlotRange: 32}, t.mockEthClock).(*aggregationPoolImpl)
	for source := range maxDataPerCommittee {
		att := t.electraAttestationWithSource(5, 1, byte(source))
		bits := []byte{0, 0x80} // two votes each, from members of their own
		bits[0] |= 0b11 << (2 * source)
		att.AggregationBits = solid.BitlistFromBytes(bits, 2048*64)
		t.Require().NoError(pool.AddAttestation(att))
	}
	t.NoError(pool.AddAttestation(t.singleVote(0xff, 8)))
	t.Len(pool.Aggregates(), maxDataPerCommittee, "a single vote does not displace stronger data")
	t.NoError(pool.AddAttestation(t.electraAttestationWithSource(5, 2, 0xff)), "other committees keep their own allowance")

	pool.pruneStale() // current slot 1000: everything is pruned and the allowance comes back
	t.Empty(pool.refused)
	t.NoError(pool.AddAttestation(t.electraAttestationWithSource(5, 1, 0xff)))
}

// singleVote is one committee member's vote, as the attestation service adds it: a committee of 15
// members, each source root giving its own AttestationData.
func (t *PoolTestSuite) singleVote(source byte, member int) *solid.Attestation {
	att := t.electraAttestationWithSource(5, 1, source)
	bits := []byte{0, 0x80}
	bits[member/8] |= 1 << (member % 8)
	att.AggregationBits = solid.BitlistFromBytes(bits, 2048*64)
	return att
}

// fillCommitteeWithTwoVoteData stores maxDataPerCommittee data with two votes each from members 0-7.
func (t *PoolTestSuite) fillCommitteeWithTwoVoteData(pool AggregationPool) {
	member := 0
	for source := range maxDataPerCommittee {
		for range 2 {
			t.Require().NoError(pool.AddAttestation(t.singleVote(byte(source), member)))
			member++
		}
	}
}

func (t *PoolTestSuite) committeeAggregateBits(pool AggregationPool, source byte) []byte {
	for _, att := range pool.Aggregates() {
		if att.Data.Source.Root[0] == source {
			return att.AggregationBits.Bytes()
		}
	}
	return nil
}

// Attesters voting early for several data must not lock the committee's honest data out. A full
// committee keeps the votes of refused data, and that data replaces the weakest stored data, with
// all its votes, once it has more voters.
func (t *PoolTestSuite) TestAddAttestationElectraEvictsWeakestDataPerCommittee() {
	t.mockEthClock.EXPECT().GetEpochAtSlot(gomock.Any()).Return(uint64(1)).AnyTimes()
	t.mockEthClock.EXPECT().StateVersionByEpoch(gomock.Any()).Return(clparams.ElectraVersion).AnyTimes()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pool := NewAggregationPool(ctx, t.mockBeaconConfig, nil, t.mockEthClock)
	t.fillCommitteeWithTwoVoteData(pool)

	t.Require().NoError(pool.AddAttestation(t.singleVote(0xaa, 8)))
	t.Require().NoError(pool.AddAttestation(t.singleVote(0xaa, 9)))
	t.Nil(t.committeeAggregateBits(pool, 0xaa), "two voters do not displace data with two")
	t.Require().NoError(pool.AddAttestation(t.singleVote(0xaa, 10)))
	t.Equal([]byte{0, 0x87}, t.committeeAggregateBits(pool, 0xaa), "the third voter brings all three votes in")
	t.Require().NoError(pool.AddAttestation(t.singleVote(0xaa, 11)))
	t.Equal([]byte{0, 0x8f}, t.committeeAggregateBits(pool, 0xaa))
	t.Len(pool.Aggregates(), maxDataPerCommittee)

	t.Nil(t.committeeAggregateBits(pool, 0), "the first data with the fewest votes was displaced")
	t.Require().NoError(pool.AddAttestation(t.singleVote(0, 12)))
	t.Equal([]byte{0x03, 0x90}, t.committeeAggregateBits(pool, 0), "displaced data keeps its votes and comes back with one more voter")
}

// A repeated delivery of one vote must not count as another voter.
func (t *PoolTestSuite) TestAddAttestationElectraRefusedDataCountsVotersOnce() {
	t.mockEthClock.EXPECT().GetEpochAtSlot(gomock.Any()).Return(uint64(1)).AnyTimes()
	t.mockEthClock.EXPECT().StateVersionByEpoch(gomock.Any()).Return(clparams.ElectraVersion).AnyTimes()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pool := NewAggregationPool(ctx, t.mockBeaconConfig, nil, t.mockEthClock)
	t.fillCommitteeWithTwoVoteData(pool)

	t.Require().NoError(pool.AddAttestation(t.singleVote(0xaa, 8)))
	for range 3 {
		t.ErrorIs(pool.AddAttestation(t.singleVote(0xaa, 8)), ErrIsSuperset)
	}
	t.Nil(t.committeeAggregateBits(pool, 0xaa))
	for source := range maxDataPerCommittee {
		t.NotNil(t.committeeAggregateBits(pool, byte(source)))
	}
}

// Refused data is bounded per committee. New data always enters, and the refused data with the
// fewest voters gives way, so data that already gathered more voters stays.
func (t *PoolTestSuite) TestAddAttestationElectraBoundsRefusedDataPerCommittee() {
	t.mockEthClock.EXPECT().GetEpochAtSlot(gomock.Any()).Return(uint64(1)).AnyTimes()
	t.mockEthClock.EXPECT().StateVersionByEpoch(gomock.Any()).Return(clparams.ElectraVersion).AnyTimes()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pool := NewAggregationPool(ctx, t.mockBeaconConfig, nil, t.mockEthClock).(*aggregationPoolImpl)
	t.fillCommitteeWithTwoVoteData(pool)
	t.Require().NoError(pool.AddAttestation(t.singleVote(0xaa, 8)))
	t.Require().NoError(pool.AddAttestation(t.singleVote(0xaa, 9)))

	for i := range maxDataPerCommittee {
		t.Require().NoError(pool.AddAttestation(t.singleVote(byte(0xb0+i), 10+i)))
	}
	t.Len(pool.refused[slotCommittee{slot: 5, committee: 1}], maxDataPerCommittee)

	t.Require().NoError(pool.AddAttestation(t.singleVote(0xaa, 14)), "the refused data kept its two voters")
	t.Equal([]byte{0, 0xc3}, t.committeeAggregateBits(pool, 0xaa))
}

// A full set of refused data must not lock out new data: each new data root starts with one voter,
// so it takes the place of the weakest refused data and gathers its votes from there.
func (t *PoolTestSuite) TestAddAttestationElectraNewDataEntersAFullRefusedSet() {
	t.mockEthClock.EXPECT().GetEpochAtSlot(gomock.Any()).Return(uint64(1)).AnyTimes()
	t.mockEthClock.EXPECT().StateVersionByEpoch(gomock.Any()).Return(clparams.ElectraVersion).AnyTimes()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pool := NewAggregationPool(ctx, t.mockBeaconConfig, nil, t.mockEthClock).(*aggregationPoolImpl)
	// A committee of 31 members.
	vote := func(source byte, member int) *solid.Attestation {
		att := t.electraAttestationWithSource(5, 1, source)
		bits := []byte{0, 0, 0, 0x80}
		bits[member/8] |= 1 << (member % 8)
		att.AggregationBits = solid.BitlistFromBytes(bits, 2048*64)
		return att
	}
	member := 0
	for source := range 2 * maxDataPerCommittee { // stored data, then refused data, two voters each
		for range 2 {
			t.Require().NoError(pool.AddAttestation(vote(byte(source), member)))
			member++
		}
	}
	t.Require().Len(pool.refused[slotCommittee{slot: 5, committee: 1}], maxDataPerCommittee)

	for range 3 {
		t.Require().NoError(pool.AddAttestation(vote(0xcc, member)))
		member++
	}
	bits := t.committeeAggregateBits(pool, 0xcc)
	t.Require().NotNil(bits, "the new data gathered three voters and displaced stored data with two")
	t.Equal(3, utils.BitsOnCount(bits)-1)
}

// Among refused data with as many voters, the tie-break picks which gives way, so a stream of fresh
// one-vote data cannot predictably flush data that is gathering voters.
func (t *PoolTestSuite) TestAddAttestationElectraTieBreakPicksTheEvictedRefusedData() {
	t.mockEthClock.EXPECT().GetEpochAtSlot(gomock.Any()).Return(uint64(1)).AnyTimes()
	t.mockEthClock.EXPECT().StateVersionByEpoch(gomock.Any()).Return(clparams.ElectraVersion).AnyTimes()
	defer func(tieBreak func(int) int) { refusedTieBreak = tieBreak }(refusedTieBreak)
	for pick := range maxDataPerCommittee {
		ctx, cancel := context.WithCancel(context.Background())
		pool := NewAggregationPool(ctx, t.mockBeaconConfig, nil, t.mockEthClock).(*aggregationPoolImpl)
		t.fillCommitteeWithTwoVoteData(pool)
		var roots []common.Hash
		for i := range maxDataPerCommittee {
			vote := t.singleVote(byte(0xb0+i), 8+i)
			t.Require().NoError(pool.AddAttestation(vote))
			root, err := vote.Data.HashSSZ()
			t.Require().NoError(err)
			roots = append(roots, root)
		}
		slices.SortFunc(roots, func(a, b common.Hash) int { return bytes.Compare(a[:], b[:]) })
		refusedTieBreak = func(n int) int {
			t.Equal(maxDataPerCommittee, n, "every single-vote data is a candidate")
			return pick
		}

		t.Require().NoError(pool.AddAttestation(t.singleVote(0xcc, 12)))

		refused := pool.refused[slotCommittee{slot: 5, committee: 1}]
		t.Len(refused, maxDataPerCommittee)
		t.NotContains(refused, roots[pick])
		cancel()
	}
}

// pinnedVote is a vote for the data the node serves its validators, from a committee member.
func (t *PoolTestSuite) pinnedVote(member int) *solid.Attestation {
	return t.singleVote(0xaa, member)
}

func (t *PoolTestSuite) newPinTestPool() *aggregationPoolImpl {
	t.mockEthClock.EXPECT().GetEpochAtSlot(gomock.Any()).DoAndReturn(func(slot uint64) uint64 { return slot / 32 }).AnyTimes()
	t.mockEthClock.EXPECT().StateVersionByEpoch(gomock.Any()).Return(clparams.ElectraVersion).AnyTimes()
	t.mockEthClock.EXPECT().GetCurrentSlot().Return(uint64(5)).AnyTimes()
	ctx, cancel := context.WithCancel(context.Background())
	t.T().Cleanup(cancel)
	return NewAggregationPool(ctx, t.mockBeaconConfig, nil, t.mockEthClock).(*aggregationPoolImpl)
}

// Fresh one-vote data must not keep the data our validators attest to out of a full committee, or
// our aggregators find nothing to aggregate: that data is stored from its first vote and keeps every
// later vote.
func (t *PoolTestSuite) TestAddAttestationElectraStoresPinnedDataInAFullCommittee() {
	pool := t.newPinTestPool()
	t.fillCommitteeWithTwoVoteData(pool)
	t.Require().NoError(pool.PinAttestationData(t.pinnedVote(8).Data))

	for round, votes := range []struct {
		pinned int
		fresh  []int
	}{{pinned: 8, fresh: []int{9, 10, 11, 12}}, {pinned: 13, fresh: []int{14}}} {
		t.Require().NoError(pool.AddAttestation(t.pinnedVote(votes.pinned)))
		for i, member := range votes.fresh {
			t.Require().NoError(pool.AddAttestation(t.singleVote(byte(0xb0+4*round+i), member)))
		}
	}

	t.Equal([]byte{0, 0xa1}, t.committeeAggregateBits(pool, 0xaa), "both votes are kept")
	t.Len(pool.Aggregates(), maxDataPerCommittee, "pinned data takes the place of the weakest data")
	t.Nil(t.committeeAggregateBits(pool, 0), "the first data with the fewest votes gave way")
}

// Votes refused before the data was pinned are kept: pinning moves them into the store.
func (t *PoolTestSuite) TestAddAttestationElectraPinPromotesRefusedVotes() {
	pool := t.newPinTestPool()
	t.fillCommitteeWithTwoVoteData(pool)
	t.Require().NoError(pool.AddAttestation(t.pinnedVote(8)))
	t.Require().Nil(t.committeeAggregateBits(pool, 0xaa))

	t.Require().NoError(pool.PinAttestationData(t.pinnedVote(8).Data))

	t.Equal([]byte{0, 0x81}, t.committeeAggregateBits(pool, 0xaa))
	t.Len(pool.Aggregates(), maxDataPerCommittee)
	t.Equal([]byte{0x03, 0x80}, t.refusedBits(pool, 0), "the displaced data keeps its votes aside")
}

// Pinned data enters a committee whose refused set is full: the displaced data still keeps its votes.
func (t *PoolTestSuite) TestAddAttestationElectraStoresPinnedDataBesideAFullRefusedSet() {
	pool := t.newPinTestPool()
	t.fillCommitteeWithTwoVoteData(pool)
	for i := range maxDataPerCommittee {
		t.Require().NoError(pool.AddAttestation(t.singleVote(byte(0xb0+i), 8+i)))
	}
	t.Require().NoError(pool.PinAttestationData(t.pinnedVote(12).Data))

	t.Require().NoError(pool.AddAttestation(t.pinnedVote(12)))

	t.Equal([]byte{0, 0x90}, t.committeeAggregateBits(pool, 0xaa))
	t.Len(pool.refused[slotCommittee{slot: 5, committee: 1}], maxDataPerCommittee)
	t.Equal([]byte{0x03, 0x80}, t.refusedBits(pool, 0), "the displaced data with two votes outranks single votes")
}

// The committee bound holds when every stored data is pinned: further pinned data waits with the
// refused data, with its votes, whether its vote comes before or after the pin.
func (t *PoolTestSuite) TestAddAttestationElectraKeepsPinnedDataWithinTheCommitteeBound() {
	pool := t.newPinTestPool()
	for i := range maxDataPerCommittee + 1 {
		t.Require().NoError(pool.PinAttestationData(t.singleVote(byte(0xa0+i), i).Data))
		t.Require().NoError(pool.AddAttestation(t.singleVote(byte(0xa0+i), i)))
	}
	late := t.singleVote(byte(0xa0+maxDataPerCommittee+1), maxDataPerCommittee+1)
	t.Require().NoError(pool.AddAttestation(late))
	t.Require().NoError(pool.PinAttestationData(late.Data))

	t.Len(pool.Aggregates(), maxDataPerCommittee)
	t.NotNil(t.refusedBits(pool, byte(0xa0+maxDataPerCommittee)))
	t.NotNil(t.refusedBits(pool, byte(0xa0+maxDataPerCommittee+1)))
}

// pinStoredData fills the committee with pinned data, one vote each from members 0-3.
func (t *PoolTestSuite) pinStoredData(pool *aggregationPoolImpl) {
	for i := range maxDataPerCommittee {
		t.Require().NoError(pool.PinAttestationData(t.singleVote(byte(0xa0+i), i).Data))
		t.Require().NoError(pool.AddAttestation(t.singleVote(byte(0xa0+i), i)))
	}
}

// Pinned data waiting with the refused data is never the refused data that gives way, even when it
// has the fewest voters.
func (t *PoolTestSuite) TestAddAttestationElectraKeepsWaitingPinnedDataInAFullRefusedSet() {
	pool := t.newPinTestPool()
	t.pinStoredData(pool)
	waiting := t.singleVote(0xa4, 4)
	t.Require().NoError(pool.PinAttestationData(waiting.Data))
	t.Require().NoError(pool.AddAttestation(waiting))
	member := 5
	for source := range maxDataPerCommittee - 1 {
		for range 2 {
			t.Require().NoError(pool.AddAttestation(t.singleVote(byte(0xb0+source), member)))
			member++
		}
	}

	t.Require().NoError(pool.AddAttestation(t.singleVote(0xcc, member)))

	t.Equal([]byte{0x10, 0x80}, t.refusedBits(pool, 0xa4))
	t.Len(pool.refused[slotCommittee{slot: 5, committee: 1}], maxDataPerCommittee)
	t.NotNil(t.refusedBits(pool, 0xcc), "an unpinned refused data gave way")
}

// When the stored and the refused data are all pinned, new data is refused: the bounds hold and the
// pinned data keeps its votes.
func (t *PoolTestSuite) TestAddAttestationElectraRefusesNewDataWhenEveryDataIsPinned() {
	pool := t.newPinTestPool()
	t.pinStoredData(pool)
	for i := range maxDataPerCommittee {
		waiting := t.singleVote(byte(0xa4+i), 4+i)
		t.Require().NoError(pool.PinAttestationData(waiting.Data))
		t.Require().NoError(pool.AddAttestation(waiting))
	}
	pinnedSources := t.refusedSources(pool)

	t.ErrorIs(pool.AddAttestation(t.singleVote(0xcc, 8)), ErrPoolLimit)
	late := t.singleVote(0xa8, 9)
	t.Require().NoError(pool.PinAttestationData(late.Data))
	t.ErrorIs(pool.AddAttestation(late), ErrPoolLimit)

	t.Len(pool.Aggregates(), maxDataPerCommittee)
	t.Equal(pinnedSources, t.refusedSources(pool))
}

// Pinned data keeps every vote against both sequences that flush unpinned data: fresh one-vote data
// between its votes, and refused data as strong as the weakest stored data that leaves it the only
// candidate for eviction.
func (t *PoolTestSuite) TestAddAttestationElectraPinnedDataKeepsEveryVote() {
	pool := t.newPinTestPool()
	vote := func(source byte, member int) *solid.Attestation { // a committee of 63 members
		att := t.electraAttestationWithSource(5, 1, source)
		bits := make([]byte, 8)
		bits[7] = 0x80
		bits[member/8] |= 1 << (member % 8)
		att.AggregationBits = solid.BitlistFromBytes(bits, 2048*64)
		return att
	}
	member := 0
	for source := range 2*maxDataPerCommittee - 1 { // four stored and three refused data, two votes each
		for range 2 {
			t.Require().NoError(pool.AddAttestation(vote(byte(source), member)))
			member++
		}
	}
	t.Require().NoError(pool.PinAttestationData(vote(0xaa, 0).Data))
	const honestVotes = 10
	for i := range honestVotes {
		t.Require().NoError(pool.AddAttestation(vote(0xaa, member)))
		t.Require().NoError(pool.AddAttestation(vote(byte(0xd0+i), member+1)))
		member += 2
	}

	bits := t.committeeAggregateBits(pool, 0xaa)
	t.Require().NotNil(bits)
	t.Equal(honestVotes, utils.BitsOnCount(bits)-1)
}

// In a committee with a local aggregator, unpinned data keeps every vote against fresh one-vote data
// between its votes: the aggregator may have got that data from another beacon node.
func (t *PoolTestSuite) TestAddAttestationElectraKeepsEveryVoteForALocalAggregator() {
	pool := t.newPinTestPool()
	pool.AddAggregatorDuty(5, 1)
	vote := func(source byte, member int) *solid.Attestation { // a committee of 63 members
		att := t.electraAttestationWithSource(5, 1, source)
		bits := make([]byte, 8)
		bits[7] = 0x80
		bits[member/8] |= 1 << (member % 8)
		att.AggregationBits = solid.BitlistFromBytes(bits, 2048*64)
		return att
	}
	member := 0
	for source := range 2*maxDataPerCommittee - 1 { // four stored and three refused data, two votes each
		for range 2 {
			t.Require().NoError(pool.AddAttestation(vote(byte(source), member)))
			member++
		}
	}
	const honestVotes = 20
	for i := range honestVotes {
		t.Require().NoError(pool.AddAttestation(vote(0xaa, member)))
		t.Require().NoError(pool.AddAttestation(vote(byte(0xd0+i), member+1)))
		member += 2
	}

	root, err := vote(0xaa, 0).Data.HashSSZ()
	t.Require().NoError(err)
	att := pool.GetAggregatationByRootAndCommittee(root, 5, 1)
	t.Require().NotNil(att)
	t.Equal(honestVotes, utils.BitsOnCount(att.AggregationBits.Bytes())-1)
	t.Len(pool.Aggregates(), maxDataPerCommittee, "block packing still sees the committee bound")
}

// Aggregator duties are kept from the previous epoch to the next, for committees that exist, and are
// pruned with the inclusion window.
func (t *PoolTestSuite) TestAddAggregatorDutyKeepsOnlyDutiesInTheWindow() {
	t.mockEthClock.EXPECT().GetEpochAtSlot(gomock.Any()).DoAndReturn(func(slot uint64) uint64 { return slot / 32 }).AnyTimes()
	currentSlot := uint64(100) // epoch 3
	t.mockEthClock.EXPECT().GetCurrentSlot().DoAndReturn(func() uint64 { return currentSlot }).AnyTimes()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pool := NewAggregationPool(ctx, t.mockBeaconConfig, &clparams.NetworkConfig{AttestationPropagationSlotRange: 32}, t.mockEthClock).(*aggregationPoolImpl)

	pool.AddAggregatorDuty(63, 1)  // epoch 1: too old
	pool.AddAggregatorDuty(64, 1)  // epoch 2
	pool.AddAggregatorDuty(96, 1)  // epoch 3
	pool.AddAggregatorDuty(160, 1) // epoch 5: too far ahead
	pool.AddAggregatorDuty(159, 1) // epoch 4
	pool.AddAggregatorDuty(100, 64)
	t.Equal(map[slotCommittee]struct{}{{slot: 64, committee: 1}: {}, {slot: 96, committee: 1}: {}, {slot: 159, committee: 1}: {}}, pool.aggregatorDuties)
	t.Equal(int(t.mockBeaconConfig.MaxValidatorsPerCommittee), pool.refusedLimit(slotCommittee{slot: 64, committee: 1}))
	t.Equal(maxDataPerCommittee, pool.refusedLimit(slotCommittee{slot: 64, committee: 2}))

	currentSlot = 128 // epoch 4: the duty of the previous epoch stays
	pool.pruneStale()
	t.Equal(map[slotCommittee]struct{}{{slot: 96, committee: 1}: {}, {slot: 159, committee: 1}: {}}, pool.aggregatorDuties)
}

// A member votes once per epoch, in one slot and committee, so a vote of a member already counted for
// other data of its committee equivocates and is refused, stored or kept aside alike.
func (t *PoolTestSuite) TestAddAttestationElectraRefusesAMemberAlreadyCountedForOtherData() {
	pool := t.newPinTestPool()
	t.Require().NoError(pool.AddAttestation(t.singleVote(0xaa, 12)))

	t.ErrorIs(pool.AddAttestation(t.singleVote(0xbb, 12)), ErrVoterAlreadyCounted)
	t.Nil(t.committeeAggregateBits(pool, 0xbb))
	t.ErrorIs(pool.AddAttestation(t.singleVote(0xaa, 12)), ErrIsSuperset, "the same vote again")

	t.fillCommitteeWithTwoVoteData(pool) // members 0-7: the committee is full
	t.Require().NoError(pool.AddAttestation(t.singleVote(0xcc, 9)))
	t.ErrorIs(pool.AddAttestation(t.singleVote(0xdd, 9)), ErrVoterAlreadyCounted)
	t.NotContains(t.refusedSources(pool), byte(0xdd))
}

// One member voting for new data again and again cannot fill the data kept aside for a local
// aggregator, so honest data there keeps its votes.
func (t *PoolTestSuite) TestAddAttestationElectraEquivocatingMemberCannotFlushALocalAggregatorsData() {
	pool := t.newPinTestPool()
	pool.AddAggregatorDuty(5, 1)
	vote := func(source byte, member int) *solid.Attestation { // a committee of 63 members
		att := t.electraAttestationWithSource(5, 1, source)
		bits := make([]byte, 8)
		bits[7] = 0x80
		bits[member/8] |= 1 << (member % 8)
		att.AggregationBits = solid.BitlistFromBytes(bits, 2048*64)
		return att
	}
	member := 0
	for source := range maxDataPerCommittee { // a full committee
		for range 2 {
			t.Require().NoError(pool.AddAttestation(vote(byte(source), member)))
			member++
		}
	}
	honest := vote(0xaa, member)
	t.Require().NoError(pool.AddAttestation(honest))
	t.Require().NoError(pool.AddAttestation(vote(0xb0, member+1)))
	for source := range 100 {
		t.ErrorIs(pool.AddAttestation(vote(byte(source+0x10), member+1)), ErrVoterAlreadyCounted)
	}

	t.Len(pool.refused[slotCommittee{slot: 5, committee: 1}], 2)
	root, err := honest.Data.HashSSZ()
	t.Require().NoError(err)
	t.NotNil(pool.GetAggregatationByRootAndCommittee(root, 5, 1))
}

// Counted members are pruned with the inclusion window.
func (t *PoolTestSuite) TestPruneStaleDropsCountedMembers() {
	t.mockEthClock.EXPECT().GetEpochAtSlot(gomock.Any()).DoAndReturn(func(slot uint64) uint64 { return slot / 32 }).AnyTimes()
	t.mockEthClock.EXPECT().StateVersionByEpoch(gomock.Any()).Return(clparams.ElectraVersion).AnyTimes()
	t.mockEthClock.EXPECT().GetCurrentSlot().Return(uint64(1000)).AnyTimes()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pool := NewAggregationPool(ctx, t.mockBeaconConfig, nil, t.mockEthClock).(*aggregationPoolImpl)
	t.Require().NoError(pool.AddAttestation(t.singleVote(0xaa, 3)))
	t.Require().NoError(pool.AddAttestation(t.electraAttestation(990, 1, 0b11)))
	t.Require().Len(pool.voters, 2)

	pool.pruneStale()

	t.Equal([]slotCommittee{{slot: 990, committee: 1}}, slices.Collect(maps.Keys(pool.voters)), "only the members inside the inclusion window stay")
}

// The aggregate endpoint also serves data the pool keeps aside: refused data and data displaced from
// the committee.
func (t *PoolTestSuite) TestGetAggregatationByRootAndCommitteeServesRefusedData() {
	pool := t.newPinTestPool()
	t.fillCommitteeWithTwoVoteData(pool)
	refusedVote := t.singleVote(0xbb, 8)
	t.Require().NoError(pool.AddAttestation(refusedVote))
	t.Require().NoError(pool.AddAttestation(t.singleVote(0xbb, 9)))
	refusedRoot, err := refusedVote.Data.HashSSZ()
	t.Require().NoError(err)

	att := pool.GetAggregatationByRootAndCommittee(refusedRoot, 5, 1)
	t.Require().NotNil(att)
	t.Equal([]byte{0, 0x83}, att.AggregationBits.Bytes())
	t.Nil(pool.GetAggregatationByRootAndCommittee(refusedRoot, 6, 1), "another slot")
	t.Nil(pool.GetAggregatationByRootAndCommittee(refusedRoot, 5, 2), "another committee")

	t.Require().NoError(pool.AddAttestation(t.singleVote(0xbb, 10)))
	displacedRoot, err := t.singleVote(0, 0).Data.HashSSZ()
	t.Require().NoError(err)
	att = pool.GetAggregatationByRootAndCommittee(displacedRoot, 5, 1)
	t.Require().NotNil(att, "the displaced data")
	t.Equal([]byte{0x03, 0x80}, att.AggregationBits.Bytes())
}

func (t *PoolTestSuite) refusedBits(pool *aggregationPoolImpl, source byte) []byte {
	for _, entry := range pool.refused[slotCommittee{slot: 5, committee: 1}] {
		if entry.attestation.Data.Source.Root[0] == source {
			return entry.attestation.AggregationBits.Bytes()
		}
	}
	return nil
}

func (t *PoolTestSuite) storedSources(pool AggregationPool) []byte {
	var sources []byte
	for _, att := range pool.Aggregates() {
		sources = append(sources, att.Data.Source.Root[0])
	}
	slices.Sort(sources)
	return sources
}

func (t *PoolTestSuite) refusedSources(pool *aggregationPoolImpl) []byte {
	var sources []byte
	for _, entry := range pool.refused[slotCommittee{slot: 5, committee: 1}] {
		sources = append(sources, entry.attestation.Data.Source.Root[0])
	}
	slices.Sort(sources)
	return sources
}

// Data with more voters replaces the weakest unpinned data, never the pinned data.
func (t *PoolTestSuite) TestAddAttestationElectraKeepsPinnedDataAgainstStrongerData() {
	pool := t.newPinTestPool()
	t.Require().NoError(pool.PinAttestationData(t.pinnedVote(0).Data))
	t.Require().NoError(pool.AddAttestation(t.pinnedVote(0)))
	member := 1
	for source := range maxDataPerCommittee - 1 {
		for range 2 {
			t.Require().NoError(pool.AddAttestation(t.singleVote(byte(source), member)))
			member++
		}
	}
	for range 3 {
		t.Require().NoError(pool.AddAttestation(t.singleVote(0xbb, member)))
		member++
	}

	t.NotNil(t.committeeAggregateBits(pool, 0xbb), "stronger data came in")
	t.Equal([]byte{0x01, 0x80}, t.committeeAggregateBits(pool, 0xaa), "the pinned data with one vote stayed")
}

func (t *PoolTestSuite) TestPruneStaleDropsPins() {
	t.mockEthClock.EXPECT().GetEpochAtSlot(gomock.Any()).DoAndReturn(func(slot uint64) uint64 { return slot / 32 }).AnyTimes()
	t.mockEthClock.EXPECT().GetCurrentSlot().Return(uint64(1000)).AnyTimes()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pool := NewAggregationPool(ctx, t.mockBeaconConfig, nil, t.mockEthClock).(*aggregationPoolImpl)
	t.Require().NoError(pool.PinAttestationData(t.pinnedVote(0).Data))
	t.Require().NoError(pool.PinAttestationData(&solid.AttestationData{Slot: 990, Source: attData1.Source, Target: attData1.Target}))

	pool.pruneStale()

	t.Len(pool.pinned, 1, "only the pin inside the inclusion window stays")
}

func (t *PoolTestSuite) TestAddAttestationElectraLimitsTotalAggregates() {
	t.mockEthClock.EXPECT().GetEpochAtSlot(gomock.Any()).Return(uint64(1)).AnyTimes()
	t.mockEthClock.EXPECT().StateVersionByEpoch(gomock.Any()).Return(clparams.ElectraVersion).AnyTimes()
	t.mockEthClock.EXPECT().GetCurrentSlot().Return(uint64(0)).AnyTimes()
	t.mockBeaconConfig.SlotsPerEpoch = 2
	t.mockBeaconConfig.MaxCommitteesPerSlot = 2
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pool := NewAggregationPool(ctx, t.mockBeaconConfig, nil, t.mockEthClock)
	limit := 2 * int(t.mockBeaconConfig.SlotsPerEpoch*t.mockBeaconConfig.MaxCommitteesPerSlot) * maxDataPerCommittee
	for i := range limit {
		t.Require().NoError(pool.AddAttestation(t.electraAttestationWithSource(uint64(i), 0, 1)))
	}
	t.ErrorIs(pool.AddAttestation(t.electraAttestationWithSource(uint64(limit), 0, 1)), ErrPoolLimit)
	t.Len(pool.Aggregates(), limit)
}

func TestPool(t *testing.T) {
	suite.Run(t, new(PoolTestSuite))
}

// A vote whose signature fails to decode evicts nothing: not from the committee, not from a full
// refused set, and not for pinned data.
func (t *PoolTestSuite) TestAddAttestationElectraKeepsFullCommitteeOnSignatureError() {
	for _, pinned := range []bool{false, true} {
		pool := t.newPinTestPool()
		t.fillCommitteeWithTwoVoteData(pool)
		for i := range maxDataPerCommittee {
			t.Require().NoError(pool.AddAttestation(t.singleVote(byte(0xb0+i), 8+i)))
		}
		stored, refused := t.storedSources(pool), t.refusedSources(pool)
		bad := t.singleVote(0xcc, 12)
		if pinned {
			t.Require().NoError(pool.PinAttestationData(bad.Data))
		}
		errBadSignature := errors.New("bad signature")
		newSignatureAggregator = func() signatureAggregator { return &fakeSignatureAggregator{failAll: errBadSignature} }

		t.ErrorIs(pool.AddAttestation(bad), errBadSignature, "pinned=%t", pinned)
		t.Equal(stored, t.storedSources(pool), "pinned=%t", pinned)
		t.Equal(refused, t.refusedSources(pool), "pinned=%t", pinned)
		newSignatureAggregator = func() signatureAggregator { return &fakeSignatureAggregator{} }
	}
}

// Stale entries are only swept once a minute; a full pool prunes them before refusing new data.
func (t *PoolTestSuite) TestAddAttestationElectraPrunesBeforeRefusingWhenFull() {
	t.mockEthClock.EXPECT().GetEpochAtSlot(gomock.Any()).DoAndReturn(func(slot uint64) uint64 { return slot / 2 }).AnyTimes()
	t.mockEthClock.EXPECT().StateVersionByEpoch(gomock.Any()).Return(clparams.ElectraVersion).AnyTimes()
	currentSlot := uint64(0)
	t.mockEthClock.EXPECT().GetCurrentSlot().DoAndReturn(func() uint64 { return currentSlot }).AnyTimes()
	t.mockBeaconConfig.SlotsPerEpoch = 2
	t.mockBeaconConfig.MaxCommitteesPerSlot = 2
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pool := NewAggregationPool(ctx, t.mockBeaconConfig, &clparams.NetworkConfig{AttestationPropagationSlotRange: 32}, t.mockEthClock)
	limit := 2 * int(t.mockBeaconConfig.SlotsPerEpoch*t.mockBeaconConfig.MaxCommitteesPerSlot) * maxDataPerCommittee
	for i := range limit {
		att := t.electraAttestationWithSource(uint64(i/8), (i/4)%2, byte(i%4))
		att.AggregationBits = solid.BitlistFromBytes([]byte{0x80 | 1<<(i%4)}, 2048*64) // one member per data
		t.Require().NoError(pool.AddAttestation(att))
	}
	currentSlot = 100 // the entries above are now outside the inclusion window
	t.NoError(pool.AddAttestation(t.electraAttestationWithSource(100, 0, 1)))
}
