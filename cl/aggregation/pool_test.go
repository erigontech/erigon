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
	"context"
	"errors"
	"log"
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
			att := pool.GetAggregatationByRootAndCommittee(tc.hashRoot, 10)
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
	merged := pool.GetAggregatationByRootAndCommittee(attData1Root, 10)
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
	cached := pool.GetAggregatationByRootAndCommittee(attData1Root, 10)
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
		att.AggregationBits = solid.BitlistFromBytes([]byte{0b111}, 2048*64) // two votes each
		t.Require().NoError(pool.AddAttestation(att))
	}
	t.NoError(pool.AddAttestation(t.electraAttestationWithSource(5, 1, 0xff)))
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

// Refused data is bounded per committee: new data replaces the refused data with the fewest voters
// only when it has at least as many.
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

// Among refused data with as many voters, the data that went longest without a new voter gives way.
func (t *PoolTestSuite) TestAddAttestationElectraEvictsTheStalestOfEquallyWeakRefusedData() {
	t.mockEthClock.EXPECT().GetEpochAtSlot(gomock.Any()).Return(uint64(1)).AnyTimes()
	t.mockEthClock.EXPECT().StateVersionByEpoch(gomock.Any()).Return(clparams.ElectraVersion).AnyTimes()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pool := NewAggregationPool(ctx, t.mockBeaconConfig, nil, t.mockEthClock).(*aggregationPoolImpl)
	t.fillCommitteeWithTwoVoteData(pool)
	for i := range maxDataPerCommittee {
		t.Require().NoError(pool.AddAttestation(t.singleVote(byte(0xb0+i), 8+i)))
	}

	t.Require().NoError(pool.AddAttestation(t.singleVote(0xcc, 12)))

	refused := pool.refused[slotCommittee{slot: 5, committee: 1}]
	t.Len(refused, maxDataPerCommittee)
	for _, entry := range refused {
		t.NotEqual(byte(0xb0), entry.attestation.Data.Source.Root[0], "the oldest single-vote data gave way")
	}
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

// A full committee makes room only for an attestation that is added; one whose signature cannot be
// decoded must not evict anything.
func (t *PoolTestSuite) TestAddAttestationElectraKeepsFullCommitteeOnSignatureError() {
	t.mockEthClock.EXPECT().GetEpochAtSlot(gomock.Any()).Return(uint64(1)).AnyTimes()
	t.mockEthClock.EXPECT().StateVersionByEpoch(gomock.Any()).Return(clparams.ElectraVersion).AnyTimes()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pool := NewAggregationPool(ctx, t.mockBeaconConfig, nil, t.mockEthClock)
	for source := range maxDataPerCommittee {
		t.Require().NoError(pool.AddAttestation(t.electraAttestationWithSource(5, 1, byte(source))))
	}
	errBadSignature := errors.New("bad signature")
	newSignatureAggregator = func() signatureAggregator { return &fakeSignatureAggregator{failAll: errBadSignature} }

	t.ErrorIs(pool.AddAttestation(t.electraAttestationWithSource(5, 1, 0xbb)), errBadSignature)
	t.Len(pool.Aggregates(), maxDataPerCommittee)
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
		t.Require().NoError(pool.AddAttestation(t.electraAttestationWithSource(uint64(i/8), (i/4)%2, byte(i%4))))
	}
	currentSlot = 100 // the entries above are now outside the inclusion window
	t.NoError(pool.AddAttestation(t.electraAttestationWithSource(100, 0, 1)))
}
