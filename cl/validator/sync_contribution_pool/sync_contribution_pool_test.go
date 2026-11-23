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

package sync_contribution_pool

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/cl/utils/bls"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/hexutil"
)

var testHash = common.Hash{0x01, 0x02, 0x03, 0x04}

func getTestCommitteesMessages(n int) (privateKeys [][]byte, messages []cltypes.SyncCommitteeMessage, s *state.CachingBeaconState) {
	s = state.New(&clparams.MainnetBeaconConfig)
	for i := range n {
		privateKey, err := bls.GenerateKey()
		if err != nil {
			panic(err)
		}
		privateKeys = append(privateKeys, privateKey.Bytes())
		domain, err := s.GetDomain(s.BeaconConfig().DomainSyncCommittee, 0)
		if err != nil {
			panic(err)
		}
		msg := crypto.Sha256(testHash[:], domain)
		sig := privateKey.Sign(msg[:])
		message := cltypes.SyncCommitteeMessage{
			BeaconBlockRoot: testHash,
			ValidatorIndex:  uint64(i),
			Signature:       common.Bytes96(sig.Bytes()),
		}
		messages = append(messages, message)
		v := solid.NewValidator()
		v.SetPublicKey(common.Bytes48(bls.CompressPublicKey(privateKey.PublicKey())))
		s.AppendValidator(v)
		currCommittee := s.CurrentSyncCommittee()
		c := currCommittee.GetCommittee()
		c[i] = common.Bytes48(bls.CompressPublicKey(privateKey.PublicKey()))
		currCommittee.SetCommittee(c)
		if err := s.SetCurrentSyncCommittee(currCommittee); err != nil {
			panic(err)
		}
	}
	return
}

func TestSyncContributionPool(t *testing.T) {
	_, msgs, s := getTestCommitteesMessages(16)
	pool := NewSyncContributionPool(&clparams.MainnetBeaconConfig)
	require.NoError(t, pool.AddSyncCommitteeMessage(s, 0, &msgs[0]))
	require.NoError(t, pool.AddSyncCommitteeMessage(s, 0, &msgs[1]))
	require.NoError(t, pool.AddSyncCommitteeMessage(s, 0, &msgs[2]))

	contribution := pool.GetSyncContribution(0, 0, testHash)
	require.NotEqual(t, contribution.Signature, common.Bytes96(bls.InfiniteSignature))

	contribution.SubcommitteeIndex = 1
	require.NoError(t, pool.AddSyncContribution(s, contribution))
}

// The next committee signs for the last slot of a period; the head may not have imported that slot yet.
func TestSyncContributionPoolAggregatesTheLastSlotOfAPeriodBeforeItsBlock(t *testing.T) {
	_, msgs, s := getTestCommitteesMessages(1)
	cfg := s.BeaconConfig()
	nextPeriod := cfg.SlotsPerEpoch * cfg.EpochsPerSyncCommitteePeriod
	require.NoError(t, s.SetSlot(nextPeriod-2))
	// Move the member from the current committee to the next one.
	current, next := s.CurrentSyncCommittee().Copy(), s.NextSyncCommittee().Copy()
	currentKeys, nextKeys := current.GetCommittee(), next.GetCommittee()
	nextKeys[0], currentKeys[0] = currentKeys[0], common.Bytes48{}
	current.SetCommittee(currentKeys)
	next.SetCommittee(nextKeys)
	require.NoError(t, s.SetCurrentSyncCommittee(current))
	require.NoError(t, s.SetNextSyncCommittee(next))

	msg := msgs[0]
	msg.Slot = nextPeriod - 1
	pool := NewSyncContributionPool(cfg)
	require.NoError(t, pool.AddSyncCommitteeMessage(s, 0, &msg))
	contribution := pool.GetSyncContribution(msg.Slot, 0, testHash)
	require.Equal(t, msg.Signature, contribution.Signature)
}

// The pool keeps contributions from the head's slot to one epoch after it: after empty slots the
// next proposer and aggregators read slots well past the head, and older ones can no longer be
// aggregated. A stalled head still bounds what is kept.
func TestSyncContributionPoolKeepsOneEpochFromTheHead(t *testing.T) {
	_, msgs, s := getTestCommitteesMessages(1)
	const headSlot = 10
	require.NoError(t, s.SetSlot(headSlot))
	epoch := s.BeaconConfig().SlotsPerEpoch
	pool := NewSyncContributionPool(s.BeaconConfig())
	kept := map[uint64]bool{
		headSlot - 1:         false,
		headSlot:             true,
		headSlot + 2:         true, // two empty slots after the head
		headSlot + epoch:     true,
		headSlot + epoch + 1: false,
	}
	for slot := range kept {
		msg := msgs[0]
		msg.Slot = slot
		require.NoError(t, pool.AddSyncCommitteeMessage(s, 0, &msg))
	}

	for slot, want := range kept {
		contribution := pool.GetSyncContribution(slot, 0, testHash)
		if want {
			require.Equal(t, msgs[0].Signature, contribution.Signature, "slot %d", slot)
		} else {
			require.Equal(t, common.Bytes96(bls.InfiniteSignature), contribution.Signature, "slot %d", slot)
		}
	}
}

// The proposer reads the contributions for blocks, which are pruned by the same window.
func TestSyncContributionPoolKeepsOneEpochFromTheHeadForBlocks(t *testing.T) {
	_, msgs, s := getTestCommitteesMessages(1)
	const headSlot = 10
	require.NoError(t, s.SetSlot(headSlot))
	epoch := s.BeaconConfig().SlotsPerEpoch
	pool := NewSyncContributionPool(s.BeaconConfig())
	kept := map[uint64]bool{
		headSlot - 1:         false,
		headSlot:             true,
		headSlot + 2:         true,
		headSlot + epoch:     true,
		headSlot + epoch + 1: false,
	}
	msg := msgs[0]
	msg.Slot = headSlot
	require.NoError(t, pool.AddSyncCommitteeMessage(s, 0, &msg))
	contribution := pool.GetSyncContribution(headSlot, 0, testHash)
	for slot := range kept {
		c := contribution.Copy()
		c.Slot = slot
		require.NoError(t, pool.AddSyncContribution(s, c))
	}
	next := msgs[0]
	next.Slot = headSlot + 1
	require.NoError(t, pool.AddSyncCommitteeMessage(s, 0, &next)) // runs the cleanup

	for slot, want := range kept {
		aggregate, err := pool.GetSyncAggregate(slot, testHash)
		require.NoError(t, err)
		require.Equal(t, want, utils.BitsOnCount(aggregate.SyncCommiteeBits) > 0, "slot %d", slot)
	}
}

func TestSyncContributionPoolSignatureIsAggregateOfAddedMessages(t *testing.T) {
	_, msgs, s := getTestCommitteesMessages(6)
	// A validator's signature is aggregated once per occupied committee position.
	current := s.CurrentSyncCommittee().Copy()
	keys := current.GetCommittee()
	keys[20] = keys[3]
	current.SetCommittee(keys)
	require.NoError(t, s.SetCurrentSyncCommittee(current))

	pool := NewSyncContributionPool(&clparams.MainnetBeaconConfig)
	var wantSignatures [][]byte
	wantBits := make([]byte, 16)
	for _, step := range []struct {
		validator int
		positions []int
	}{
		{0, []int{0}},
		{3, []int{3, 20}},
		{1, []int{1}},
		{3, nil},
		{5, []int{5}},
		{2, []int{2}},
	} {
		require.NoError(t, pool.AddSyncCommitteeMessage(s, 0, &msgs[step.validator]))
		for _, position := range step.positions {
			utils.FlipBitOn(wantBits, position)
			wantSignatures = append(wantSignatures, msgs[step.validator].Signature[:])
		}
		wantSignature, err := bls.AggregateSignatures(wantSignatures)
		require.NoError(t, err)

		contribution := pool.GetSyncContribution(0, 0, testHash)
		require.Equal(t, hexutil.Bytes(wantBits), contribution.AggregationBits, "after validator %d", step.validator)
		require.Equal(t, common.Bytes96(wantSignature), contribution.Signature, "after validator %d", step.validator)
	}
}

func TestSyncContributionPoolRejectsMalformedSignatureWithoutChangingContribution(t *testing.T) {
	_, msgs, s := getTestCommitteesMessages(2)
	pool := NewSyncContributionPool(&clparams.MainnetBeaconConfig)
	require.NoError(t, pool.AddSyncCommitteeMessage(s, 0, &msgs[0]))

	malformed := msgs[1]
	for i := range malformed.Signature {
		malformed.Signature[i] = 0xFF
	}
	require.Error(t, pool.AddSyncCommitteeMessage(s, 0, &malformed))
	contribution := pool.GetSyncContribution(0, 0, testHash)
	require.Equal(t, 1, utils.BitsOnCount(contribution.AggregationBits))
	require.Equal(t, msgs[0].Signature, contribution.Signature)

	require.NoError(t, pool.AddSyncCommitteeMessage(s, 0, &msgs[1]))
	contribution = pool.GetSyncContribution(0, 0, testHash)
	require.Equal(t, 2, utils.BitsOnCount(contribution.AggregationBits))
	wantSignature, err := bls.AggregateSignatures([][]byte{msgs[0].Signature[:], msgs[1].Signature[:]})
	require.NoError(t, err)
	require.Equal(t, common.Bytes96(wantSignature), contribution.Signature)
}

func TestSyncContributionPoolCleanupDropsRunningSignature(t *testing.T) {
	_, msgs, s := getTestCommitteesMessages(2)
	const staleSlot = 10
	epoch := s.BeaconConfig().SlotsPerEpoch
	pool := NewSyncContributionPool(s.BeaconConfig())

	require.NoError(t, s.SetSlot(staleSlot))
	stale := msgs[0]
	stale.Slot = staleSlot
	require.NoError(t, pool.AddSyncCommitteeMessage(s, 0, &stale))

	require.NoError(t, s.SetSlot(staleSlot+epoch+1))
	later := msgs[1]
	later.Slot = staleSlot + epoch + 1
	require.NoError(t, pool.AddSyncCommitteeMessage(s, 0, &later))
	require.Zero(t, utils.BitsOnCount(pool.GetSyncContribution(staleSlot, 0, testHash).AggregationBits))

	require.NoError(t, s.SetSlot(staleSlot))
	again := msgs[1]
	again.Slot = staleSlot
	require.NoError(t, pool.AddSyncCommitteeMessage(s, 0, &again))
	contribution := pool.GetSyncContribution(staleSlot, 0, testHash)
	require.Equal(t, 1, utils.BitsOnCount(contribution.AggregationBits))
	require.Equal(t, msgs[1].Signature, contribution.Signature)
}
