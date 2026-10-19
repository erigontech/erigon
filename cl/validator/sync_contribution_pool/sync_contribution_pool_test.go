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
