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

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/cl/utils/bls"
	"github.com/stretchr/testify/require"
)

var testHash = common.Hash{0x01, 0x02, 0x03, 0x04}

func getTestCommitteesMessages(n int) (privateKeys [][]byte, messages []cltypes.SyncCommitteeMessage, s *state.CachingBeaconState) {
	s = state.New(&clparams.MainnetBeaconConfig)
	for i := 0; i < n; i++ {
		privateKey, err := bls.GenerateKey()
		if err != nil {
			panic(err)
		}
		privateKeys = append(privateKeys, privateKey.Bytes())
		domain, err := s.GetDomain(s.BeaconConfig().DomainSyncCommittee, 0)
		if err != nil {
			panic(err)
		}
		msg := utils.Sha256(testHash[:], domain)
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
		s.SetCurrentSyncCommittee(currCommittee)
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
