package sync_contribution_pool

import (
	"testing"

	"github.com/Giulio2002/bls"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
	"github.com/ledgerwatch/erigon/cl/utils"
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
