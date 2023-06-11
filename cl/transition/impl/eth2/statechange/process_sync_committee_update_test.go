package statechange_test

import (
	"encoding/binary"
	"github.com/ledgerwatch/erigon/cl/transition/impl/eth2/statechange"
	"testing"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
	"github.com/ledgerwatch/erigon/common"
	"github.com/stretchr/testify/require"
)

func TestProcessSyncCommittee(t *testing.T) {
	pkBytes := common.Hex2Bytes("88c141df77cd9d8d7a71a75c826c41a9c9f03c6ee1b180f3e7852f6a280099ded351b58d66e653af8e42816a4d8f532e")
	var pk [48]byte
	copy(pk[:], pkBytes)
	validatorNum := 10_000
	s := state.New(&clparams.MainnetBeaconConfig)
	currentCommittee := &solid.SyncCommittee{}
	nextCommittee := &solid.SyncCommittee{}
	for i := 0; i < validatorNum; i++ {
		var pubKey [48]byte
		binary.BigEndian.PutUint64(pubKey[:], uint64(i))
		v := solid.NewValidator()
		v.SetExitEpoch(clparams.MainnetBeaconConfig.FarFutureEpoch)
		v.SetPublicKey(pk)
		v.SetEffectiveBalance(2000000000)
		s.AddValidator(v, 2000000000)
	}
	s.SetCurrentSyncCommittee(currentCommittee)
	s.SetNextSyncCommittee(nextCommittee)
	prevNextSyncCommittee := s.NextSyncCommittee()
	s.SetSlot(8160)
	require.NoError(t, statechange.ProcessSyncCommitteeUpdate(s))
	require.Equal(t, s.CurrentSyncCommittee(), prevNextSyncCommittee)
	require.NotEqual(t, s.NextSyncCommittee(), prevNextSyncCommittee)
}
