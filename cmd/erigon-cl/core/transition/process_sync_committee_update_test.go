package transition_test

import (
	"encoding/binary"
	"testing"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/state"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/transition"
	"github.com/ledgerwatch/erigon/common"
	"github.com/stretchr/testify/require"
)

func TestProcessSyncCommittee(t *testing.T) {
	pkBytes := common.Hex2Bytes("88c141df77cd9d8d7a71a75c826c41a9c9f03c6ee1b180f3e7852f6a280099ded351b58d66e653af8e42816a4d8f532e")
	var pk [48]byte
	copy(pk[:], pkBytes)
	validatorNum := 10_000
	state := state.GetEmptyBeaconState()
	currentCommittee := &cltypes.SyncCommittee{}
	nextCommittee := &cltypes.SyncCommittee{}
	for i := 0; i < validatorNum; i++ {
		var pubKey [48]byte
		binary.BigEndian.PutUint64(pubKey[:], uint64(i))
		v := &cltypes.Validator{}
		v.SetExitEpoch(clparams.MainnetBeaconConfig.FarFutureEpoch)
		v.SetPublicKey(pk)
		v.SetEffectiveBalance(2000000000)
		state.AddValidator(v, 2000000000)
		if len(currentCommittee.PubKeys) != cltypes.SyncCommitteeSize {
			currentCommittee.PubKeys = append(currentCommittee.PubKeys, [48]byte{})
		} else if len(nextCommittee.PubKeys) != cltypes.SyncCommitteeSize {
			nextCommittee.PubKeys = append(currentCommittee.PubKeys, [48]byte{})
		}
	}
	state.SetCurrentSyncCommittee(currentCommittee)
	state.SetNextSyncCommittee(nextCommittee)
	prevNextSyncCommittee := state.NextSyncCommittee()
	state.SetSlot(8160)
	require.NoError(t, transition.ProcessSyncCommitteeUpdate(state))
	require.Equal(t, state.CurrentSyncCommittee(), prevNextSyncCommittee)
	require.NotEqual(t, state.NextSyncCommittee(), prevNextSyncCommittee)
}
