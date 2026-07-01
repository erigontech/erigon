package devvalidator

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/common"
)

// TestBuildSyncCommitteeMessage verifies the submission body decodes into the
// exact type the beacon node's pool handler expects
// ([]*cltypes.SyncCommitteeMessage), with all fields preserved.
func TestBuildSyncCommitteeMessage(t *testing.T) {
	root := common.Hash{0xde, 0xad, 0xbe, 0xef}
	sig := common.Bytes96{0x11, 0x22, 0x33}

	body := []interface{}{buildSyncCommitteeMessage(42, root, 7, sig)}

	raw, err := json.Marshal(body)
	require.NoError(t, err)

	var decoded []*cltypes.SyncCommitteeMessage
	require.NoError(t, json.Unmarshal(raw, &decoded))
	require.Len(t, decoded, 1)
	require.Equal(t, uint64(42), decoded[0].Slot)
	require.Equal(t, root, decoded[0].BeaconBlockRoot)
	require.Equal(t, uint64(7), decoded[0].ValidatorIndex)
	require.Equal(t, sig, decoded[0].Signature)
}
