package solid

import (
	"testing"

	"github.com/ledgerwatch/erigon-lib/common"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/stretchr/testify/assert"
)

func TestSyncCommittee(t *testing.T) {
	// Test NewSyncCommitteeFromParameters
	committee := make([][48]byte, 512)
	aggregatePublicKey := [48]byte{1, 2, 3} // Example aggregate public key
	syncCommittee := NewSyncCommitteeFromParameters(committee, aggregatePublicKey)
	assert.NotNil(t, syncCommittee)

	// Test GetCommittee
	gotCommittee := syncCommittee.GetCommittee()
	assert.Equal(t, committee, gotCommittee)

	// Test SetCommittee
	newCommittee := make([][48]byte, 512)
	for i := 0; i < 512; i++ {
		copy(newCommittee[i][:], []byte{byte(i)})
	}
	syncCommittee.SetCommittee(newCommittee)
	updatedCommittee := syncCommittee.GetCommittee()
	assert.Equal(t, newCommittee, updatedCommittee)

	// Test AggregatePublicKey
	gotAggregatePublicKey := syncCommittee.AggregatePublicKey()
	assert.Equal(t, aggregatePublicKey, gotAggregatePublicKey)

	// Test SetAggregatePublicKey
	newAggregatePublicKey := [48]byte{4, 5, 6} // Example new aggregate public key
	syncCommittee.SetAggregatePublicKey(newAggregatePublicKey)
	updatedAggregatePublicKey := syncCommittee.AggregatePublicKey()
	assert.Equal(t, newAggregatePublicKey, updatedAggregatePublicKey)

	// Test EncodingSizeSSZ
	expectedEncodingSize := syncCommitteeSize
	encodingSize := syncCommittee.EncodingSizeSSZ()
	assert.Equal(t, expectedEncodingSize, encodingSize)

	// Test EncodeSSZ and DecodeSSZ
	encodedData, err := syncCommittee.EncodeSSZ(nil)
	assert.NoError(t, err)
	decodedSyncCommittee := &SyncCommittee{}
	err = decodedSyncCommittee.DecodeSSZ(encodedData, encodingSize)
	assert.NoError(t, err)
	assert.Equal(t, syncCommittee, decodedSyncCommittee)

	// Test Clone
	clone := syncCommittee.Clone().(*SyncCommittee)
	assert.NotEqual(t, nil, clone)

	// Test Copy
	copy := syncCommittee.Copy()
	assert.Equal(t, syncCommittee, copy)

	// Test Equal
	otherSyncCommittee := &SyncCommittee{}
	assert.False(t, syncCommittee.Equal(otherSyncCommittee))
	assert.True(t, syncCommittee.Equal(syncCommittee))

	// Test HashSSZ
	expectedRoot := common.HexToHash("28628f3f10fa1070f2a42aeeeae792cd6ded1ef81030104e765e1498a1cfcfbd") // Example expected root
	root, err := syncCommittee.HashSSZ()
	assert.NoError(t, err)
	assert.Equal(t, expectedRoot, libcommon.Hash(root))

	// Test Static
	assert.True(t, syncCommittee.Static())
}
