package solid

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPendingAttestation(t *testing.T) {
	// Create sample data
	aggregationBits := []byte{1, 0, 1, 0}
	attestationData := AttestationData{1, 2, 3}
	inclusionDelay := uint64(10)
	proposerIndex := uint64(20)

	// Test NewPendingAttestionFromParameters
	pendingAttestation := NewPendingAttestionFromParameters(aggregationBits, attestationData, inclusionDelay, proposerIndex)
	assert.NotNil(t, pendingAttestation)
	assert.Equal(t, aggregationBits, pendingAttestation.AggregationBits())
	assert.Equal(t, inclusionDelay, pendingAttestation.InclusionDelay())
	assert.Equal(t, proposerIndex, pendingAttestation.ProposerIndex())

	// Test EncodingSizeSSZ
	expectedEncodingSize := pendingAttestationStaticBufferSize + len(aggregationBits)
	encodingSize := pendingAttestation.EncodingSizeSSZ()
	assert.Equal(t, expectedEncodingSize, encodingSize)

	// Test EncodeSSZ and DecodeSSZ
	encodedData, err := pendingAttestation.EncodeSSZ(nil)
	assert.NoError(t, err)
	decodedPendingAttestation := &PendingAttestation{}
	err = decodedPendingAttestation.DecodeSSZ(encodedData, encodingSize)
	assert.NoError(t, err)
	assert.Equal(t, pendingAttestation, decodedPendingAttestation)
}
