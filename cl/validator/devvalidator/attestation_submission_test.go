package devvalidator

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/common"
)

func testAttData() *solid.AttestationData {
	return &solid.AttestationData{
		Slot:            5,
		CommitteeIndex:  0, // deprecated/zero from Electra onwards
		BeaconBlockRoot: common.Hash{0xaa},
		Source:          solid.Checkpoint{Epoch: 0, Root: common.Hash{0xbb}},
		Target:          solid.Checkpoint{Epoch: 0, Root: common.Hash{0xcc}},
	}
}

// TestBuildAttestationSubmission_Electra verifies that on Electra the dev
// validator submits a SingleAttestation to the V2 pool endpoint with the
// consensus-version header, and that the body decodes into exactly the type
// the beacon node's V2 handler expects ([]*solid.SingleAttestation).
func TestBuildAttestationSubmission_Electra(t *testing.T) {
	sig := common.Bytes96{0x01, 0x02, 0x03}
	sub := buildAttestationSubmission(clparams.ElectraVersion, 3, 7, testAttData(), sig, 4, 1)

	require.Equal(t, "/eth/v2/beacon/pool/attestations", sub.path)
	require.Equal(t, clparams.ElectraVersion.String(), sub.version)

	raw, err := json.Marshal(sub.body)
	require.NoError(t, err)

	var decoded []*solid.SingleAttestation
	require.NoError(t, json.Unmarshal(raw, &decoded))
	require.Len(t, decoded, 1)
	require.Equal(t, uint64(3), decoded[0].CommitteeIndex)
	require.Equal(t, uint64(7), decoded[0].AttesterIndex)
	require.NotNil(t, decoded[0].Data)
	require.Equal(t, uint64(5), decoded[0].Data.Slot)
	require.Equal(t, uint64(0), decoded[0].Data.CommitteeIndex)
	require.Equal(t, sig, decoded[0].Signature)
}

// TestBuildAttestationSubmission_PreElectra verifies the legacy V1 path still
// produces an aggregate the V1 handler accepts ([]*solid.Attestation) with
// exactly one aggregation bit set for the validator's committee position.
func TestBuildAttestationSubmission_PreElectra(t *testing.T) {
	sig := common.Bytes96{0x09}
	sub := buildAttestationSubmission(clparams.DenebVersion, 0, 7, testAttData(), sig, 4, 1)

	require.Equal(t, "/eth/v1/beacon/pool/attestations", sub.path)
	require.Empty(t, sub.version)

	raw, err := json.Marshal(sub.body)
	require.NoError(t, err)

	var decoded []*solid.Attestation
	require.NoError(t, json.Unmarshal(raw, &decoded))
	require.Len(t, decoded, 1)
	require.Equal(t, sig, decoded[0].Signature)
	require.Equal(t, uint64(5), decoded[0].Data.Slot)
}
