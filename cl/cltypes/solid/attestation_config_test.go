package solid

import (
	"encoding/binary"
	"testing"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/stretchr/testify/require"
)

func TestAttestationDecodeSSZWithConfigRejectsWrongCommitteeBitsSize(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.MaxCommitteesPerSlot = 4
	attestation := &Attestation{
		AggregationBits: BitlistFromBytes([]byte{1}, int(cfg.MaxCommitteesPerSlot)*maxValidatorsPerCommittee),
		Data:            &AttestationData{},
		CommitteeBits:   NewBitVector(int(cfg.MaxCommitteesPerSlot)),
	}
	encoded, err := attestation.EncodeSSZ(nil)
	require.NoError(t, err)
	offset := int(binary.LittleEndian.Uint32(encoded[:4]))
	malformed := append([]byte(nil), encoded[:offset]...)
	malformed = append(malformed, 0)
	malformed = append(malformed, encoded[offset:]...)
	binary.LittleEndian.PutUint32(malformed[:4], uint32(offset+1))

	decoded := &Attestation{}
	require.Error(t, decoded.DecodeSSZWithConfig(malformed, int(clparams.GloasVersion), &cfg))
}

func TestAttestationValidateForConfigNormalizesJSONCommitteeBits(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.MaxCommitteesPerSlot = 4
	committeeBits := &BitVector{}
	require.NoError(t, committeeBits.UnmarshalJSON([]byte(`"0x01"`)))
	attestation := &Attestation{
		AggregationBits: BitlistFromBytes([]byte{1}, int(cfg.MaxCommitteesPerSlot)*maxValidatorsPerCommittee),
		Data:            &AttestationData{},
		CommitteeBits:   committeeBits,
	}

	require.NoError(t, attestation.ValidateForConfig(&cfg, clparams.GloasVersion))
	require.Equal(t, 4, attestation.CommitteeBits.BitCap())

	tooLong := &BitVector{}
	require.NoError(t, tooLong.UnmarshalJSON([]byte(`"0x0100"`)))
	attestation.CommitteeBits = tooLong
	require.Error(t, attestation.ValidateForConfig(&cfg, clparams.GloasVersion))
}
