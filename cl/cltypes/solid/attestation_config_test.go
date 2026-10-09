package solid

import (
	"encoding/binary"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/common/ssz"
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

func TestAttestationDecodeSSZStrictRejectsOffsetGap(t *testing.T) {
	tests := []struct {
		name    string
		version clparams.StateVersion
		value   *Attestation
	}{
		{
			name:    "deneb",
			version: clparams.DenebVersion,
			value: &Attestation{
				AggregationBits: BitlistFromBytes([]byte{1}, maxValidatorsPerCommittee),
				Data:            &AttestationData{},
			},
		},
		{
			name:    "gloas",
			version: clparams.GloasVersion,
			value: &Attestation{
				AggregationBits: BitlistFromBytes([]byte{1}, aggregationBitsSizeElectra),
				Data:            &AttestationData{},
				CommitteeBits:   NewBitVector(int(clparams.MainnetBeaconConfig.MaxCommitteesPerSlot)),
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			encoded, err := test.value.EncodeSSZ(nil)
			require.NoError(t, err)
			require.NoError(t, new(Attestation).DecodeSSZStrictWithConfig(encoded, int(test.version), &clparams.MainnetBeaconConfig))
			fixedSize := int(binary.LittleEndian.Uint32(encoded[:4]))
			malformed := append([]byte(nil), encoded[:fixedSize]...)
			malformed = append(malformed, make([]byte, 4)...)
			malformed = append(malformed, encoded[fixedSize:]...)
			binary.LittleEndian.PutUint32(malformed, uint32(fixedSize+4))
			require.Error(t, new(Attestation).DecodeSSZStrictWithConfig(malformed, int(test.version), &clparams.MainnetBeaconConfig))
		})
	}
}

func TestAttestationDecodeSSZRejectsOffsetPastBufferBeforeAllocation(t *testing.T) {
	const fixedHeaderSize = 4 + AttestationDataSize + 96
	buf := make([]byte, fixedHeaderSize+1)
	binary.LittleEndian.PutUint32(buf[:4], 1<<20)

	for _, strict := range []bool{false, true} {
		t.Run(map[bool]string{false: "permissive", true: "strict"}[strict], func(t *testing.T) {
			decoded := new(Attestation)
			var err error
			if strict {
				err = decoded.DecodeSSZStrict(buf, int(clparams.GloasVersion))
			} else {
				err = decoded.DecodeSSZ(buf, int(clparams.GloasVersion))
			}
			require.True(t, errors.Is(err, ssz.ErrBadOffset), err)
			require.Nil(t, decoded.CommitteeBits)
		})
	}
}

func TestAttestationDecodeSSZWithoutConfigBoundsCommitteeBitsByMainnetWidth(t *testing.T) {
	mainnetCommittees := int(clparams.MainnetBeaconConfig.MaxCommitteesPerSlot)
	tests := []struct {
		name       string
		committees int
		wantErr    bool
	}{
		{name: "minimal preset", committees: 4},
		{name: "mainnet preset", committees: mainnetCommittees},
		{name: "one byte wider than mainnet", committees: mainnetCommittees + 8, wantErr: true},
	}
	for _, test := range tests {
		for _, strict := range []bool{false, true} {
			t.Run(test.name+map[bool]string{false: " permissive", true: " strict"}[strict], func(t *testing.T) {
				attestation := &Attestation{
					AggregationBits: BitlistFromBytes([]byte{1}, aggregationBitsSizeElectra),
					Data:            &AttestationData{},
					CommitteeBits:   NewBitVector(test.committees),
				}
				encoded, err := attestation.EncodeSSZ(nil)
				require.NoError(t, err)

				decoded := new(Attestation)
				if strict {
					err = decoded.DecodeSSZStrict(encoded, int(clparams.GloasVersion))
				} else {
					err = decoded.DecodeSSZ(encoded, int(clparams.GloasVersion))
				}
				if !test.wantErr {
					require.NoError(t, err)
					require.Equal(t, (test.committees+7)/8, decoded.CommitteeBits.EncodingSizeSSZ())
					return
				}
				require.ErrorIs(t, err, errCommitteeBitsTooWide)
				require.Nil(t, decoded.CommitteeBits)
				require.Nil(t, decoded.AggregationBits)
			})
		}
	}
}
