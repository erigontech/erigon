package solid

import (
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/types/clonable"
	"github.com/ledgerwatch/erigon-lib/types/ssz"
	"github.com/ledgerwatch/erigon/cl/merkle_tree"
	"github.com/ledgerwatch/erigon/common"
)

const (
	// agg bits offset: 4 bytes
	// attestationData: 128
	// Signature: 96 bytes
	attestationStaticBufferSize = 4 + attestationDataBufferSize + 96

	// offset is usually always the same
	aggregationBitsOffset = 228
)

// Attestation type represents a statement or confirmation of some occurrence or phenomenon.
type Attestation struct {
	// Statically sized fields (aggregation bits offset, attestation data, and signature)
	staticBuffer [attestationStaticBufferSize]byte
	// Dynamic field to store aggregation bits
	aggregationBitsBuffer []byte
}

// Static returns whether the attestation is static or not. For Attestation, it's always false.
func (*Attestation) Static() bool {
	return false
}

// NewAttestionFromParameters creates a new Attestation instance using provided parameters
func NewAttestionFromParameters(
	aggregationBits []byte,
	attestationData AttestationData,
	signature [96]byte,
) *Attestation {
	a := &Attestation{}
	a.SetAttestationData(attestationData)
	a.SetSignature(signature)
	a.SetAggregationBits(aggregationBits)
	return a
}

// AggregationBits returns the aggregation bits buffer of the Attestation instance.
func (a *Attestation) AggregationBits() []byte {
	return a.aggregationBitsBuffer
}

// SetAggregationBits sets the aggregation bits buffer of the Attestation instance.
func (a *Attestation) SetAggregationBits(bits []byte) {
	a.aggregationBitsBuffer = bits
}

// AttestantionData returns the attestation data of the Attestation instance.
func (a *Attestation) AttestantionData() AttestationData {
	return (AttestationData)(a.staticBuffer[4:132])
}

// Signature returns the signature of the Attestation instance.
func (a *Attestation) Signature() (o [96]byte) {
	copy(o[:], a.staticBuffer[132:228])
	return
}

// SetAttestationData sets the attestation data of the Attestation instance.
func (a *Attestation) SetAttestationData(d AttestationData) {
	copy(a.staticBuffer[4:132], d)
}

// SetSignature sets the signature of the Attestation instance.
func (a *Attestation) SetSignature(signature [96]byte) {
	copy(a.staticBuffer[132:], signature[:])
}

// EncodingSizeSSZ returns the size of the Attestation instance when encoded in SSZ format.
func (a *Attestation) EncodingSizeSSZ() (size int) {
	size = attestationStaticBufferSize
	if a == nil {
		return
	}
	return size + len(a.aggregationBitsBuffer)
}

// DecodeSSZ decodes the provided buffer into the Attestation instance.
func (a *Attestation) DecodeSSZ(buf []byte, _ int) error {
	if len(buf) < attestationStaticBufferSize {
		return ssz.ErrLowBufferSize
	}
	copy(a.staticBuffer[:], buf)
	a.aggregationBitsBuffer = common.CopyBytes(buf[aggregationBitsOffset:])
	return nil
}

// EncodeSSZ encodes the Attestation instance into the provided buffer.
func (a *Attestation) EncodeSSZ(dst []byte) ([]byte, error) {
	buf := dst
	buf = append(buf, a.staticBuffer[:]...)
	buf = append(buf, a.aggregationBitsBuffer...)
	return buf, nil
}

// CopyHashBufferTo copies the hash buffer of the Attestation instance to the provided byte slice.
func (a *Attestation) CopyHashBufferTo(o []byte) error {
	for i := 0; i < 128; i++ {
		o[i] = 0
	}
	aggBytesRoot, err := merkle_tree.BitlistRootWithLimit(a.AggregationBits(), 2048)
	if err != nil {
		return err
	}
	dataRoot, err := a.AttestantionData().HashSSZ()
	if err != nil {
		return err
	}
	copy(o[:128], a.staticBuffer[132:228])
	if err = merkle_tree.InPlaceRoot(o); err != nil {
		return err
	}
	copy(o[64:], o[:32])
	copy(o[:32], aggBytesRoot[:])
	copy(o[32:64], dataRoot[:])
	return nil
}

// HashSSZ hashes the Attestation instance using SSZ.
// It creates a byte slice `leaves` with a size based on length.Hash,
// then fills this slice with the values from the Attestation's hash buffer.
func (a *Attestation) HashSSZ() (o [32]byte, err error) {
	leaves := make([]byte, length.Hash*4)
	if err = a.CopyHashBufferTo(leaves); err != nil {
		return
	}
	err = merkle_tree.MerkleRootFromFlatLeaves(leaves, o[:])
	return
}

// Clone creates a new clone of the Attestation instance.
// This can be useful for creating copies without changing the original object.
func (*Attestation) Clone() clonable.Clonable {
	return &Attestation{}
}
