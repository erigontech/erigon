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

type Attestation struct {
	staticBuffer [attestationStaticBufferSize]byte // staticBuffer contains statically sized fields
	// Dynamic buffers
	aggregationBitsBuffer []byte
}

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

func (a *Attestation) AggregationBits() []byte {
	return a.aggregationBitsBuffer
}

func (a *Attestation) SetAggregationBits(bits []byte) {
	a.aggregationBitsBuffer = bits
}

func (a *Attestation) AttestantionData() AttestationData {
	return (AttestationData)(a.staticBuffer[4:132])
}

func (a *Attestation) Signature() (o [96]byte) {
	copy(o[:], a.staticBuffer[132:228])
	return
}

func (a *Attestation) SetAttestationData(d AttestationData) {
	copy(a.staticBuffer[4:132], d)
}

func (a *Attestation) SetSignature(signature [96]byte) {
	copy(a.staticBuffer[132:], signature[:])
}

func (a *Attestation) EncodingSizeSSZ() (size int) {
	size = attestationStaticBufferSize
	if a == nil {
		return
	}
	return size + len(a.aggregationBitsBuffer)
}

func (a *Attestation) DecodeSSZ(buf []byte, _ int) error {
	if len(buf) < attestationStaticBufferSize {
		return ssz.ErrLowBufferSize
	}
	copy(a.staticBuffer[:], buf)
	a.aggregationBitsBuffer = common.CopyBytes(buf[aggregationBitsOffset:])
	return nil
}

func (a *Attestation) EncodeSSZ(dst []byte) ([]byte, error) {
	buf := dst
	buf = append(buf, a.staticBuffer[:]...)
	buf = append(buf, a.aggregationBitsBuffer...)
	return buf, nil
}

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

func (a *Attestation) HashSSZ() (o [32]byte, err error) {
	leaves := make([]byte, length.Hash*4)
	if err = a.CopyHashBufferTo(leaves); err != nil {
		return
	}
	err = TreeHashFlatSlice(leaves, o[:])
	return
}

func (*Attestation) Clone() clonable.Clonable {
	return &Attestation{}
}
