package solid

import (
	"encoding/binary"
	"encoding/json"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"

	"github.com/ledgerwatch/erigon-lib/types/clonable"
	"github.com/ledgerwatch/erigon-lib/types/ssz"
	"github.com/ledgerwatch/erigon/cl/merkle_tree"
)

const (
	// agg bits offset: 4 bytes
	// attestationData: 128
	// InclusionDelay: 8 bytes
	// ProposerIndex: 8 bytes
	pendingAttestationStaticBufferSize = 4 + AttestationDataBufferSize + 8 + 8

	// offset is usually always the same
	pendingAggregationBitsOffset = 148
)

type PendingAttestation struct {
	staticBuffer [pendingAttestationStaticBufferSize]byte // staticBuffer contains statically sized fields
	// Dynamic buffers
	aggregationBitsBuffer []byte
}

func NewPendingAttestionFromParameters(
	aggregationBits []byte,
	attestationData AttestationData,
	inclusionDelay uint64,
	proposerIndex uint64,
) *PendingAttestation {
	a := &PendingAttestation{}
	a.SetAggregationBits(aggregationBits)
	a.SetAttestationData(attestationData)
	a.SetInclusionDelay(inclusionDelay)
	a.SetProposerIndex(proposerIndex)
	return a
}

func (a *PendingAttestation) AggregationBits() []byte {
	return a.aggregationBitsBuffer
}

func (a *PendingAttestation) SetAggregationBits(bits []byte) {
	a.aggregationBitsBuffer = bits
}

func (a *PendingAttestation) AttestantionData() AttestationData {
	return (AttestationData)(a.staticBuffer[4:132])
}

func (a *PendingAttestation) InclusionDelay() uint64 {
	return binary.LittleEndian.Uint64(a.staticBuffer[132:140])
}

func (a *PendingAttestation) SetAttestationData(d AttestationData) {
	copy(a.staticBuffer[4:132], d)
}

func (a *PendingAttestation) SetInclusionDelay(inclusionDelay uint64) {
	binary.LittleEndian.PutUint64(a.staticBuffer[132:140], inclusionDelay)
}

func (a *PendingAttestation) SetProposerIndex(proposerIndex uint64) {
	binary.LittleEndian.PutUint64(a.staticBuffer[140:], proposerIndex)
}

func (a *PendingAttestation) ProposerIndex() uint64 {
	return binary.LittleEndian.Uint64(a.staticBuffer[140:])
}

func (a *PendingAttestation) EncodingSizeSSZ() (size int) {
	size = pendingAttestationStaticBufferSize
	if a == nil {
		return
	}
	return size + len(a.aggregationBitsBuffer)
}

func (a *PendingAttestation) DecodeSSZ(buf []byte, _ int) error {
	if len(buf) < pendingAttestationStaticBufferSize {
		return ssz.ErrLowBufferSize
	}
	copy(a.staticBuffer[:], buf)
	a.aggregationBitsBuffer = common.CopyBytes(buf[pendingAggregationBitsOffset:])
	return nil
}

func (a *PendingAttestation) EncodeSSZ(dst []byte) ([]byte, error) {
	buf := dst
	buf = append(buf, a.staticBuffer[:]...)
	buf = append(buf, a.aggregationBitsBuffer...)
	return buf, nil
}

func (a *PendingAttestation) HashSSZ() (o [32]byte, err error) {
	bitsRoot, err := merkle_tree.BitlistRootWithLimit(a.AggregationBits(), 2048)
	if err != nil {
		return [32]byte{}, err
	}
	return merkle_tree.HashTreeRoot(bitsRoot[:], a.AttestantionData(), a.InclusionDelay(), a.ProposerIndex())
}

func (*PendingAttestation) Clone() clonable.Clonable {
	return &PendingAttestation{}
}

func (a *PendingAttestation) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		AggregationBits hexutility.Bytes `json:"aggregation_bits"`
		AttestationData AttestationData  `json:"attestation_data"`
		InclusionDelay  uint64           `json:"inclusion_delay,string"`
		ProposerIndex   uint64           `json:"proposer_index,string"`
	}{
		AggregationBits: a.AggregationBits(),
		AttestationData: a.AttestantionData(),
		InclusionDelay:  a.InclusionDelay(),
		ProposerIndex:   a.ProposerIndex(),
	})
}

func (a *PendingAttestation) UnmarshalJSON(input []byte) error {
	var err error
	var tmp struct {
		AggregationBits hexutility.Bytes `json:"aggregation_bits"`
		AttestationData AttestationData  `json:"attestation_data"`
		InclusionDelay  uint64           `json:"inclusion_delay,string"`
		ProposerIndex   uint64           `json:"proposer_index,string"`
	}
	tmp.AttestationData = NewAttestationData()

	if err = json.Unmarshal(input, &tmp); err != nil {
		return err
	}
	a.SetAggregationBits(tmp.AggregationBits)
	a.SetAttestationData(tmp.AttestationData)
	a.SetInclusionDelay(tmp.InclusionDelay)
	a.SetProposerIndex(tmp.ProposerIndex)
	return nil
}
