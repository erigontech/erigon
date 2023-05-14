package cltypes

import (
	"errors"
	"fmt"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/types/ssz"

	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/merkle_tree"
)

type AttestationList struct {
	attestations []*solid.Attestation

	listRoot libcommon.Hash
}

func (a *AttestationList) DecodeSSZ(buf []byte, _ int) (err error) {
	a.attestations, err = ssz.DecodeDynamicList[*solid.Attestation](buf, 0, uint32(len(buf)), uint64(MaxAttestations), 0)
	if err != nil {
		return
	}
	a.listRoot = libcommon.Hash{}
	return
}

func (a *AttestationList) EncodeSSZ(buf []byte) (dst []byte, err error) {
	dst = buf
	dst, err = ssz.EncodeDynamicList(dst, a.attestations)
	if err != nil {
		return
	}
	return
}

func (a *AttestationList) Len() int {
	return len(a.attestations)
}

func (a *AttestationList) HashSSZ() ([32]byte, error) {
	if a.listRoot != (libcommon.Hash{}) {
		return a.listRoot, nil
	}
	var err error
	a.listRoot, err = merkle_tree.ListObjectSSZRoot(a.attestations, MaxAttestations)
	return a.listRoot, err
}

func (a *AttestationList) Get(index int) *solid.Attestation {
	return a.attestations[index]
}

func AttestionListFrom(atts ...*solid.Attestation) *AttestationList {
	a := &AttestationList{}
	a.attestations = atts
	a.listRoot = libcommon.Hash{}
	return a
}

func (a *AttestationList) ForEach(fn func(a *solid.Attestation, idx int, total int) bool) {
	if a == nil {
		return
	}
	for idx, v := range a.attestations {
		ok := fn(v, idx, len(a.attestations))
		if !ok {
			break
		}
	}
}

/*
 * IndexedAttestation are attestantions sets to prove that someone misbehaved.
 */
type IndexedAttestation struct {
	AttestingIndices []uint64
	Data             solid.AttestationData
	Signature        [96]byte
}

func (i *IndexedAttestation) EncodeSSZ(buf []byte) (dst []byte, err error) {
	dst = buf
	// Write indicies offset.
	dst = append(dst, ssz.OffsetSSZ(228)...)

	// Process data field.
	if dst, err = i.Data.EncodeSSZ(dst); err != nil {
		return
	}
	// Write signature
	dst = append(dst, i.Signature[:]...)

	// Field (0) 'AttestingIndices'
	if len(i.AttestingIndices) > 2048 {
		return nil, errors.New("too bing attesting indices")
	}
	for _, index := range i.AttestingIndices {
		dst = append(dst, ssz.Uint64SSZ(index)...)
	}

	return
}

// DecodeSSZ ssz unmarshals the IndexedAttestation object
func (i *IndexedAttestation) DecodeSSZ(buf []byte, version int) error {
	var err error
	size := uint64(len(buf))
	if size < 228 {
		return fmt.Errorf("[IndexedAttestation] err: %s", ssz.ErrLowBufferSize)
	}

	i.Data = solid.NewAttestationData()
	if err = i.Data.DecodeSSZ(buf[4:132], version); err != nil {
		return err
	}

	copy(i.Signature[:], buf[132:228])
	bitsBuf := buf[228:]
	num := len(bitsBuf) / 8
	if len(bitsBuf)%8 != 0 {
		return ssz.ErrBufferNotRounded
	}
	if num > 2048 {
		return ssz.ErrBadDynamicLength
	}
	i.AttestingIndices = make([]uint64, num)

	for index := 0; index < num; index++ {
		i.AttestingIndices[index] = ssz.UnmarshalUint64SSZ(bitsBuf[index*8:])
	}
	return nil
}

// EncodingSizeSSZ returns the ssz encoded size in bytes for the IndexedAttestation object
func (i *IndexedAttestation) EncodingSizeSSZ() int {
	return 228 + len(i.AttestingIndices)*8
}

// HashSSZ ssz hashes the IndexedAttestation object
func (i *IndexedAttestation) HashSSZ() ([32]byte, error) {
	leaves := make([][32]byte, 3)
	var err error
	leaves[0], err = merkle_tree.Uint64ListRootWithLimit(i.AttestingIndices, ssz.CalculateIndiciesLimit(2048, uint64(len(i.AttestingIndices)), 8))
	if err != nil {
		return [32]byte{}, err
	}

	leaves[1], err = i.Data.HashSSZ()
	if err != nil {
		return [32]byte{}, err
	}

	leaves[2], err = merkle_tree.SignatureRoot(i.Signature)
	if err != nil {
		return [32]byte{}, err
	}
	return merkle_tree.ArraysRoot(leaves, 4)
}

// Pending attestation. (only in Phase0 state)
type PendingAttestation struct {
	AggregationBits []byte
	Data            solid.AttestationData
	InclusionDelay  uint64
	ProposerIndex   uint64
}

// MarshalSSZTo ssz marshals the Attestation object to a target array
func (a *PendingAttestation) EncodeSSZ(buf []byte) (dst []byte, err error) {
	dst = buf
	dst = append(dst, ssz.OffsetSSZ(148)...)
	if dst, err = a.Data.EncodeSSZ(dst); err != nil {
		return
	}
	dst = append(dst, ssz.Uint64SSZ(a.InclusionDelay)...)
	dst = append(dst, ssz.Uint64SSZ(a.ProposerIndex)...)

	if len(a.AggregationBits) > 2048 {
		return nil, fmt.Errorf("too many aggregation bits in attestation")
	}
	dst = append(dst, a.AggregationBits...)

	return
}

// DecodeSSZ ssz unmarshals the Attestation object
func (a *PendingAttestation) DecodeSSZ(buf []byte, version int) error {
	var err error
	if len(buf) < a.EncodingSizeSSZ() {
		return fmt.Errorf("[PendingAttestation] err: %s", ssz.ErrLowBufferSize)
	}

	tail := buf

	// Field (1) 'Data'
	if a.Data == nil {
		a.Data = solid.NewAttestationData()
	}
	if err = a.Data.DecodeSSZ(buf[4:132], version); err != nil {
		return err
	}

	a.InclusionDelay = ssz.UnmarshalUint64SSZ(buf[132:])
	a.ProposerIndex = ssz.UnmarshalUint64SSZ(buf[140:])

	buf = tail[148:]

	if cap(a.AggregationBits) == 0 {
		a.AggregationBits = make([]byte, 0, len(buf))
	}
	a.AggregationBits = append(a.AggregationBits, buf...)

	return err
}

// EncodingSizeSSZ returns the ssz encoded size in bytes for the Attestation object
func (a *PendingAttestation) EncodingSizeSSZ() int {
	return 148 + len(a.AggregationBits)
}

// HashSSZ ssz hashes the Attestation object
func (a *PendingAttestation) HashSSZ() ([32]byte, error) {
	leaves := make([][32]byte, 4)
	var err error
	if a.Data == nil {
		return [32]byte{}, fmt.Errorf("missing attestation data")
	}
	leaves[0], err = merkle_tree.BitlistRootWithLimit(a.AggregationBits, 2048)
	if err != nil {
		return [32]byte{}, err
	}

	leaves[1], err = a.Data.HashSSZ()
	if err != nil {
		return [32]byte{}, err
	}

	leaves[2] = merkle_tree.Uint64Root(a.InclusionDelay)
	leaves[3] = merkle_tree.Uint64Root(a.ProposerIndex)

	return merkle_tree.ArraysRoot(leaves, 4)
}

func IsSlashableAttestationData(d1, d2 solid.AttestationData) bool {
	return (!d1.Equal(d2) && d1.Target().Epoch() == d2.Target().Epoch()) ||
		(d1.Source().Epoch() < d2.Source().Epoch() && d2.Target().Epoch() < d1.Target().Epoch())
}
