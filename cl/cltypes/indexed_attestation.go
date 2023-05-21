package cltypes

import (
	"fmt"

	"github.com/ledgerwatch/erigon-lib/types/ssz"

	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/merkle_tree"
	ssz2 "github.com/ledgerwatch/erigon/cl/ssz"
)

/*
 * IndexedAttestation are attestantions sets to prove that someone misbehaved.
 */
type IndexedAttestation struct {
	AttestingIndices solid.Uint64ListSSZ
	Data             solid.AttestationData
	Signature        [96]byte
}

func (i *IndexedAttestation) Static() bool {
	return false
}

func (i *IndexedAttestation) EncodeSSZ(buf []byte) (dst []byte, err error) {
	return ssz2.Encode(buf, i.AttestingIndices, i.Data, i.Signature[:])
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
	i.AttestingIndices = solid.NewUint64ListSSZ(2048)
	return i.AttestingIndices.DecodeSSZ(bitsBuf, version)
}

// EncodingSizeSSZ returns the ssz encoded size in bytes for the IndexedAttestation object
func (i *IndexedAttestation) EncodingSizeSSZ() int {
	return 228 + i.AttestingIndices.EncodingSizeSSZ()
}

// HashSSZ ssz hashes the IndexedAttestation object
func (i *IndexedAttestation) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(i.AttestingIndices, i.Data, i.Signature[:])
}

func IsSlashableAttestationData(d1, d2 solid.AttestationData) bool {
	return (!d1.Equal(d2) && d1.Target().Epoch() == d2.Target().Epoch()) ||
		(d1.Source().Epoch() < d2.Source().Epoch() && d2.Target().Epoch() < d1.Target().Epoch())
}
