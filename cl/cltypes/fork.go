package cltypes

import (
	"github.com/ledgerwatch/erigon/cl/merkle_tree"
	ssz2 "github.com/ledgerwatch/erigon/cl/ssz"
)

// Fork data, contains if we were on bellatrix/alteir/phase0 and transition epoch.
type Fork struct {
	PreviousVersion Bytes4
	CurrentVersion  Bytes4
	Epoch           uint64
}

func (*Fork) Static() bool {
	return true
}

func (f *Fork) Copy() *Fork {
	return &Fork{
		PreviousVersion: f.PreviousVersion,
		CurrentVersion:  f.CurrentVersion,
		Epoch:           f.Epoch,
	}
}

func (f *Fork) EncodeSSZ(dst []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(dst, f.PreviousVersion[:], f.CurrentVersion[:], f.Epoch)
}

func (f *Fork) DecodeSSZ(buf []byte, _ int) error {
	return ssz2.UnmarshalSSZ(buf, 0, f.PreviousVersion[:], f.CurrentVersion[:], &f.Epoch)

}

func (f *Fork) EncodingSizeSSZ() int {
	return 16
}

func (f *Fork) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(f.PreviousVersion[:], f.CurrentVersion[:], f.Epoch)
}
