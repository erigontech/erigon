package cltypes

import (
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes/ssz"
	"github.com/ledgerwatch/erigon/cl/merkle_tree"
)

// Fork data, contains if we were on bellatrix/alteir/phase0 and transition epoch.
type Fork struct {
	PreviousVersion [4]byte
	CurrentVersion  [4]byte
	Epoch           uint64
}

func (f *Fork) EncodeSSZ(dst []byte) ([]byte, error) {
	buf := dst
	buf = append(buf, f.PreviousVersion[:]...)
	buf = append(buf, f.CurrentVersion[:]...)
	buf = append(buf, ssz.Uint64SSZ(f.Epoch)...)
	return buf, nil
}

func (f *Fork) DecodeSSZ(buf []byte) error {
	if len(buf) < f.EncodingSizeSSZ() {
		return ssz.ErrLowBufferSize
	}
	copy(f.PreviousVersion[:], buf)
	copy(f.CurrentVersion[:], buf[clparams.VersionLength:])
	f.Epoch = ssz.UnmarshalUint64SSZ(buf[clparams.VersionLength*2:])
	return nil
}

func (f *Fork) EncodingSizeSSZ() int {
	return clparams.VersionLength*2 + 8
}

func (f *Fork) HashSSZ() ([32]byte, error) {
	leaves := make([][32]byte, 3)
	copy(leaves[0][:], f.PreviousVersion[:])
	copy(leaves[1][:], f.CurrentVersion[:])
	leaves[2] = merkle_tree.Uint64Root(f.Epoch)
	return merkle_tree.ArraysRoot(leaves, 4)
}
