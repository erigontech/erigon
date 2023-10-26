package cltypes

import (
	"github.com/ledgerwatch/erigon-lib/types/clonable"
	"github.com/ledgerwatch/erigon/cl/utils"
)

type ParticipationFlags byte

func (f ParticipationFlags) Add(index int) ParticipationFlags {
	return f | ParticipationFlags(utils.PowerOf2(uint64(index)))
}

func (f ParticipationFlags) HasFlag(index int) bool {
	flag := ParticipationFlags(utils.PowerOf2(uint64(index)))
	return f&flag == flag
}
func (f *ParticipationFlags) DecodeSSZ(b []byte, _ int) error {
	*f = ParticipationFlags(b[0])
	return nil
}

func (f ParticipationFlags) EncodeSSZ(buf []byte) ([]byte, error) {
	return append(buf, byte(f)), nil
}

func (f ParticipationFlags) Clone() clonable.Clonable {
	return f
}

func (ParticipationFlags) EncodingSizeSSZ() int {
	return 1
}

func (ParticipationFlags) Static() bool {
	return true
}

func (f ParticipationFlags) HashSSZ() (out [32]byte, err error) {
	out[0] = byte(f)
	return
}

type ParticipationFlagsList []ParticipationFlags

func (p ParticipationFlagsList) Bytes() []byte {
	b := make([]byte, len(p))
	for i := range p {
		b[i] = byte(p[i])
	}
	return b
}

func (p ParticipationFlagsList) Copy() ParticipationFlagsList {
	c := make(ParticipationFlagsList, len(p))
	copy(c, p)
	return c
}

func ParticipationFlagsListFromBytes(buf []byte) ParticipationFlagsList {
	flagsList := make([]ParticipationFlags, len(buf))
	for i := range flagsList {
		flagsList[i] = ParticipationFlags(buf[i])
	}
	return flagsList
}
