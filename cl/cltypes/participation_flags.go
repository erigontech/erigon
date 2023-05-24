package cltypes

import (
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
