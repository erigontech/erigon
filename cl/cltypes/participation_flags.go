// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package cltypes

import (
	"github.com/erigontech/erigon/cl/utils"
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
