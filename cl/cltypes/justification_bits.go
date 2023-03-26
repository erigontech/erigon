package cltypes

import "github.com/ledgerwatch/erigon/cl/utils"

const JustificationBitsLength = 4

type JustificationBits [JustificationBitsLength]bool // Bit vector of size 4

func (j JustificationBits) Byte() (out byte) {
	for i, bit := range j {
		if !bit {
			continue
		}
		out += byte(utils.PowerOf2(uint64(i)))
	}
	return
}

func (j *JustificationBits) FromByte(b byte) {
	j[0] = b&1 > 0
	j[1] = b&2 > 0
	j[2] = b&4 > 0
	j[3] = b&8 > 0
}

// CheckRange checks if bits in certain range are all enabled.
func (j JustificationBits) CheckRange(start int, end int) bool {
	checkBits := j[start:end]
	for _, bit := range checkBits {
		if !bit {
			return false
		}
	}
	return true
}

func (j JustificationBits) Copy() JustificationBits {
	return JustificationBits{j[0], j[1], j[2], j[3]}
}
