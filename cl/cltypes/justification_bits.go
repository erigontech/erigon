package cltypes

import (
	"encoding/json"

	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon-lib/types/clonable"
	"github.com/ledgerwatch/erigon/cl/utils"
)

const JustificationBitsLength = 4

type JustificationBits [JustificationBitsLength]bool // Bit vector of size 4

func (j JustificationBits) Clone() clonable.Clonable {
	return JustificationBits{}
}
func (j JustificationBits) Byte() (out byte) {
	for i, bit := range j {
		if !bit {
			continue
		}
		out += byte(utils.PowerOf2(uint64(i)))
	}
	return
}

func (j *JustificationBits) DecodeSSZ(b []byte, _ int) error {
	j[0] = b[0]&1 > 0
	j[1] = b[0]&2 > 0
	j[2] = b[0]&4 > 0
	j[3] = b[0]&8 > 0
	return nil
}

func (j JustificationBits) EncodeSSZ(buf []byte) ([]byte, error) {
	return append(buf, j.Byte()), nil
}

func (JustificationBits) EncodingSizeSSZ() int {
	return 1
}

func (JustificationBits) Static() bool {
	return true
}

func (j *JustificationBits) HashSSZ() (out [32]byte, err error) {
	out[0] = j.Byte()
	return
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

func (j JustificationBits) MarshalJSON() ([]byte, error) {
	enc, err := j.EncodeSSZ(nil)
	if err != nil {
		return nil, err
	}
	return json.Marshal(hexutility.Bytes(enc))
}

func (j *JustificationBits) UnmarshalJSON(input []byte) error {
	var hex hexutility.Bytes
	if err := json.Unmarshal(input, &hex); err != nil {
		return err
	}
	return j.DecodeSSZ(hex, 0)
}
