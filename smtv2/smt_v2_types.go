package smtv2

import (
	"encoding/binary"
	"fmt"
	"math/big"

	"github.com/erigontech/erigon/smt/pkg/utils"
)

type SmtKey [4]uint64

func (nk *SmtKey) IsZero() bool {
	return nk[0] == 0 && nk[1] == 0 && nk[2] == 0 && nk[3] == 0
}

func (nk *SmtKey) IsEqualTo(nk2 SmtKey) bool {
	return nk[0] == nk2[0] && nk[1] == nk2[1] && nk[2] == nk2[2] && nk[3] == nk2[3]
}

func (nk *SmtKey) ToBigInt() *big.Int {
	return utils.ArrayToScalar(nk[:])
}

func (nk *SmtKey) AsUint64Pointer() *[4]uint64 {
	return (*[4]uint64)(nk)
}

func (nk *SmtKey) GetPath() []int {
	res := make([]int, 256) // Pre-allocate exact size needed
	auxk := [4]uint64{nk[0], nk[1], nk[2], nk[3]}

	idx := 0
	for j := 0; j < 64; j++ {
		for i := 0; i < 4; i++ {
			res[idx] = int(auxk[i] & 1) // Use direct index assignment instead of append
			auxk[i] >>= 1
			idx++
		}
	}

	return res
}

func (nk *SmtKey) SetFromPath(path []int) (SmtKey, error) {
	if len(path) != 256 {
		return SmtKey{}, fmt.Errorf("path is not 256 bits")
	}

	res := [4]uint64{0, 0, 0, 0}

	for j := 0; j < 256; j++ {
		i := j % 4

		k := j / 4
		res[i] |= uint64(path[j]) << k
	}

	return res, nil
}

type SmtValue8 [8]uint64

func (sv *SmtValue8) IsZero() bool {
	return sv[0] == 0 && sv[1] == 0 && sv[2] == 0 && sv[3] == 0 && sv[4] == 0 && sv[5] == 0 && sv[6] == 0 && sv[7] == 0
}

func (sv *SmtValue8) ToUintArrayByPointer() *[8]uint64 {
	return (*[8]uint64)(sv)
}

func (sv *SmtValue8) ToBytes() []byte {
	buf := make([]byte, 64)
	binary.BigEndian.PutUint64(buf[0:8], sv[0])
	binary.BigEndian.PutUint64(buf[8:16], sv[1])
	binary.BigEndian.PutUint64(buf[16:24], sv[2])
	binary.BigEndian.PutUint64(buf[24:32], sv[3])
	binary.BigEndian.PutUint64(buf[32:40], sv[4])
	binary.BigEndian.PutUint64(buf[40:48], sv[5])
	binary.BigEndian.PutUint64(buf[48:56], sv[6])
	binary.BigEndian.PutUint64(buf[56:64], sv[7])

	return buf
}

func (sv *SmtValue8) FromBytes(bytes []byte) SmtValue8 {
	sv[0] = binary.BigEndian.Uint64(bytes[0:8])
	sv[1] = binary.BigEndian.Uint64(bytes[8:16])
	sv[2] = binary.BigEndian.Uint64(bytes[16:24])
	sv[3] = binary.BigEndian.Uint64(bytes[24:32])
	sv[4] = binary.BigEndian.Uint64(bytes[32:40])
	sv[5] = binary.BigEndian.Uint64(bytes[40:48])
	sv[6] = binary.BigEndian.Uint64(bytes[48:56])
	sv[7] = binary.BigEndian.Uint64(bytes[56:64])

	return *sv
}
