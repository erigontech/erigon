package ethdb

import "github.com/ledgerwatch/turbo-geth/common"

type noLeadingZeroes struct{}

// NoLeadingZeroes - uses 1 byte to store there amount of leading zeroes and drop them
// maximum 256
var NoLeadingZeroes noLeadingZeroes

func (noLeadingZeroes) Marshal(in []byte) (out []byte) {
	leadingZeros := uint8(0)
	for i := 0; i < len(in); i++ {
		if in[i] != 0 || leadingZeros == 255 {
			break
		}
		leadingZeros++
	}
	if leadingZeros > 0 {
		out = common.CopyBytes(in)
		out[leadingZeros-1] = leadingZeros
		out = out[leadingZeros-1:]
	} else {
		out = append([]byte{0}, in...)
	}
	return out
}

func (noLeadingZeroes) Unmarshal(in []byte) (out []byte) {
	// 1st byte stores amount of leading zeroes - it's db-level detail, don't return it to user
	if in[0] == 0 {
		return in[1:]
	}

	if in[0] == 1 {
		in[0] = 0
		return in
	}

	withLeadingZeroes := make([]byte, int(in[0])+len(in)-1)
	copy(withLeadingZeroes[int(in[0]):], in[1:])
	return withLeadingZeroes
}
