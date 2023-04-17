package merkletree

import (
	"encoding/binary"
	"fmt"
	"math/big"
	"strings"

	poseidon "github.com/iden3/go-iden3-crypto/goldenposeidon"
	"github.com/ledgerwatch/erigon/zkevm/hex"
)

// maxBigIntLen is 256 bits (32 bytes)
const maxBigIntLen = 32

// wordLength is the number of bits of each ff limb
const wordLength = 64

// fea2scalar converts array of uint64 values into one *big.Int.
func fea2scalar(v []uint64) *big.Int {
	if len(v) != poseidon.NROUNDSF {
		return big.NewInt(0)
	}
	res := new(big.Int).SetUint64(v[0])
	res.Add(res, new(big.Int).Lsh(new(big.Int).SetUint64(v[1]), 32))  //nolint:gomnd
	res.Add(res, new(big.Int).Lsh(new(big.Int).SetUint64(v[2]), 64))  //nolint:gomnd
	res.Add(res, new(big.Int).Lsh(new(big.Int).SetUint64(v[3]), 96))  //nolint:gomnd
	res.Add(res, new(big.Int).Lsh(new(big.Int).SetUint64(v[4]), 128)) //nolint:gomnd
	res.Add(res, new(big.Int).Lsh(new(big.Int).SetUint64(v[5]), 160)) //nolint:gomnd
	res.Add(res, new(big.Int).Lsh(new(big.Int).SetUint64(v[6]), 192)) //nolint:gomnd
	res.Add(res, new(big.Int).Lsh(new(big.Int).SetUint64(v[7]), 224)) //nolint:gomnd
	return res
}

// scalar2fea splits a *big.Int into array of 32bit uint64 values.
func scalar2fea(value *big.Int) []uint64 {
	val := make([]uint64, 8)                          //nolint:gomnd
	mask, _ := new(big.Int).SetString("FFFFFFFF", 16) //nolint:gomnd
	val[0] = new(big.Int).And(value, mask).Uint64()
	val[1] = new(big.Int).And(new(big.Int).Rsh(value, 32), mask).Uint64()  //nolint:gomnd
	val[2] = new(big.Int).And(new(big.Int).Rsh(value, 64), mask).Uint64()  //nolint:gomnd
	val[3] = new(big.Int).And(new(big.Int).Rsh(value, 96), mask).Uint64()  //nolint:gomnd
	val[4] = new(big.Int).And(new(big.Int).Rsh(value, 128), mask).Uint64() //nolint:gomnd
	val[5] = new(big.Int).And(new(big.Int).Rsh(value, 160), mask).Uint64() //nolint:gomnd
	val[6] = new(big.Int).And(new(big.Int).Rsh(value, 192), mask).Uint64() //nolint:gomnd
	val[7] = new(big.Int).And(new(big.Int).Rsh(value, 224), mask).Uint64() //nolint:gomnd
	return val
}

// h4ToScalar converts array of 4 uint64 into a unique 256 bits scalar.
func h4ToScalar(h4 []uint64) *big.Int {
	if len(h4) == 0 {
		return new(big.Int)
	}
	result := new(big.Int).SetUint64(h4[0])

	for i := 1; i < 4; i++ {
		b2 := new(big.Int).SetUint64(h4[i])
		b2.Lsh(b2, uint(wordLength*i))
		result = result.Add(result, b2)
	}

	return result
}

// H4ToString converts array of 4 Scalars of 64 bits into an hex string.
func H4ToString(h4 []uint64) string {
	sc := h4ToScalar(h4)

	return fmt.Sprintf("0x%064s", hex.EncodeToString(sc.Bytes()))
}

// StringToh4 converts an hex string into array of 4 Scalars of 64 bits.
func StringToh4(str string) ([]uint64, error) {
	if strings.HasPrefix(str, "0x") { // nolint
		str = str[2:]
	}

	bi, ok := new(big.Int).SetString(str, hex.Base)
	if !ok {
		return nil, fmt.Errorf("Could not convert %q into big int", str)
	}

	return scalarToh4(bi), nil
}

// scalarToh4 converts a *big.Int into an array of 4 uint64
func scalarToh4(s *big.Int) []uint64 {
	b := ScalarToFilledByteSlice(s)

	r := make([]uint64, 4) //nolint:gomnd

	f, _ := hex.DecodeHex("0xFFFFFFFFFFFFFFFF")
	fbe := binary.BigEndian.Uint64(f)

	r[3] = binary.BigEndian.Uint64(b[0:8]) & fbe
	r[2] = binary.BigEndian.Uint64(b[8:16]) & fbe
	r[1] = binary.BigEndian.Uint64(b[16:24]) & fbe
	r[0] = binary.BigEndian.Uint64(b[24:]) & fbe

	return r
}

// ScalarToFilledByteSlice converts a *big.Int into an array of maxBigIntLen
// bytes.
func ScalarToFilledByteSlice(s *big.Int) []byte {
	buf := make([]byte, maxBigIntLen)
	return s.FillBytes(buf)
}

// h4ToFilledByteSlice converts an array of 4 uint64 into an array of
// maxBigIntLen bytes.
func h4ToFilledByteSlice(h4 []uint64) []byte {
	return ScalarToFilledByteSlice(h4ToScalar(h4))
}

// string2fea converts an string into an array of 32bit uint64 values.
func string2fea(s string) ([]uint64, error) {
	bi, ok := new(big.Int).SetString(s, hex.Base)
	if !ok {
		return nil, fmt.Errorf("Could not convert %q into big int", s)
	}
	return scalar2fea(bi), nil
}

// fea2string converts an array of 32bit uint64 values into a string.
func fea2string(fea []uint64) string {
	bi := fea2scalar(fea)

	biBytes := ScalarToFilledByteSlice(bi)

	return hex.EncodeToHex(biBytes)
}
