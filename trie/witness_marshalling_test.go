package trie

import (
	"bytes"
	"fmt"
	"math/big"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCompressNibbles(t *testing.T) {
	cases := []struct {
		in     string
		expect string
	}{
		{in: "00", expect: "0000"},
		{in: "0000", expect: "0100"},
		{in: "000000", expect: "020000"},
		{in: "000001", expect: "020010"},
		{in: "01", expect: "0010"},
		{in: "010203040506070809", expect: "081234567890"},
		{in: "0f0000", expect: "02f000"},
		{in: "0f", expect: "00f0"},
		{in: "0f00", expect: "01f0"},
	}

	compressedOut := &bytes.Buffer{}
	decompressedOut := &bytes.Buffer{}
	for _, tc := range cases {
		in := strToNibs(tc.in)
		compressNibbles(in, compressedOut)
		compressed := compressedOut.Bytes()
		msg := "On: " + tc.in + " Len: " + strconv.Itoa(len(compressed))
		assert.Equal(t, tc.expect, fmt.Sprintf("%x", compressed), msg)
		compressedOut.Reset()

		decompressNibbles(compressed, decompressedOut)
		decompressed := decompressedOut.Bytes()
		assert.Equal(t, tc.in, fmt.Sprintf("%x", decompressed), msg)
		decompressedOut.Reset()
	}
}

func BenchmarkParseNib(b *testing.B) {
	in := []byte("0f09090909090909090f090")
	//out := bytes.NewBuffer(make([]byte, 40))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		keyNibblesToBytes(in)
		//compressNibbles(in, out)
		//_ = out.Bytes()
		//out.Reset()
		//n2bV1Compressed(in)
		//n2bV1(in)
		//n2bV2(in)
	}
}

func TestN2bV1Compressed_Others(t *testing.T) {
	fmt.Printf("Res: %x\n", n2bV1Compressed([]byte("00010b")))
	//10b1
	//fmt.Printf("Res: %x\n", n2bV1Compressed([]byte("0f00")))
	return
}

func TestN2bV1Compressed_N3(t *testing.T) {
	oldN := N
	defer func() { N = oldN }()
	N = big.NewInt(3)

	cases := []struct {
		in     string
		expect string
	}{
		{in: "0f0000", expect: "f001"},
		{in: "01", expect: "10"},
		{in: "00", expect: "00"},
		{in: "0000", expect: "0000"},
		{in: "000000", expect: "0001"},
		{in: "000001", expect: "0010"},
		{in: "0f", expect: "f0"},
		{in: "0f00", expect: "f000"},
	}

	for _, tc := range cases {
		fmt.Println("Testing: ", tc.in)
		in := strToNibs(tc.in)
		res := n2bV1Compressed(in)
		msg := "On: " + tc.in + " Len: " + strconv.Itoa(len(res))

		assert.Equal(t, tc.expect, fmt.Sprintf("%x", res), msg)
	}
}

func TestN2bV1Compressed_CompressionLevel(t *testing.T) {
	fmt.Println("LenBefore:", lenArrS(o))
	r := make([][]byte, len(o))
	for i := 0; i < len(o); i++ {
		r[i] = n2bV1Compressed([]byte(o[i]))
	}
	fmt.Println("LenAfter:", lenArr(r))
	for i := 0; i < len(o); i++ {
		fmt.Printf("%s -> %x\n", o[i], r[i])
	}
}

var N = big.NewInt(128)
var DimB = big.NewInt(N.Int64()/2 + 1)
var powersOf16 = make([]*big.Int, int(N.Int64()))
var powersOf256 = make([]*big.Int, int(N.Int64()+1))
var sumOfPowersOf16 = make([]*big.Int, int(N.Int64()))
var sumOfPowersOf256 = make([]*big.Int, int(N.Int64()+1))

var n16 = big.NewInt(16)
var n256 = big.NewInt(256)
var digits = make([]*big.Int, 256)

func init() {
	for i := 0; i < len(powersOf16); i++ {
		a := big.NewInt(int64(i))
		powersOf16[i] = a.Exp(n16, a, nil)
	}

	for k := 0; k < len(sumOfPowersOf16); k++ {
		sum := big.NewInt(0)
		for i := 0; i < k+1; i++ {
			sum.Add(sum, powersOf16[i])
		}
		sumOfPowersOf16[k] = sum
	}

	for i := 0; i < len(powersOf256); i++ {
		a := big.NewInt(int64(i))
		powersOf256[i] = a.Exp(n256, a, nil)
	}
	for k := 0; k < len(sumOfPowersOf256); k++ {
		sum := big.NewInt(0)
		for i := 0; i < k+1; i++ {
			sum.Add(sum, powersOf256[i])
		}
		sumOfPowersOf256[k] = sum
	}

	for i := 0; i < 256; i++ {
		digits[i] = big.NewInt(int64(i))
	}
}

func nibToUint8(in []byte) uint8 {
	nib, err := strconv.ParseUint(string(in), 16, 4)
	if err != nil {
		panic(err)
	}
	return uint8(nib)
}

func nibToDec(in []byte) []uint8 {
	res := make([]uint8, len(in)/2+len(in)%2)
	for i := 0; i < len(in)-1; i = i + 2 {
		a := nibToUint8(in[i+1 : i+2])
		res[i/2] = a
	}
	return res
}

func strToNibs(in string) []uint8 {
	nibs := []byte(in)
	res := make([]uint8, len(in)/2+len(in)%2)
	for i := 0; i < len(nibs)-1; i = i + 2 {
		a := nibToUint8(nibs[i+1 : i+2])
		res[i/2] = a
	}
	return res
}

func n2bV1(in []byte) []byte {
	D := big.NewInt(0)
	nibs := nibToDec(in)
	for k := 0; k < len(nibs); k++ {
		D.Add(D, new(big.Int).Mul(digits[nibs[k]], sumOfPowersOf16[k]))
	}

	D.Add(D, digits[len(nibs)-1])
	return []byte(fmt.Sprintf("%x\n", D))
}

func n2bV2(in []byte, N uint) []byte {
	D := big.NewInt(0)
	for i := uint(0); i < N; i++ {
		sum := big.NewInt(0)
		for k := uint(0); k < N-i; k++ {
			sum.Add(sum, digits[in[k]])
		}
		D.Add(D, sum.Mul(sum, powersOf16[i]))
	}

	D.Add(D, digits[len(in)-1])

	return d2Hex(in, D, N)
}

func lenArr(a [][]byte) int {
	res := 0
	for i := 0; i < len(a); i++ {
		res += len(a[i])
	}
	return res
}

func n2bV1Compressed(in []byte) []byte {
	res := &bytes.Buffer{}
	if in[len(in)-1] != 0 {
		compressNibbles(in, res)
		return res.Bytes()
	}

	var amountOfZeroes int
	for i := len(in) - 1; i >= 0; i-- {
		if in[i] != 0 {
			break
		}
		amountOfZeroes++
	}
	nonZero := len(in) - amountOfZeroes
	var lead []byte
	if nonZero != 0 {
		res.Reset()
		compressNibbles(in, res)
		lead = res.Bytes()
	}
	rest := n2bV2(in[len(in)-amountOfZeroes:], uint(amountOfZeroes))
	return append(lead, rest...)
}

func d2Hex(in []byte, D *big.Int, N uint) []byte {
	DimB2 := N/2 + 1
	res := make([]uint8, 0)
	d := new(big.Int).Set(D)
	for k := uint(0); k < DimB2-1; k++ {

		sum := big.NewInt(0)
		for i := uint(0); i < k; i++ { // - Sum (byte[i] * Sum 256^j)
			s := new(big.Int).Mul(digits[res[i]], sumOfPowersOf256[DimB2-i-1])
			sum.Add(sum, s)
		}

		d.Sub(d, digits[k]) // -k
		//d.Sub(d, sum)
		val := new(big.Int).Div(d, sumOfPowersOf256[DimB2-k-1])
		d.Rem(d, sumOfPowersOf256[DimB2-k-1])

		res = append(res, uint8(val.Uint64()))

		fmt.Printf("Sum: %s, %x\n", sum.String(), sum)
		fmt.Printf("Val: %s, %x\n", val.String(), val)
		fmt.Printf("D: %s, %x\n", d.String(), d)

		if d.Sign() == 0 {
			break
		}
	}

	return res
}
func lenArrS(a []string) int {
	res := 0
	for i := 0; i < len(a); i++ {
		res += len(a[i])
	}
	return res
}

var o = []string{
	"0001",
	"00010b",
	"00010b04",
	"00010b0408",
	"0100",
	"010009",
	"01000902",
	"010f",
	"010f0f",
	"010f0f0d",
	"010f0f0d0e",
	"020a",
	"020a01",
	"020a010c",
	"0308",
	"030805",
	"0308050b",
	"0308050b00",
	"0506",
	"050601",
	"05060100",
	"050601000a",
	"0509",
	"05090e",
	"05090e07",
	"05090e0704",
	"090f",
	"090f01",
	"090f010e",
	"090f010e06",
	"0a04",
	"0a0408",
	"0a040805",
	"0a0a",
	"0a0a06",
	"0a0a0604",
	"0a0a06040f",
	"0b03",
	"0b0300",
	"0b030002",
	"0b0a",
	"0b0a03",
	"0b0a0309",
	"0b0a03090e",
	"0d0c",
	"0d0c07",
	"0d0c070f",
	"0d0c070f04",
	"0f0f",
	"0f0f01",
	"0f0f0105",
	"0f0f010509",
	"0b03000207",
	"0008",
	"00080e",
	"00080e05",
	"0009",
	"00090c",
	"00090c06",
	"0108",
	"010800",
	"01080003",
	"0108000301",
	"0204",
	"020403",
	"02040302",
	"020403020b",
	"02040d",
	"02040d09",
	"0300",
	"03000f",
	"03000f03",
	"03000f030205",
	"030b",
	"030b08",
	"030b0809",
	"0404",
	"040405",
	"04040506",
	"0404050604",
	"0503",
	"05030b",
	"05030b0a",
	"050c",
	"050c09",
	"050c0903",
	"050c090301",
	"060c",
	"060c09",
	"060c090e",
	"060e",
	"060e0e",
	"060e0e0a",
	"070f0f",
	"070f0f00",
	"0805",
	"08050f",
	"08050f00",
	"0807",
	"080700",
	"0807000c",
	"080e",
	"080e09",
	"080e090a",
	"090f0e",
	"090f0e07",
	"090f0e0703",
	"0b06",
	"0b0601",
	"0b060109",
	"0b0a0301",
	"0b0d",
	"0b0d0f",
	"0b0d0f0f",
	"0b0d0f0f0203",
	"0c05",
	"0c050b",
	"0c050b08",
	"0c0d",
	"0c0d0d",
	"0c0d0d02",
	"0c0d0d0200",
	"0d0b",
	"0d0b0c",
	"0d0b0c07",
	"0d0b0c070b",
	"0d0e",
	"0d0e07",
	"0d0e0704",
	"0d0e07040e",
	"0e01",
	"0e010a",
	"0e010a04",
	"0e04",
	"0e0404",
	"0e040407",
	"0f02",
	"0f020d",
	"0f020d05",
	"0f03",
	"0f0303",
	"0f03030c",
	"0f03030c06",
	"0f09",
	"0f0906",
	"0f09060a",
	"0f0d",
	"0f0d07",
	"0f0d0703",
	"0f0d070309",
	//"0b06010900040c0a000201050a0e0f080c070a0c05060809010b08090d0c0806000b07090101050d020a0e020a040c090a0e010d030302030f070c0c070c020f",
	//"0b06010900040c0a000201050a0e0f080c070a0c05060809010b08090d0c0806000b07090101050d020a0e020a040c090a0e010d030302030f070c0c070c020f00",
	//"0b06010900040c0a000201050a0e0f080c070a0c05060809010b08090d0c0806000b07090101050d020a0e020a040c090a0e010d030302030f070c0c070c020f0005",
	//"0b06010900040c0a000201050a0e0f080c070a0c05060809010b08090d0c0806000b07090101050d020a0e020a040c090a0e010d030302030f070c0c070c020f00050e",
	//"0b06010900040c0a000201050a0e0f080c070a0c05060809010b08090d0c0806000b07090101050d020a0e020a040c090a0e010d030302030f070c0c070c020f0b",
	//"0b06010900040c0a000201050a0e0f080c070a0c05060809010b08090d0c0806000b07090101050d020a0e020a040c090a0e010d030302030f070c0c070c020f0b00",
	//"0b06010900040c0a000201050a0e0f080c070a0c05060809010b08090d0c0806000b07090101050d020a0e020a040c090a0e010d030302030f070c0c070c020f0b000b",
	"0b04",
	"0b0402",
	"0b04020f",
	"0b02",
	"0b020e",
	"0b020e01",
	"0003",
	"00030d",
	"00030d0b",
	"00030d0b0a",
	"0004",
	"000401",
	"0004010b",
	"000c",
	"000c0c",
	"000c0c06",
	"000e",
	"000e09",
	"000e0903",
	"0104",
	"01040c",
	"01040c09",
	"01040c0902",
	"0109",
	"010904",
	"01090402",
	"010904020e",
	"010909",
	"01090908",
	"0109090803",
	"020e",
	"020e01",
	"020e010d",
	"020e010d02",
	"0305",
	"03050f",
	"03050f00",
	"03050f0006",
	"0307",
	"030707",
	"0307070b",
	"030802",
	"03080202",
	"0308020205",
	"0405",
	"04050c",
	"04050c0f",
	"0406",
	"040606",
	"0406060b",
	"0406060b08",
	"0409",
	"040903",
	"04090304",
	"040903040f",
	"040b",
	"040b02",
	"040b0203",
	"040b020304",
	"040c",
	"040c0f",
	"040c0f0f",
	"0501",
	"05010c",
	"05010c0c",
	"05010c0c0d",
	"050b",
	"050b0c",
	"050b0c09",
	"050b0c0900",
	"0601",
	"06010c",
	"06010c0f",
	"06010c0f05",
	"0701",
	"07010e",
	"07010e0e",
	"07010e0e0b",
	"0707",
	"070707",
	"07070700",
	"0906",
	"09060f",
	"09060f0e",
	"090b",
	"090b04",
	"090b0408",
	"090d",
	"090d0d",
	"090d0d02",
	"090d0d0200",
	"0a00",
	"0a0003",
	"0a000303",
	"0a0003030e",
	"0a0003030e03",
	"0a06",
	"0a0609",
	"0a06090f",
	"0b020e00",
	"0b020e000a",
	"0b0b",
	"0b0b0d",
	"0b0b0d0a",
	"0b0c",
	"0b0c06",
	"0b0c060d",
	"0c0c",
	"0c0c08",
	"0c0c080d",
	"0c0c080d09",
	"0d0e0707",
	"0e06",
	"0e0607",
	"0e060708",
	"0e0607080c",
	"0e0c",
	"0e0c0e",
	"0e0c0e04",
	"0f0a",
	"0f0a06",
	"0f0a060c",
	//"0b06010900040c0a000201050a0e0f080c070a0c05060809010b08090d0c0806000b07090101050d020a0e020a040c090a0e010d030302030f070c0c070c020f03",
	//"0b06010900040c0a000201050a0e0f080c070a0c05060809010b08090d0c0806000b07090101050d020a0e020a040c090a0e010d030302030f070c0c070c020f030f",
	//"0b06010900040c0a000201050a0e0f080c070a0c05060809010b08090d0c0806000b07090101050d020a0e020a040c090a0e010d030302030f070c0c070c020f07",
	//"0b06010900040c0a000201050a0e0f080c070a0c05060809010b08090d0c0806000b07090101050d020a0e020a040c090a0e010d030302030f070c0c070c020f0706",
	"0307070b0d",
}

// Get High and Low nibbles from byte
func BenchmarkHiNibbleBits(b *testing.B) {
	var x byte = 230
	for i := 0; i < b.N; i++ {
		for j := 0; j < 1_000_000_000; j++ {
			_ = (x >> 4) & 0x0F
		}
	}
}

func BenchmarkHiNibble(b *testing.B) {
	var x byte = 230
	for i := 0; i < b.N; i++ {
		for j := 0; j < 1_000_000_000; j++ {
			_ = x / 16
		}
	}
}

func BenchmarkLoNibbleBits(b *testing.B) {
	var x byte = 230
	for i := 0; i < b.N; i++ {
		for j := 0; j < 1_000_000_000; j++ {
			_ = x & 0x0F
		}
	}
}

func BenchmarkLoNibble(b *testing.B) {
	var x byte = 230
	for i := 0; i < b.N; i++ {
		for j := 0; j < 1_000_000_000; j++ {
			_ = x / 16
		}
	}
}
