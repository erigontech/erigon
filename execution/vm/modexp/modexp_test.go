package modexp

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// precompiledTest matches the format in execution/vm/testdata/precompiles/*.json
type precompiledTest struct {
	Input       string `json:"Input"`
	Expected    string `json:"Expected"`
	Gas         uint64 `json:"Gas"`
	Name        string `json:"Name"`
	NoBenchmark bool   `json:"NoBenchmark"`
}

// parseModexpInput extracts base, exp, mod from ABI-encoded modexp precompile input.
func parseModexpInput(inputHex string) (base, exp, mod []byte) {
	input, _ := hex.DecodeString(inputHex)
	if len(input) < 96 {
		padded := make([]byte, 96)
		copy(padded, input)
		input = padded
	}

	baseLen := new(big.Int).SetBytes(input[0:32]).Uint64()
	expLen := new(big.Int).SetBytes(input[32:64]).Uint64()
	modLen := new(big.Int).SetBytes(input[64:96]).Uint64()

	data := input[96:]
	getData := func(offset, length uint64) []byte {
		if offset >= uint64(len(data)) {
			return make([]byte, length)
		}
		end := offset + length
		if end > uint64(len(data)) {
			result := make([]byte, length)
			copy(result, data[offset:])
			return result
		}
		result := make([]byte, length)
		copy(result, data[offset:end])
		return result
	}

	base = getData(0, baseLen)
	exp = getData(baseLen, expLen)
	mod = getData(baseLen+expLen, modLen)
	return
}

func testdataDir() string {
	_, filename, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(filename), "..", "testdata", "precompiles")
}

func loadPrecompileVectors(t testing.TB, name string) []precompiledTest {
	t.Helper()
	path := filepath.Join(testdataDir(), name+".json")
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	var tests []precompiledTest
	require.NoError(t, json.Unmarshal(data, &tests))
	return tests
}

// TestEVMVectors tests against all existing EVM precompile test vectors.
func TestEVMVectors(t *testing.T) {
	for _, file := range []string{"modexp", "modexp_eip2565", "modexp_eip7883"} {
		t.Run(file, func(t *testing.T) {
			tests := loadPrecompileVectors(t, file)
			for _, tc := range tests {
				t.Run(tc.Name, func(t *testing.T) {
					base, exp, mod := parseModexpInput(tc.Input)
					got := Exp(base, exp, mod)
					expected, _ := hex.DecodeString(tc.Expected)
					require.Equal(t, hex.EncodeToString(expected), hex.EncodeToString(got),
						"base=%x exp=%x mod=%x", base, exp, mod)
				})
			}
		})
	}
}

// TestBasicCases tests fundamental modexp edge cases.
func TestBasicCases(t *testing.T) {
	tests := []struct {
		name     string
		base     string // hex
		exp      string
		mod      string
		expected string
	}{
		{"0^0 mod 1 = 0", "00", "00", "01", "00"},
		{"0^0 mod 2 = 1", "00", "00", "02", "01"},
		{"0^1 mod 2 = 0", "00", "01", "02", "00"},
		{"1^0 mod 2 = 1", "01", "00", "02", "01"},
		{"2^10 mod 1000 = 24", "02", "0a", "03e8", "0018"},
		{"2^10 mod 1023 = 1", "02", "0a", "03ff", "0001"},
		{"2^10 mod 1024 = 0", "02", "0a", "0400", "0000"},
		{"3^0 mod 5 = 1", "03", "00", "05", "01"},
		{"0^5 mod 7 = 0", "00", "05", "07", "00"},
		{"5^1 mod 3 = 2", "05", "01", "03", "02"},
		{"7^2 mod 5 = 4", "07", "02", "05", "04"},
		{"255^255 mod 256 = 255", "ff", "ff", "0100", "00ff"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			base, _ := hex.DecodeString(tc.base)
			exp, _ := hex.DecodeString(tc.exp)
			mod, _ := hex.DecodeString(tc.mod)
			expected, _ := hex.DecodeString(tc.expected)
			got := Exp(base, exp, mod)
			require.Equal(t, hex.EncodeToString(expected), hex.EncodeToString(got))
		})
	}
}

// TestEvenModuli tests CRT path with various even moduli.
func TestEvenModuli(t *testing.T) {
	tests := []struct {
		name string
		base *big.Int
		exp  *big.Int
		mod  *big.Int
	}{
		{"mod=2", big.NewInt(3), big.NewInt(7), big.NewInt(2)},
		{"mod=4", big.NewInt(3), big.NewInt(100), big.NewInt(4)},
		{"mod=6", big.NewInt(5), big.NewInt(30), big.NewInt(6)},
		{"mod=8", big.NewInt(7), big.NewInt(50), big.NewInt(8)},
		{"mod=12", big.NewInt(11), big.NewInt(25), big.NewInt(12)},
		{"mod=16", big.NewInt(15), big.NewInt(33), big.NewInt(16)},
		{"mod=100", big.NewInt(99), big.NewInt(99), big.NewInt(100)},
		{"mod=256", big.NewInt(255), big.NewInt(255), big.NewInt(256)},
		{"mod=1024", big.NewInt(999), big.NewInt(999), big.NewInt(1024)},
		{"mod=2^64", big.NewInt(0).SetUint64(0xDEADBEEF), big.NewInt(100),
			new(big.Int).Lsh(big.NewInt(1), 64)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			expected := new(big.Int).Exp(tc.base, tc.exp, tc.mod)
			modBytes := tc.mod.Bytes()
			got := Exp(tc.base.Bytes(), tc.exp.Bytes(), modBytes)
			gotInt := new(big.Int).SetBytes(got)
			require.Equal(t, expected.String(), gotInt.String(),
				"base=%s exp=%s mod=%s", tc.base, tc.exp, tc.mod)
		})
	}
}

// TestWordBoundaries tests sizes at word boundaries (63/64/65 bits, etc.).
func TestWordBoundaries(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	boundaries := []int{63, 64, 65, 127, 128, 129, 191, 192, 193, 255, 256, 257, 511, 512, 513}

	for _, bits := range boundaries {
		t.Run(fmt.Sprintf("%d-bits", bits), func(t *testing.T) {
			byteLen := (bits + 7) / 8
			base := randomBytes(rng, byteLen)
			exp := randomBytes(rng, byteLen)
			mod := randomOddBytes(rng, byteLen) // odd for Montgomery path

			expected := new(big.Int).Exp(
				new(big.Int).SetBytes(base),
				new(big.Int).SetBytes(exp),
				new(big.Int).SetBytes(mod),
			)
			got := Exp(base, exp, mod)
			gotInt := new(big.Int).SetBytes(got)
			require.Equal(t, expected.String(), gotInt.String(),
				"bits=%d", bits)
		})
	}
}

// TestLargeExponents tests with large exponents (up to 1024 bytes).
func TestLargeExponents(t *testing.T) {
	rng := rand.New(rand.NewSource(43))
	for _, expSize := range []int{32, 64, 128, 256, 512} {
		t.Run(fmt.Sprintf("exp-%dB", expSize), func(t *testing.T) {
			base := randomBytes(rng, 32)
			exp := randomBytes(rng, expSize)
			mod := randomOddBytes(rng, 32)

			expected := new(big.Int).Exp(
				new(big.Int).SetBytes(base),
				new(big.Int).SetBytes(exp),
				new(big.Int).SetBytes(mod),
			)
			got := Exp(base, exp, mod)
			gotInt := new(big.Int).SetBytes(got)
			require.Equal(t, expected.String(), gotInt.String())
		})
	}
}

// TestCarryPropagation tests carry chain edge cases.
func TestCarryPropagation(t *testing.T) {
	tests := []struct {
		name string
		base []byte
		exp  []byte
		mod  []byte
	}{
		{
			"all-ones-base",
			bytes.Repeat([]byte{0xFF}, 32),
			[]byte{0x02},
			bytes.Repeat([]byte{0xFF}, 32),
		},
		{
			"all-ones-mod",
			[]byte{0x02},
			bytes.Repeat([]byte{0xFF}, 32),
			bytes.Repeat([]byte{0xFF}, 32),
		},
		{
			"max-limb-values",
			bytes.Repeat([]byte{0xFF}, 64),
			[]byte{0x03},
			append(bytes.Repeat([]byte{0xFF}, 63), 0xFD), // odd
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			expected := new(big.Int).Exp(
				new(big.Int).SetBytes(tc.base),
				new(big.Int).SetBytes(tc.exp),
				new(big.Int).SetBytes(tc.mod),
			)
			got := Exp(tc.base, tc.exp, tc.mod)
			gotInt := new(big.Int).SetBytes(got)
			require.Equal(t, expected.String(), gotInt.String(), tc.name)
		})
	}
}

// TestOracleRandom tests random inputs against Go's math/big as oracle.
func TestOracleRandom(t *testing.T) {
	rng := rand.New(rand.NewSource(44))

	for i := 0; i < 500; i++ {
		// Random sizes: base 1-128 bytes, exp 1-64 bytes, mod 1-128 bytes
		baseSize := rng.Intn(128) + 1
		expSize := rng.Intn(64) + 1
		modSize := rng.Intn(128) + 1

		base := randomBytes(rng, baseSize)
		exp := randomBytes(rng, expSize)
		mod := randomNonZeroBytes(rng, modSize)

		// Ensure mod > 1
		modInt := new(big.Int).SetBytes(mod)
		if modInt.Cmp(big.NewInt(1)) <= 0 {
			modInt.SetInt64(3)
			mod = modInt.Bytes()
		}

		expected := new(big.Int).Exp(
			new(big.Int).SetBytes(base),
			new(big.Int).SetBytes(exp),
			modInt,
		)

		got := Exp(base, exp, mod)
		gotInt := new(big.Int).SetBytes(got)
		if expected.Cmp(gotInt) != 0 {
			t.Fatalf("mismatch at i=%d: base=%x exp=%x mod=%x expected=%s got=%s",
				i, base, exp, mod, expected.String(), gotInt.String())
		}
	}
}

// TestAsymmetricSizes tests with different sizes for base, exp, and mod.
func TestAsymmetricSizes(t *testing.T) {
	rng := rand.New(rand.NewSource(45))
	cases := [][3]int{
		{1, 1, 32},    // tiny base/exp, normal mod
		{32, 1, 32},   // normal base, tiny exp
		{1, 32, 32},   // tiny base, normal exp
		{128, 32, 64}, // large base, normal exp, medium mod
		{32, 128, 32}, // normal base, large exp
		{64, 64, 128}, // mod larger than base
	}
	for _, sizes := range cases {
		name := fmt.Sprintf("b%d-e%d-m%d", sizes[0], sizes[1], sizes[2])
		t.Run(name, func(t *testing.T) {
			base := randomBytes(rng, sizes[0])
			exp := randomBytes(rng, sizes[1])
			mod := randomOddBytes(rng, sizes[2])

			expected := new(big.Int).Exp(
				new(big.Int).SetBytes(base),
				new(big.Int).SetBytes(exp),
				new(big.Int).SetBytes(mod),
			)
			got := Exp(base, exp, mod)
			gotInt := new(big.Int).SetBytes(got)
			require.Equal(t, expected.String(), gotInt.String())
		})
	}
}

// TestSpecialModValues tests mod values that are special for Montgomery.
func TestSpecialModValues(t *testing.T) {
	tests := []struct {
		name string
		mod  *big.Int
	}{
		{"mod=3", big.NewInt(3)},
		{"mod=2^64-1", new(big.Int).SetUint64(^uint64(0))},
		{"mod=2^128-1", new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 128), big.NewInt(1))},
		{"mod=2^256-1", new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 256), big.NewInt(1))},
		// secp256k1 prime
		{"mod=secp256k1", hexInt("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEFFFFFC2F")},
	}
	base := hexInt("DEADBEEF")
	exp := hexInt("CAFEBABE")
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			expected := new(big.Int).Exp(base, exp, tc.mod)
			modBytes := tc.mod.Bytes()
			got := Exp(base.Bytes(), exp.Bytes(), modBytes)
			gotInt := new(big.Int).SetBytes(got)
			require.Equal(t, expected.String(), gotInt.String())
		})
	}
}

// --- helpers ---

func hexInt(s string) *big.Int {
	n, _ := new(big.Int).SetString(s, 16)
	return n
}

func randomBytes(rng *rand.Rand, n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(rng.Intn(256))
	}
	// Ensure at least one non-zero byte
	if n > 0 {
		b[0] |= 1
	}
	return b
}

func randomOddBytes(rng *rand.Rand, n int) []byte {
	b := randomBytes(rng, n)
	b[n-1] |= 1 // make odd
	return b
}

func randomNonZeroBytes(rng *rand.Rand, n int) []byte {
	b := randomBytes(rng, n)
	b[0] |= 1 // ensure non-zero
	return b
}

// --- Nat unit tests ---

func TestNatFromBytes(t *testing.T) {
	tests := []struct {
		hex    string
		limbs  int
		isZero bool
	}{
		{"00", 0, true},
		{"01", 1, false},
		{"ff", 1, false},
		{strings.Repeat("ff", 8), 1, false},
		{strings.Repeat("ff", 9), 2, false},
		{strings.Repeat("00", 32), 0, true},
	}
	for _, tc := range tests {
		b, _ := hex.DecodeString(tc.hex)
		n := natFromBytes(b)
		require.Equal(t, tc.limbs, len(n.limbs), "hex=%s", tc.hex)
		require.Equal(t, tc.isZero, n.isZero(), "hex=%s", tc.hex)
	}
}

func TestNatBytes(t *testing.T) {
	b, _ := hex.DecodeString("DEADBEEF")
	n := natFromBytes(b)
	got := n.bytes(4)
	require.Equal(t, "deadbeef", hex.EncodeToString(got))

	// Zero-padded
	got = n.bytes(8)
	require.Equal(t, "00000000deadbeef", hex.EncodeToString(got))
}

func TestMinusInverseModW(t *testing.T) {
	// For any odd x, x * (-x^{-1}) ≡ -1 (mod 2^W)
	odds := []uint{1, 3, 5, 7, 0xFF, 0xDEADBEEF, ^uint(0), ^uint(0) - 1}
	for _, x := range odds {
		if x&1 == 0 {
			continue
		}
		inv := minusInverseModW(x)
		product := x * inv
		require.Equal(t, ^uint(0), product, "x=%d", x)
	}
}

func TestAddMulVVW(t *testing.T) {
	// z += x * y
	z := []uint{0, 0, 0}
	x := []uint{^uint(0), ^uint(0), ^uint(0)}
	y := uint(2)
	carry := addMulVVW(z, x, y)

	// x * 2 should be (2^W - 1) * 2 for each limb
	// Result: z[0] = max-1, carry from z[0] = 1
	// etc.
	expected := new(big.Int).Mul(
		new(big.Int).SetBits([]big.Word{big.Word(^uint(0)), big.Word(^uint(0)), big.Word(^uint(0))}),
		big.NewInt(2),
	)
	var gotLimbs []big.Word
	for _, l := range z {
		gotLimbs = append(gotLimbs, big.Word(l))
	}
	gotLimbs = append(gotLimbs, big.Word(carry))
	got := new(big.Int).SetBits(gotLimbs)
	require.Equal(t, expected.String(), got.String())
}
