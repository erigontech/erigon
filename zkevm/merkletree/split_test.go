package merkletree

import (
	"fmt"
	"math/big"
	"testing"

	"github.com/0xPolygonHermez/zkevm-node/test/testutils"
	"github.com/ledgerwatch/erigon/zkevm/hex"
	"github.com/stretchr/testify/require"
)

func TestSplit(t *testing.T) {
	v1str := "115792089237316195423570985008687907853269984665640564039457584007913129639935"

	v1, ok := new(big.Int).SetString(v1str, 10)
	require.True(t, ok)

	v := scalar2fea(v1)

	v2 := fea2scalar(v)
	require.Equal(t, v1, v2)

	vv := scalar2fea(v2)

	require.Equal(t, v, vv)
}

func Test_h4ToScalar(t *testing.T) {
	tcs := []struct {
		input    []uint64
		expected string
	}{
		{
			input:    []uint64{0, 0, 0, 0},
			expected: "0",
		},
		{
			input:    []uint64{0, 1, 2, 3},
			expected: "18831305206160042292187933003464876175252262292329349513216",
		},
	}

	for i, tc := range tcs {
		tc := tc
		t.Run(fmt.Sprintf("case %d", i), func(t *testing.T) {
			actual := h4ToScalar(tc.input)
			expected, ok := new(big.Int).SetString(tc.expected, 10)
			require.True(t, ok)
			require.Equal(t, expected, actual)
		})
	}
}

func Test_scalarToh4(t *testing.T) {
	tcs := []struct {
		input    string
		expected []uint64
	}{
		{
			input:    "0",
			expected: []uint64{0, 0, 0, 0},
		},
		{
			input:    "18831305206160042292187933003464876175252262292329349513216",
			expected: []uint64{0, 1, 2, 3},
		},
	}

	for i, tc := range tcs {
		tc := tc
		t.Run(fmt.Sprintf("case %d", i), func(t *testing.T) {
			bi, ok := new(big.Int).SetString(tc.input, 10)
			require.True(t, ok)

			actual := scalarToh4(bi)
			require.Equal(t, tc.expected, actual)
		})
	}
}

func Test_h4ToString(t *testing.T) {
	tcs := []struct {
		input    []uint64
		expected string
	}{
		{
			input:    []uint64{0, 0, 0, 0},
			expected: "0x0000000000000000000000000000000000000000000000000000000000000000",
		},
		{
			input:    []uint64{0, 1, 2, 3},
			expected: "0x0000000000000003000000000000000200000000000000010000000000000000",
		},
	}

	for i, tc := range tcs {
		tc := tc
		t.Run(fmt.Sprintf("case %d", i), func(t *testing.T) {
			actual := H4ToString(tc.input)
			require.Equal(t, tc.expected, actual)
		})
	}
}

func Test_Conversions(t *testing.T) {
	tcs := []struct {
		input []uint64
	}{
		{
			input: []uint64{0, 0, 0, 0},
		},
		{
			input: []uint64{0, 1, 2, 3},
		},
	}

	for i, tc := range tcs {
		tc := tc
		t.Run(fmt.Sprintf("case %d", i), func(t *testing.T) {
			resScalar := h4ToScalar(tc.input)
			init := scalarToh4(resScalar)
			require.Equal(t, tc.input, init)
		})
	}
}

func Test_scalar2fea(t *testing.T) {
	tcs := []struct {
		input string
	}{
		{
			input: "0",
		},
		{
			input: "100",
		},
		{
			input: "115792089237316195423570985008687907853269984665640564039457584007913129639935",
		},
	}

	for i, tc := range tcs {
		tc := tc
		t.Run(fmt.Sprintf("case %d", i), func(t *testing.T) {
			scalar, ok := new(big.Int).SetString(tc.input, 10)
			require.True(t, ok)

			res := scalar2fea(scalar)

			actual := fea2scalar(res)
			require.Equal(t, tc.input, actual.String())
		})
	}
}

func Test_fea2scalar(t *testing.T) {
	tcs := []struct {
		input []uint64
	}{
		{
			input: []uint64{0, 0, 0, 0, 0, 0, 0, 0},
		},
		{
			input: []uint64{1, 1, 1, 1, 1, 1, 1, 1},
		},
		{
			input: []uint64{1, 0, 0, 0, 3693650181, 4001443757, 599269951, 1255793162},
		},
	}

	for i, tc := range tcs {
		tc := tc
		t.Run(fmt.Sprintf("case %d", i), func(t *testing.T) {
			res := fea2scalar(tc.input)

			actual := scalar2fea(res)

			require.Equal(t, tc.input, actual)
		})
	}
}

func Test_stringToh4(t *testing.T) {
	tcs := []struct {
		description    string
		input          string
		expected       []uint64
		expectedErr    bool
		expectedErrMsg string
	}{
		{
			description: "happy path",
			input:       "cafe",
			expected:    []uint64{51966, 0, 0, 0},
		},
		{
			description: "0x prefix is allowed",
			input:       "0xcafe",
			expected:    []uint64{51966, 0, 0, 0},
		},

		{
			description:    "non hex input causes error",
			input:          "yu74",
			expectedErr:    true,
			expectedErrMsg: "Could not convert",
		},
		{
			description:    "empty input causes error",
			input:          "",
			expectedErr:    true,
			expectedErrMsg: "Could not convert",
		},
	}

	for _, tc := range tcs {
		tc := tc
		t.Run(tc.description, func(t *testing.T) {
			actual, err := StringToh4(tc.input)
			require.NoError(t, testutils.CheckError(err, tc.expectedErr, tc.expectedErrMsg))

			require.Equal(t, tc.expected, actual)
		})
	}
}

func Test_ScalarToFilledByteSlice(t *testing.T) {
	tcs := []struct {
		input    string
		expected string
	}{
		{
			input:    "0",
			expected: "0x0000000000000000000000000000000000000000000000000000000000000000",
		},
		{
			input:    "256",
			expected: "0x0000000000000000000000000000000000000000000000000000000000000100",
		},
		{
			input:    "235938498573495379548793890390932048239042839490238",
			expected: "0x0000000000000000000000a16f882ee8972432c0a71c5e309ad5f7215690aebe",
		},
		{
			input:    "4309593458485959083095843905390485089430985490434080439904305093450934509490",
			expected: "0x098724b9a1bc97eee674cf5b6b56b8fafd83ac49c3da1f2c87c822548bbfdfb2",
		},
		{
			input:    "98999023430240239049320492430858334093493024832984092384902398409234090932489",
			expected: "0xdadf762a31e865f150a1456d7db7963c91361b771c8381a3fb879cf5bf91b909",
		},
	}

	for i, tc := range tcs {
		tc := tc
		t.Run(fmt.Sprintf("test case %d", i), func(t *testing.T) {
			input, ok := big.NewInt(0).SetString(tc.input, 10)
			require.True(t, ok)

			actualSlice := ScalarToFilledByteSlice(input)

			actual := hex.EncodeToHex(actualSlice)

			require.Equal(t, tc.expected, actual)
		})
	}
}

func Test_h4ToFilledByteSlice(t *testing.T) {
	tcs := []struct {
		input    []uint64
		expected string
	}{
		{
			input:    []uint64{0, 0, 0, 0},
			expected: "0x0000000000000000000000000000000000000000000000000000000000000000",
		},
		{
			input:    []uint64{0, 1, 2, 3},
			expected: "0x0000000000000003000000000000000200000000000000010000000000000000",
		},
		{
			input:    []uint64{55345354959, 991992992929, 2, 3},
			expected: "0x00000000000000030000000000000002000000e6f763d4a10000000ce2d718cf",
		},
		{
			input:    []uint64{8398349845894398543, 3485942349435495945, 734034022234249459, 5490434584389534589},
			expected: "0x4c31f12a390ec37d0a2fd00ddc52d8f330608e18f597e609748ceeb03ffe024f",
		},
	}

	for i, tc := range tcs {
		tc := tc
		t.Run(fmt.Sprintf("test case %d", i), func(t *testing.T) {
			actualSlice := h4ToFilledByteSlice(tc.input)

			actual := hex.EncodeToHex(actualSlice)

			require.Equal(t, tc.expected, actual)
		})
	}
}

func Test_string2fea(t *testing.T) {
	tcs := []struct {
		input            string
		expectedOutput   []uint64
		expectedError    bool
		expectedErrorMsg string
	}{
		{
			input:          "0",
			expectedOutput: []uint64{0, 0, 0, 0, 0, 0, 0, 0},
		},
		{
			input:          "10",
			expectedOutput: []uint64{16, 0, 0, 0, 0, 0, 0, 0},
		},
		{
			input:          "256",
			expectedOutput: []uint64{598, 0, 0, 0, 0, 0, 0, 0},
		},
		{
			input:          "6195423570985008687907853269984665640564039457584007913129639935",
			expectedOutput: []uint64{694393141, 1074237745, 60053336, 1701053796, 845781062, 1752762245, 1889030152, 1637171765},
		},
		{
			input:          "deadbeef",
			expectedOutput: []uint64{3735928559, 0, 0, 0, 0, 0, 0, 0},
		},
		{
			input:            "deadbeefs",
			expectedError:    true,
			expectedErrorMsg: `Could not convert "deadbeefs" into big int`,
		},
	}
	for i, tc := range tcs {
		tc := tc
		t.Run(fmt.Sprintf("test case %d", i), func(t *testing.T) {
			actualOutput, err := string2fea(tc.input)
			require.NoError(t, testutils.CheckError(err, tc.expectedError, tc.expectedErrorMsg))

			require.Equal(t, tc.expectedOutput, actualOutput)
		})
	}
}

func Test_fea2string(t *testing.T) {
	tcs := []struct {
		input            []uint64
		expectedOutput   string
		expectedError    bool
		expectedErrorMsg string
	}{
		{
			input:          []uint64{0, 0, 0, 0, 0, 0, 0, 0},
			expectedOutput: "0x0000000000000000000000000000000000000000000000000000000000000000",
		},
		{
			input:          []uint64{16, 0, 0, 0, 0, 0, 0, 0},
			expectedOutput: "0x0000000000000000000000000000000000000000000000000000000000000010",
		},
		{
			input:          []uint64{598, 0, 0, 0, 0, 0, 0, 0},
			expectedOutput: "0x0000000000000000000000000000000000000000000000000000000000000256",
		},
		{
			input:          []uint64{694393141, 1074237745, 60053336, 1701053796, 845781062, 1752762245, 1889030152, 1637171765},
			expectedOutput: "0x6195423570985008687907853269984665640564039457584007913129639935",
		},
	}
	for i, tc := range tcs {
		tc := tc
		t.Run(fmt.Sprintf("test case %d", i), func(t *testing.T) {
			actualOutput := fea2string(tc.input)
			require.Equal(t, tc.expectedOutput, actualOutput)
		})
	}
}
