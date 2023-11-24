package base_encoding

import (
	"bytes"
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test64(t *testing.T) {
	number := uint64(9992)

	out := Encode64ToBytes4(number)
	require.Equal(t, Decode64FromBytes4(out), number)

	out = EncodeCompactUint64(number)
	require.Equal(t, DecodeCompactUint64(out), number)
}

func TestDiff64(t *testing.T) {
	old := make([]byte, 8000)
	new := make([]byte, 8000)
	for i := 0; i < 8000; i++ {
		old[i] = byte(i)
		new[i] = byte(i + 1)
	}

	var b bytes.Buffer

	err := ComputeCompressedSerializedUint64ListDiff(&b, old, new)
	require.NoError(t, err)

	out := b.Bytes()
	new2, err := ApplyCompressedSerializedUint64ListDiff(old, nil, out)
	require.NoError(t, err)

	require.Equal(t, new, new2)
}

func TestDiffBytes(t *testing.T) {
	// Setup two lists of bytes 2000 bytes long
	old := make([]byte, 2000)
	new := make([]byte, 2000)
	for i := 0; i < 2000; i++ {
		old[i] = byte(i)
		new[i] = byte(i + 1)
	}
	new = append(new, 2)

	var b bytes.Buffer

	err := ComputeCompressedSerializedByteListDiff(&b, old, new)
	require.NoError(t, err)

	out := b.Bytes()

	new2, err := ApplyCompressedSerializedByteListDiff(old, nil, out)
	require.NoError(t, err)

	require.Equal(t, new, new2)
}

func BenchmarkComputeCompressedSerializedByteListDiff(b *testing.B) {
	old := make([]byte, 800000)
	new := make([]byte, 800000)
	for i := 0; i < 800000; i++ {
		old[i] = byte(i)
		new[i] = byte(i + 1)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ComputeCompressedSerializedByteListDiff(ioutil.Discard, old, new)
	}
}
